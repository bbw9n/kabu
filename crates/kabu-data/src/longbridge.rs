use std::sync::Arc;

use chrono::{DateTime, Utc};
use kabu_core::{
    Brokerage, BrokerageAccount, BrokerageAccountId, BrokerageAssetKind, BrokerageError,
    BrokerageErrorKind, BrokeragePosition, Instrument, PortfolioProvider, PortfolioSnapshot,
};
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::{
    BrokerOAuth, BrokerageClient, SecretStore, broker_oauth, disconnect_after_close,
    malformed_response,
    mcp::{BrokerMcpConfig, BrokerMcpSession, ReadCapabilityInput},
    parse_bounded_decimal, safe_account_label,
};

const ENDPOINT: &str = "https://mcp.longbridge.com";

#[derive(Deserialize)]
struct BalanceWire {
    total_cash: String,
    max_finance_amount: String,
    remaining_finance_amount: String,
    margin_call: String,
    currency: String,
    #[serde(default)]
    cash_infos: Vec<CashInfoWire>,
    net_assets: String,
    init_margin: String,
    maintenance_margin: String,
    buy_power: String,
    #[serde(default)]
    frozen_transaction_fees: Vec<FrozenTransactionFeeWire>,
}

#[derive(Deserialize)]
struct CashInfoWire {
    withdraw_cash: String,
    available_cash: String,
    frozen_cash: String,
    settling_cash: String,
}

#[derive(Deserialize)]
struct FrozenTransactionFeeWire {
    frozen_transaction_fee: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PositionsEnvelope {
    list: Vec<PositionChannelWire>,
    #[serde(default)]
    _us_asset_overview: Option<Value>,
    #[serde(default)]
    _warnings: Option<Vec<String>>,
}

#[derive(Deserialize)]
struct PositionChannelWire {
    stock_info: Vec<StockPositionWire>,
}

#[derive(Deserialize)]
struct StockPositionWire {
    symbol: String,
    symbol_name: String,
    quantity: String,
    available_quantity: String,
    currency: String,
    cost_price: String,
    market: String,
    #[serde(default)]
    init_quantity: Option<String>,
}

/// Normalize the documented Longbridge balance array and stock-position envelope.
pub fn normalize_longbridge(
    balances: Value,
    positions: Value,
    received_at: DateTime<Utc>,
) -> Result<PortfolioSnapshot, BrokerageError> {
    let balances: Vec<BalanceWire> =
        serde_json::from_value(balances).map_err(|_| malformed_response())?;
    let positions: PositionsEnvelope =
        serde_json::from_value(positions).map_err(|_| malformed_response())?;

    let mut accounts = Vec::with_capacity(balances.len());
    for (index, balance) in balances.iter().enumerate() {
        validate_balance_decimals(balance)?;
        let opaque_id = format!("longbridge-{}", index + 1);
        accounts.push(BrokerageAccount {
            brokerage: Brokerage::Longbridge,
            id: BrokerageAccountId::new(opaque_id.clone()),
            display_label: safe_account_label("Longbridge", &opaque_id),
            base_currency: balance.currency.clone(),
            equity: Some(parse_bounded_decimal(&balance.net_assets)?),
            cash: Some(parse_bounded_decimal(&balance.total_cash)?),
            buying_power: Some(parse_bounded_decimal(&balance.buy_power)?),
        });
    }

    let Some(default_account) = accounts.first().map(|account| account.id.clone()) else {
        return Ok(PortfolioSnapshot {
            brokerage: Brokerage::Longbridge,
            accounts,
            positions: Vec::new(),
            received_at,
        });
    };
    let mut normalized_positions = Vec::new();
    for channel in positions.list {
        for position in channel.stock_info {
            normalized_positions.push(normalize_position(position, default_account.clone())?);
        }
    }
    Ok(PortfolioSnapshot {
        brokerage: Brokerage::Longbridge,
        accounts,
        positions: normalized_positions,
        received_at,
    })
}

fn validate_balance_decimals(balance: &BalanceWire) -> Result<(), BrokerageError> {
    for value in [
        &balance.total_cash,
        &balance.max_finance_amount,
        &balance.remaining_finance_amount,
        &balance.margin_call,
        &balance.net_assets,
        &balance.init_margin,
        &balance.maintenance_margin,
        &balance.buy_power,
    ] {
        parse_bounded_decimal(value)?;
    }
    for cash in &balance.cash_infos {
        for value in [
            &cash.withdraw_cash,
            &cash.available_cash,
            &cash.frozen_cash,
            &cash.settling_cash,
        ] {
            parse_bounded_decimal(value)?;
        }
    }
    for fee in &balance.frozen_transaction_fees {
        parse_bounded_decimal(&fee.frozen_transaction_fee)?;
    }
    Ok(())
}

fn normalize_position(
    position: StockPositionWire,
    account_id: BrokerageAccountId,
) -> Result<BrokeragePosition, BrokerageError> {
    parse_bounded_decimal(&position.available_quantity)?;
    if let Some(initial_quantity) = position.init_quantity.as_deref() {
        parse_bounded_decimal(initial_quantity)?;
    }
    let instrument = position
        .symbol
        .rsplit_once('.')
        .and_then(|(symbol, suffix)| {
            (position.market.eq_ignore_ascii_case("US") && suffix.eq_ignore_ascii_case("US"))
                .then(|| {
                    Instrument::equity(symbol, None, &position.symbol_name, &position.currency).ok()
                })
                .flatten()
        });
    Ok(BrokeragePosition {
        brokerage: Brokerage::Longbridge,
        account_id,
        native_symbol: position.symbol,
        display_name: position.symbol_name,
        asset_kind: BrokerageAssetKind::Equity,
        instrument,
        quantity: parse_bounded_decimal(&position.quantity)?,
        average_cost: Some(parse_bounded_decimal(&position.cost_price)?),
        market_value: None,
        unrealized_pnl: None,
        currency: position.currency,
    })
}

/// Hosted, read-only Longbridge MCP portfolio provider.
pub struct LongbridgePortfolioProvider {
    endpoint: Url,
    oauth: BrokerOAuth,
    session: Mutex<Option<BrokerMcpSession>>,
    session_close_state: Mutex<SessionCloseState>,
}

#[derive(Default)]
struct SessionCloseState {
    terminal_error: Option<BrokerageError>,
}

impl SessionCloseState {
    fn reset(&mut self) {
        self.terminal_error = None;
    }

    fn record_failure(&mut self, error: BrokerageError) {
        self.terminal_error = Some(error);
    }

    fn no_session_result(&self) -> Result<(), BrokerageError> {
        self.terminal_error.clone().map_or(Ok(()), Err)
    }
}

impl LongbridgePortfolioProvider {
    fn new(secrets: Arc<dyn SecretStore>) -> Result<Self, BrokerageError> {
        let endpoint =
            Url::parse(ENDPOINT).map_err(|_| BrokerageError::new(BrokerageErrorKind::Internal))?;
        Ok(Self {
            oauth: broker_oauth(Brokerage::Longbridge, endpoint.clone(), secrets),
            endpoint,
            session: Mutex::new(None),
            session_close_state: Mutex::new(SessionCloseState::default()),
        })
    }

    async fn start_session(&self) -> Result<BrokerMcpSession, BrokerageError> {
        let client = self.oauth.authorized_transport_client().await?;
        BrokerMcpSession::connect(BrokerMcpConfig::new(
            Brokerage::Longbridge,
            self.endpoint.clone(),
            client,
        ))
        .await
    }

    async fn store_session(&self, session: BrokerMcpSession) {
        *self.session.lock().await = Some(session);
        self.session_close_state.lock().await.reset();
    }
}

#[async_trait::async_trait]
impl BrokerageClient for LongbridgePortfolioProvider {
    async fn connect(
        &self,
        open_authorization_url: &(
             dyn for<'url> Fn(&'url Url) -> Result<(), BrokerageError> + Send + Sync
         ),
        cancellation: CancellationToken,
    ) -> Result<(), BrokerageError> {
        if self.session.lock().await.is_some() {
            return Ok(());
        }
        let initial_session = tokio::select! {
            result = self.start_session() => result,
            _ = cancellation.cancelled() => {
                return Err(BrokerageError::new(BrokerageErrorKind::AuthorizationDenied));
            }
        };
        match initial_session {
            Ok(connected) => {
                if cancellation.is_cancelled() {
                    let _ = connected.close().await;
                    return Err(BrokerageError::new(BrokerageErrorKind::AuthorizationDenied));
                }
                self.store_session(connected).await;
                return Ok(());
            }
            Err(error) if error.kind == BrokerageErrorKind::AuthorizationRequired => {}
            Err(error) => return Err(error),
        }
        let mut pending = self.oauth.begin_authorization().await?;
        if cancellation.is_cancelled() {
            let _ = pending.cancel().await;
            return Err(BrokerageError::new(BrokerageErrorKind::AuthorizationDenied));
        }
        if let Err(error) = open_authorization_url(&pending.authorization_url) {
            let _ = pending.cancel().await;
            return Err(error);
        }
        let callback = tokio::select! {
            callback = pending.wait_for_callback() => match callback {
                Ok(callback) => callback,
                Err(error) => {
                    let _ = pending.cancel().await;
                    return Err(error);
                }
            },
            _ = cancellation.cancelled() => {
                let _ = pending.cancel().await;
                return Err(BrokerageError::new(BrokerageErrorKind::AuthorizationDenied));
            }
        };
        if cancellation.is_cancelled() {
            let _ = pending.cancel().await;
            return Err(BrokerageError::new(BrokerageErrorKind::AuthorizationDenied));
        }
        self.oauth
            .complete_authorization(pending, &callback)
            .await?;
        let connected = tokio::select! {
            result = self.start_session() => result?,
            _ = cancellation.cancelled() => {
                return Err(BrokerageError::new(BrokerageErrorKind::AuthorizationDenied));
            }
        };
        if cancellation.is_cancelled() {
            let _ = connected.close().await;
            return Err(BrokerageError::new(BrokerageErrorKind::AuthorizationDenied));
        }
        self.store_session(connected).await;
        Ok(())
    }

    async fn close_session(&self) -> Result<(), BrokerageError> {
        let Some(session) = self.session.lock().await.take() else {
            return self.session_close_state.lock().await.no_session_result();
        };
        match session.close().await {
            Ok(()) => {
                self.session_close_state.lock().await.reset();
                Ok(())
            }
            Err(error) => {
                self.session_close_state
                    .lock()
                    .await
                    .record_failure(error.clone());
                Err(error)
            }
        }
    }
}

#[async_trait::async_trait]
impl PortfolioProvider for LongbridgePortfolioProvider {
    fn brokerage(&self) -> Brokerage {
        Brokerage::Longbridge
    }

    async fn refresh_portfolio(&self) -> Result<PortfolioSnapshot, BrokerageError> {
        let session = self.session.lock().await;
        let session = session
            .as_ref()
            .ok_or_else(|| BrokerageError::new(BrokerageErrorKind::Disconnected))?;
        let balances = session
            .call_read(ReadCapabilityInput::Accounts {
                currency: Some("USD".into()),
            })
            .await?
            .into_value();
        let positions = session
            .call_read(ReadCapabilityInput::EquityPositions)
            .await?
            .into_value();
        let mut snapshot = normalize_longbridge(balances, positions, Utc::now())?;
        snapshot.received_at = Utc::now();
        Ok(snapshot)
    }

    async fn disconnect(&self) -> Result<(), BrokerageError> {
        disconnect_after_close(self.close_session().await, self.oauth.disconnect()).await
    }
}

pub fn longbridge_provider(
    secrets: Arc<dyn SecretStore>,
) -> Result<Arc<dyn BrokerageClient>, BrokerageError> {
    Ok(Arc::new(LongbridgePortfolioProvider::new(secrets)?))
}
