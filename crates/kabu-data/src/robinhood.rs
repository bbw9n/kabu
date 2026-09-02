use std::sync::Arc;

use chrono::{DateTime, Utc};
use kabu_core::{
    Brokerage, BrokerageAccount, BrokerageAccountId, BrokerageAssetKind, BrokerageError,
    BrokerageErrorKind, BrokeragePosition, Instrument, PortfolioProvider, PortfolioSnapshot,
};
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::Mutex;
use url::Url;

use crate::{
    BrokerOAuth, BrokerageClient, SecretStore, broker_oauth, malformed_response,
    mcp::{BrokerMcpConfig, BrokerMcpSession, ReadCapabilityInput},
    parse_bounded_decimal, parse_optional_decimal, safe_account_label,
};

const ENDPOINT: &str = "https://agent.robinhood.com/mcp/trading";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Page<T> {
    results: Vec<T>,
    #[serde(default, rename = "next")]
    _next: Option<Value>,
    #[serde(default, rename = "previous")]
    _previous: Option<Value>,
}

#[derive(Deserialize)]
struct AccountWire {
    account_number: String,
    #[serde(rename = "currency")]
    _currency: String,
}

#[derive(Deserialize)]
struct PortfolioWire {
    account_number: String,
    #[serde(rename = "currency")]
    _currency: String,
    #[serde(default)]
    equity: Option<String>,
    #[serde(default)]
    cash: Option<String>,
    #[serde(default)]
    buying_power: Option<String>,
}

#[derive(Deserialize)]
struct PositionWire {
    #[serde(default)]
    account_number: Option<String>,
    symbol: String,
    name: String,
    asset_type: String,
    quantity: String,
    #[serde(default)]
    average_buy_price: Option<String>,
    #[serde(default)]
    market_value: Option<String>,
    #[serde(default)]
    unrealized_pl: Option<String>,
    currency: String,
    #[serde(default)]
    base_currency_code: Option<String>,
    #[serde(default)]
    quote_currency_code: Option<String>,
}

/// Normalize the read-only Robinhood wire profile without exposing raw account IDs in labels.
pub fn normalize_robinhood(
    accounts: Value,
    portfolio: Value,
    equity_positions: Value,
    crypto_positions: Value,
    received_at: DateTime<Utc>,
) -> Result<PortfolioSnapshot, BrokerageError> {
    let accounts: Page<AccountWire> =
        serde_json::from_value(accounts).map_err(|_| malformed_response())?;
    if accounts.results.is_empty() {
        return Ok(PortfolioSnapshot {
            brokerage: Brokerage::Robinhood,
            accounts: Vec::new(),
            positions: Vec::new(),
            received_at,
        });
    }
    let portfolio: PortfolioWire =
        serde_json::from_value(portfolio).map_err(|_| malformed_response())?;
    let equities: Page<PositionWire> =
        serde_json::from_value(equity_positions).map_err(|_| malformed_response())?;
    let crypto: Page<PositionWire> =
        serde_json::from_value(crypto_positions).map_err(|_| malformed_response())?;

    let mut normalized_accounts = Vec::with_capacity(accounts.results.len());
    for account in accounts.results {
        let summary_matches = account.account_number == portfolio.account_number;
        normalized_accounts.push(BrokerageAccount {
            brokerage: Brokerage::Robinhood,
            id: BrokerageAccountId::new(account.account_number.clone()),
            display_label: safe_account_label("Robinhood", &account.account_number),
            base_currency: account._currency,
            equity: summary_matches
                .then(|| parse_optional_decimal(portfolio.equity.as_deref()))
                .transpose()?
                .flatten(),
            cash: summary_matches
                .then(|| parse_optional_decimal(portfolio.cash.as_deref()))
                .transpose()?
                .flatten(),
            buying_power: summary_matches
                .then(|| parse_optional_decimal(portfolio.buying_power.as_deref()))
                .transpose()?
                .flatten(),
        });
    }

    let default_account = normalized_accounts[0].id.clone();
    let mut positions = Vec::with_capacity(equities.results.len() + crypto.results.len());
    for position in equities.results.into_iter().chain(crypto.results) {
        let account_id = position
            .account_number
            .as_deref()
            .and_then(|id| {
                normalized_accounts
                    .iter()
                    .find(|account| account.id.as_str() == id)
            })
            .map(|account| account.id.clone())
            .unwrap_or_else(|| default_account.clone());
        positions.push(normalize_position(position, account_id)?);
    }
    Ok(PortfolioSnapshot {
        brokerage: Brokerage::Robinhood,
        accounts: normalized_accounts,
        positions,
        received_at,
    })
}

fn normalize_position(
    position: PositionWire,
    account_id: BrokerageAccountId,
) -> Result<BrokeragePosition, BrokerageError> {
    let asset_kind = match position.asset_type.to_ascii_lowercase().as_str() {
        "equity" | "stock" => BrokerageAssetKind::Equity,
        "crypto" | "cryptocurrency" => BrokerageAssetKind::Crypto,
        "option" => BrokerageAssetKind::Option,
        "fund" | "etf" | "mutual_fund" => BrokerageAssetKind::Fund,
        "cash" => BrokerageAssetKind::Cash,
        _ => BrokerageAssetKind::Other,
    };
    let instrument = match asset_kind {
        BrokerageAssetKind::Equity => {
            Instrument::equity(&position.symbol, None, &position.name, &position.currency).ok()
        }
        BrokerageAssetKind::Crypto => match (
            position.base_currency_code.as_deref(),
            position.quote_currency_code.as_deref(),
        ) {
            (Some(base), Some(quote)) => Instrument::crypto(base, quote, &position.name).ok(),
            _ => None,
        },
        _ => None,
    };
    Ok(BrokeragePosition {
        brokerage: Brokerage::Robinhood,
        account_id,
        native_symbol: position.symbol,
        display_name: position.name,
        asset_kind,
        instrument,
        quantity: parse_bounded_decimal(&position.quantity)?,
        average_cost: parse_optional_decimal(position.average_buy_price.as_deref())?,
        market_value: parse_optional_decimal(position.market_value.as_deref())?,
        unrealized_pnl: parse_optional_decimal(position.unrealized_pl.as_deref())?,
        currency: position.currency,
    })
}

/// Hosted, read-only Robinhood MCP portfolio provider.
pub struct RobinhoodPortfolioProvider {
    endpoint: Url,
    oauth: BrokerOAuth,
    session: Mutex<Option<BrokerMcpSession>>,
}

impl RobinhoodPortfolioProvider {
    fn new(secrets: Arc<dyn SecretStore>) -> Result<Self, BrokerageError> {
        let endpoint =
            Url::parse(ENDPOINT).map_err(|_| BrokerageError::new(BrokerageErrorKind::Internal))?;
        Ok(Self {
            oauth: broker_oauth(Brokerage::Robinhood, endpoint.clone(), secrets),
            endpoint,
            session: Mutex::new(None),
        })
    }

    async fn start_session(&self) -> Result<BrokerMcpSession, BrokerageError> {
        let client = self.oauth.authorized_transport_client().await?;
        BrokerMcpSession::connect(BrokerMcpConfig::new(
            Brokerage::Robinhood,
            self.endpoint.clone(),
            client,
        ))
        .await
    }
}

#[async_trait::async_trait]
impl BrokerageClient for RobinhoodPortfolioProvider {
    async fn connect(
        &self,
        open_authorization_url: &(
             dyn for<'url> Fn(&'url Url) -> Result<(), BrokerageError> + Send + Sync
         ),
    ) -> Result<(), BrokerageError> {
        let mut session = self.session.lock().await;
        if session.is_some() {
            return Ok(());
        }
        match self.start_session().await {
            Ok(connected) => {
                *session = Some(connected);
                return Ok(());
            }
            Err(error) if error.kind == BrokerageErrorKind::AuthorizationRequired => {}
            Err(error) => return Err(error),
        }
        let mut pending = self.oauth.begin_authorization().await?;
        if let Err(error) = open_authorization_url(&pending.authorization_url) {
            let _ = pending.cancel().await;
            return Err(error);
        }
        let callback = match pending.wait_for_callback().await {
            Ok(callback) => callback,
            Err(error) => {
                let _ = pending.cancel().await;
                return Err(error);
            }
        };
        self.oauth
            .complete_authorization(pending, &callback)
            .await?;
        *session = Some(self.start_session().await?);
        Ok(())
    }
}

#[async_trait::async_trait]
impl PortfolioProvider for RobinhoodPortfolioProvider {
    fn brokerage(&self) -> Brokerage {
        Brokerage::Robinhood
    }

    async fn refresh_portfolio(&self) -> Result<PortfolioSnapshot, BrokerageError> {
        let session = self.session.lock().await;
        let session = session
            .as_ref()
            .ok_or_else(|| BrokerageError::new(BrokerageErrorKind::Disconnected))?;
        let accounts = session
            .call_read(ReadCapabilityInput::Accounts { currency: None })
            .await?
            .into_value();
        if accounts
            .get("results")
            .and_then(Value::as_array)
            .is_some_and(Vec::is_empty)
        {
            return normalize_robinhood(
                accounts,
                Value::Null,
                Value::Null,
                Value::Null,
                Utc::now(),
            );
        }
        let portfolio = session
            .call_read(ReadCapabilityInput::PortfolioSummary)
            .await?
            .into_value();
        let equities = session
            .call_read(ReadCapabilityInput::EquityPositions)
            .await?
            .into_value();
        let crypto = session
            .call_read(ReadCapabilityInput::CryptoPositions)
            .await?
            .into_value();
        let mut snapshot = normalize_robinhood(accounts, portfolio, equities, crypto, Utc::now())?;
        snapshot.received_at = Utc::now();
        Ok(snapshot)
    }

    async fn disconnect(&self) -> Result<(), BrokerageError> {
        if let Some(session) = self.session.lock().await.take() {
            session.close().await?;
        }
        self.oauth.disconnect().await
    }
}

pub fn robinhood_provider(
    secrets: Arc<dyn SecretStore>,
) -> Result<Arc<dyn BrokerageClient>, BrokerageError> {
    Ok(Arc::new(RobinhoodPortfolioProvider::new(secrets)?))
}
