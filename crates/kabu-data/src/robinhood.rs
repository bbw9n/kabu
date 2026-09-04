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

    let mut positions = Vec::with_capacity(equities.results.len() + crypto.results.len());
    for position in equities.results.into_iter().chain(crypto.results) {
        let account_id = match position.account_number.as_deref() {
            Some(id) => normalized_accounts
                .iter()
                .find(|account| account.id.as_str() == id)
                .map(|account| account.id.clone())
                .ok_or_else(malformed_response)?,
            None if normalized_accounts.len() == 1 => normalized_accounts[0].id.clone(),
            None => return Err(malformed_response()),
        };
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

impl RobinhoodPortfolioProvider {
    fn new(secrets: Arc<dyn SecretStore>) -> Result<Self, BrokerageError> {
        let endpoint =
            Url::parse(ENDPOINT).map_err(|_| BrokerageError::new(BrokerageErrorKind::Internal))?;
        Ok(Self {
            oauth: broker_oauth(Brokerage::Robinhood, endpoint.clone(), secrets),
            endpoint,
            session: Mutex::new(None),
            session_close_state: Mutex::new(SessionCloseState::default()),
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

    async fn store_session(&self, session: BrokerMcpSession) {
        *self.session.lock().await = Some(session);
        self.session_close_state.lock().await.reset();
    }
}

#[async_trait::async_trait]
impl BrokerageClient for RobinhoodPortfolioProvider {
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
        disconnect_after_close(self.close_session().await, self.oauth.disconnect()).await
    }
}

pub fn robinhood_provider(
    secrets: Arc<dyn SecretStore>,
) -> Result<Arc<dyn BrokerageClient>, BrokerageError> {
    Ok(Arc::new(RobinhoodPortfolioProvider::new(secrets)?))
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use kabu_core::{BrokerageError, BrokerageErrorKind, PortfolioProvider};

    use super::RobinhoodPortfolioProvider;
    use crate::{BrokerageClient, SecretStore, SecretStoreError};

    #[test]
    fn terminal_session_close_failure_is_not_reported_as_a_later_success() {
        let mut close_state = super::SessionCloseState::default();
        close_state.record_failure(BrokerageError::new(BrokerageErrorKind::Unavailable));

        let error = close_state
            .no_session_result()
            .expect_err("a consumed session with failed cleanup remains unsuccessful");
        assert_eq!(error.kind, BrokerageErrorKind::Unavailable);
    }

    #[derive(Default)]
    struct MemorySecretStore {
        value: Mutex<Option<Vec<u8>>>,
    }

    impl SecretStore for MemorySecretStore {
        fn get(&self, _service: &str, _account: &str) -> Result<Option<Vec<u8>>, SecretStoreError> {
            Ok(self
                .value
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone())
        }

        fn set(
            &self,
            _service: &str,
            _account: &str,
            value: &[u8],
        ) -> Result<(), SecretStoreError> {
            *self
                .value
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(value.to_vec());
            Ok(())
        }

        fn delete(&self, _service: &str, _account: &str) -> Result<(), SecretStoreError> {
            *self
                .value
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
            Ok(())
        }
    }

    #[tokio::test]
    async fn closing_a_session_preserves_credentials_while_disconnect_clears_them() {
        let secrets = Arc::new(MemorySecretStore::default());
        secrets
            .set("test-service", "test-account", b"stored-credentials")
            .expect("seed credentials");
        let provider = RobinhoodPortfolioProvider::new(secrets.clone()).expect("provider");

        provider.close_session().await.expect("close session");
        assert!(
            secrets
                .get("test-service", "test-account")
                .expect("read retained credentials")
                .is_some()
        );

        provider.disconnect().await.expect("explicit disconnect");
        assert_eq!(
            secrets
                .get("test-service", "test-account")
                .expect("read cleared credentials"),
            None
        );
    }
}
