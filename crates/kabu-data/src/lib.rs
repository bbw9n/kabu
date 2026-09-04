//! Provider integrations and persistence for Kabu.

mod binance;
mod http;
mod longbridge;
pub mod mcp;
pub mod persistence;
mod robinhood;
mod secret_store;
mod yahoo;

use std::future::Future;
use std::sync::Arc;

use kabu_core::{BrokerageError, BrokerageErrorKind, PortfolioProvider};
use rust_decimal::Decimal;
use url::Url;

pub use binance::BinanceClient;
pub use longbridge::{LongbridgePortfolioProvider, longbridge_provider, normalize_longbridge};
pub use mcp::{BrokerCredentialStore, BrokerOAuth, PendingAuthorization};
pub use persistence::{JsonRepository, Recovery};
pub use robinhood::{RobinhoodPortfolioProvider, normalize_robinhood, robinhood_provider};
pub use secret_store::{MacKeychainSecretStore, SecretStore, SecretStoreError};
pub use yahoo::YahooClient;

/// A portfolio provider with an explicitly user-mediated hosted-broker connection.
#[async_trait::async_trait]
pub trait BrokerageClient: PortfolioProvider {
    async fn connect(
        &self,
        open_authorization_url: &(
             dyn for<'url> Fn(&'url Url) -> Result<(), BrokerageError> + Send + Sync
         ),
    ) -> Result<(), BrokerageError>;

    /// Close only the live MCP session, preserving stored OAuth credentials.
    async fn close_session(&self) -> Result<(), BrokerageError>;
}

fn malformed_response() -> BrokerageError {
    BrokerageError::new(BrokerageErrorKind::MalformedResponse)
}

/// Parse a broker decimal without admitting unbounded numeric spellings.
fn parse_bounded_decimal(value: &str) -> Result<Decimal, BrokerageError> {
    let value = value.trim();
    let unsigned = value.strip_prefix(['+', '-']).unwrap_or(value);
    let (mantissa, exponent) = match unsigned.split_once(['e', 'E']) {
        Some((mantissa, exponent)) if !exponent.contains(['e', 'E']) => (mantissa, Some(exponent)),
        Some(_) => return Err(malformed_response()),
        None => (unsigned, None),
    };

    if mantissa.is_empty()
        || mantissa.matches('.').count() > 1
        || mantissa
            .chars()
            .any(|character| character != '.' && !character.is_ascii_digit())
    {
        return Err(malformed_response());
    }
    let digits: String = mantissa
        .chars()
        .filter(|character| character.is_ascii_digit())
        .collect();
    if digits.is_empty() || digits.trim_start_matches('0').len().max(1) > 38 {
        return Err(malformed_response());
    }
    if let Some(exponent) = exponent {
        let exponent_digits = exponent.strip_prefix(['+', '-']).unwrap_or(exponent);
        if exponent_digits.is_empty()
            || !exponent_digits
                .chars()
                .all(|character| character.is_ascii_digit())
            || exponent.parse::<i32>().is_err()
        {
            return Err(malformed_response());
        }
        return Decimal::from_scientific(value).map_err(|_| malformed_response());
    }
    Decimal::from_str_exact(value).map_err(|_| malformed_response())
}

fn parse_optional_decimal(value: Option<&str>) -> Result<Option<Decimal>, BrokerageError> {
    value.map(parse_bounded_decimal).transpose()
}

fn safe_account_label(broker: &str, account_id: &str) -> String {
    let suffix: String = account_id
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .rev()
        .take(4)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    if suffix.is_empty() {
        broker.to_owned()
    } else {
        format!("{broker} •••{suffix}")
    }
}

fn broker_oauth(
    brokerage: kabu_core::Brokerage,
    endpoint: Url,
    secrets: Arc<dyn SecretStore>,
) -> BrokerOAuth {
    BrokerOAuth::new(
        brokerage,
        endpoint,
        Arc::new(BrokerCredentialStore::new(brokerage, secrets)),
    )
}

async fn disconnect_after_close(
    close_result: Result<(), BrokerageError>,
    clear_credentials: impl Future<Output = Result<(), BrokerageError>>,
) -> Result<(), BrokerageError> {
    match clear_credentials.await {
        Err(error) => Err(error),
        Ok(()) => close_result,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    use kabu_core::{BrokerageError, BrokerageErrorKind};

    use super::disconnect_after_close;

    #[tokio::test]
    async fn credential_clearing_is_attempted_after_close_failure_and_its_safe_error_wins() {
        let cleared = Arc::new(AtomicBool::new(false));
        let clear_flag = cleared.clone();
        let error = disconnect_after_close(
            Err(BrokerageError::new(BrokerageErrorKind::Unavailable)),
            async move {
                clear_flag.store(true, Ordering::SeqCst);
                Err(BrokerageError::new(BrokerageErrorKind::SecureStorage))
            },
        )
        .await
        .expect_err("failed credential clearing is reported after close failure");

        assert!(cleared.load(Ordering::SeqCst));
        assert_eq!(error.kind, BrokerageErrorKind::SecureStorage);
        assert!(!error.to_string().contains("credential"));
    }
}
