use crate::Instrument;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::{fmt, time::Duration};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum Brokerage {
    Robinhood,
    Longbridge,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum BrokerageAssetKind {
    Equity,
    Crypto,
    Option,
    Fund,
    Cash,
    Other,
}

#[derive(Clone, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct BrokerageAccountId(String);

impl BrokerageAccountId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for BrokerageAccountId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("BrokerageAccountId([REDACTED])")
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BrokerageAccount {
    pub brokerage: Brokerage,
    pub id: BrokerageAccountId,
    pub display_label: String,
    pub base_currency: String,
    pub equity: Option<Decimal>,
    pub cash: Option<Decimal>,
    pub buying_power: Option<Decimal>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BrokeragePosition {
    pub brokerage: Brokerage,
    pub account_id: BrokerageAccountId,
    pub native_symbol: String,
    pub display_name: String,
    pub asset_kind: BrokerageAssetKind,
    pub instrument: Option<Instrument>,
    pub quantity: Decimal,
    pub average_cost: Option<Decimal>,
    pub market_value: Option<Decimal>,
    pub unrealized_pnl: Option<Decimal>,
    pub currency: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PortfolioSnapshot {
    pub brokerage: Brokerage,
    pub accounts: Vec<BrokerageAccount>,
    pub positions: Vec<BrokeragePosition>,
    pub received_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BrokerageConnectionState {
    Disconnected,
    Authorizing,
    Connecting,
    Connected {
        last_synced_at: Option<DateTime<Utc>>,
    },
    Refreshing {
        last_synced_at: Option<DateTime<Utc>>,
    },
    Error {
        message: String,
    },
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum BrokerageErrorKind {
    AuthorizationRequired,
    AuthorizationDenied,
    Permission,
    Timeout,
    RateLimited,
    Unavailable,
    MalformedResponse,
    IncompatibleToolSchema,
    Disconnected,
    SecureStorage,
    Internal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrokerageError {
    pub kind: BrokerageErrorKind,
    pub retry_after: Option<Duration>,
}

impl BrokerageError {
    pub fn new(kind: BrokerageErrorKind) -> Self {
        Self {
            kind,
            retry_after: None,
        }
    }

    pub fn with_retry_after(kind: BrokerageErrorKind, retry_after: Duration) -> Self {
        Self {
            kind,
            retry_after: Some(retry_after),
        }
    }

    pub fn user_message(&self) -> &'static str {
        match self.kind {
            BrokerageErrorKind::AuthorizationRequired => {
                "Connect this broker to load its portfolio."
            }
            BrokerageErrorKind::AuthorizationDenied => "Broker authorization was denied.",
            BrokerageErrorKind::Permission => {
                "This broker connection does not have the required permissions."
            }
            BrokerageErrorKind::Timeout => "The broker did not respond in time. Please try again.",
            BrokerageErrorKind::RateLimited => {
                "The broker is receiving too many requests. Please try again shortly."
            }
            BrokerageErrorKind::Unavailable => {
                "This broker is temporarily unavailable. Please try again later."
            }
            BrokerageErrorKind::MalformedResponse => "The broker returned an invalid response.",
            BrokerageErrorKind::IncompatibleToolSchema => {
                "This broker connection needs to be updated before it can be used."
            }
            BrokerageErrorKind::Disconnected => "This broker is disconnected.",
            BrokerageErrorKind::SecureStorage => {
                "Secure storage is unavailable for this broker connection."
            }
            BrokerageErrorKind::Internal => {
                "Unable to load this broker portfolio. Please try again."
            }
        }
    }
}

impl fmt::Display for BrokerageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.user_message())
    }
}

impl std::error::Error for BrokerageError {}

#[test]
fn account_ids_are_redacted_from_debug() {
    let id = BrokerageAccountId::new("RH-123456789");
    assert_eq!(format!("{id:?}"), "BrokerageAccountId([REDACTED])");
    assert_eq!(id.as_str(), "RH-123456789");
}

#[test]
fn portfolio_keeps_missing_money_distinct_from_zero() {
    let account = BrokerageAccount {
        brokerage: Brokerage::Robinhood,
        id: BrokerageAccountId::new("opaque"),
        display_label: "Robinhood ••••6789".into(),
        base_currency: "USD".into(),
        equity: None,
        cash: Some(Decimal::ZERO),
        buying_power: None,
    };
    assert_eq!(account.equity, None);
    assert_eq!(account.cash, Some(Decimal::ZERO));
}

#[test]
fn brokerage_errors_expose_only_fixed_safe_messages() {
    let error = BrokerageError::new(BrokerageErrorKind::AuthorizationRequired);
    assert_eq!(
        error.user_message(),
        "Connect this broker to load its portfolio."
    );
    assert!(!format!("{error:?}").contains("token"));
}
