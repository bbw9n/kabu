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

#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct BrokerageAccount {
    pub brokerage: Brokerage,
    pub id: BrokerageAccountId,
    pub display_label: String,
    pub base_currency: String,
    pub equity: Option<Decimal>,
    pub cash: Option<Decimal>,
    pub buying_power: Option<Decimal>,
}

impl fmt::Debug for BrokerageAccount {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("BrokerageAccount([REDACTED])")
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
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

impl fmt::Debug for BrokeragePosition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("BrokeragePosition([REDACTED])")
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct PortfolioSnapshot {
    pub brokerage: Brokerage,
    pub accounts: Vec<BrokerageAccount>,
    pub positions: Vec<BrokeragePosition>,
    pub received_at: DateTime<Utc>,
}

impl fmt::Debug for PortfolioSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PortfolioSnapshot([REDACTED])")
    }
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

#[test]
fn brokerage_account_debug_redacts_portfolio_values() {
    let account = BrokerageAccount {
        brokerage: Brokerage::Robinhood,
        id: BrokerageAccountId::new("RH-123456789"),
        display_label: "Robinhood ••••6789".into(),
        base_currency: "USD".into(),
        equity: Some(Decimal::new(123_456, 2)),
        cash: Some(Decimal::new(7_890, 2)),
        buying_power: Some(Decimal::new(5_000, 2)),
    };

    let debug = format!("{account:?}");
    assert!(!debug.contains("1234.56"));
    assert!(!debug.contains("78.90"));
    assert!(!debug.contains("50.00"));
}

#[test]
fn brokerage_position_debug_redacts_portfolio_values() {
    let position = BrokeragePosition {
        brokerage: Brokerage::Longbridge,
        account_id: BrokerageAccountId::new("LB-987654321"),
        native_symbol: "SECRET".into(),
        display_name: "Confidential position".into(),
        asset_kind: BrokerageAssetKind::Equity,
        instrument: None,
        quantity: Decimal::new(42_000, 3),
        average_cost: Some(Decimal::new(321_000, 2)),
        market_value: Some(Decimal::new(654_000, 2)),
        unrealized_pnl: Some(Decimal::new(333_000, 2)),
        currency: "USD".into(),
    };

    let debug = format!("{position:?}");
    assert!(!debug.contains("SECRET"));
    assert!(!debug.contains("42.000"));
    assert!(!debug.contains("3210.00"));
    assert!(!debug.contains("6540.00"));
    assert!(!debug.contains("3330.00"));
}

#[test]
fn portfolio_snapshot_debug_redacts_its_contents() {
    let snapshot = PortfolioSnapshot {
        brokerage: Brokerage::Robinhood,
        accounts: vec![BrokerageAccount {
            brokerage: Brokerage::Robinhood,
            id: BrokerageAccountId::new("RH-246801357"),
            display_label: "Robinhood ••••1357".into(),
            base_currency: "USD".into(),
            equity: Some(Decimal::new(9_999_999, 2)),
            cash: None,
            buying_power: None,
        }],
        positions: vec![],
        received_at: Utc::now(),
    };

    let debug = format!("{snapshot:?}");
    assert!(!debug.contains("246801357"));
    assert!(!debug.contains("99999.99"));
}
