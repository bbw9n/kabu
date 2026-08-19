use crate::quote::DataSource;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum InstrumentId {
    Equity {
        symbol: String,
        venue: Option<String>,
    },
    Crypto {
        base: String,
        quote: String,
    },
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum AssetClass {
    Equity,
    Crypto,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct Instrument {
    pub id: InstrumentId,
    pub display_name: String,
    pub currency: String,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum InstrumentError {
    #[error("{field} must not be blank")]
    Blank { field: &'static str },
    #[error("{field} must not contain a slash")]
    EmbeddedSlash { field: &'static str },
    #[error("{field} must not contain ASCII control characters")]
    AsciiControl { field: &'static str },
    #[error("crypto base and quote assets must differ")]
    IdenticalCryptoAssets,
}

impl InstrumentId {
    pub fn asset_class(&self) -> AssetClass {
        match self {
            Self::Equity { .. } => AssetClass::Equity,
            Self::Crypto { .. } => AssetClass::Crypto,
        }
    }

    pub fn equity(symbol: &str, venue: Option<&str>) -> Result<Self, InstrumentError> {
        let symbol = normalize_asset(symbol, "symbol", false)?;
        let venue = venue.map(normalize_venue).transpose()?;
        Ok(Self::Equity { symbol, venue })
    }
    pub fn crypto(base: &str, quote: &str) -> Result<Self, InstrumentError> {
        let base = normalize_asset(base, "base", true)?;
        let quote = normalize_asset(quote, "quote", true)?;
        if base == quote {
            return Err(InstrumentError::IdenticalCryptoAssets);
        }
        Ok(Self::Crypto { base, quote })
    }
    pub fn canonical_symbol(&self) -> String {
        match self {
            Self::Equity { symbol, .. } => symbol.clone(),
            Self::Crypto { base, quote } => format!("{base}/{quote}"),
        }
    }
    pub fn provider_symbol(&self, source: DataSource) -> Option<String> {
        match (self, source) {
            (Self::Equity { symbol, .. }, DataSource::Yahoo) => Some(symbol.clone()),
            (Self::Crypto { base, quote }, DataSource::Binance) => Some(format!("{base}{quote}")),
            _ => None,
        }
    }
}

impl Instrument {
    pub fn equity(
        symbol: &str,
        venue: Option<&str>,
        display_name: &str,
        currency: &str,
    ) -> Result<Self, InstrumentError> {
        Ok(Self {
            id: InstrumentId::equity(symbol, venue)?,
            display_name: display_name.to_owned(),
            currency: currency.to_owned(),
        })
    }
    pub fn crypto(base: &str, quote: &str, display_name: &str) -> Result<Self, InstrumentError> {
        Ok(Self {
            id: InstrumentId::crypto(base, quote)?,
            display_name: display_name.to_owned(),
            currency: quote.trim().to_ascii_uppercase(),
        })
    }
}

fn normalize_asset(
    value: &str,
    field: &'static str,
    reject_slash: bool,
) -> Result<String, InstrumentError> {
    let value = value.trim();
    if value.is_empty() {
        return Err(InstrumentError::Blank { field });
    }
    if value.chars().any(|character| character.is_ascii_control()) {
        return Err(InstrumentError::AsciiControl { field });
    }
    if reject_slash && value.contains('/') {
        return Err(InstrumentError::EmbeddedSlash { field });
    }
    Ok(value.to_ascii_uppercase())
}

fn normalize_venue(venue: &str) -> Result<String, InstrumentError> {
    let venue = normalize_asset(venue, "venue", false)?;
    Ok(match venue.as_str() {
        "NMS" | "NGM" | "NCM" | "NASDAQ" => "NASDAQ".into(),
        "NYQ" | "NYSE" => "NYSE".into(),
        _ => venue,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn crypto_identity_keeps_base_and_quote_separate() {
        let id = InstrumentId::crypto("btc", "usdt").expect("valid pair");
        assert_eq!(id.canonical_symbol(), "BTC/USDT");
        assert_eq!(
            id.provider_symbol(DataSource::Binance),
            Some("BTCUSDT".into())
        );
    }
    #[test]
    fn equity_identity_normalizes_case() {
        let id = InstrumentId::equity("nvda", Some("NASDAQ")).expect("valid equity");
        assert_eq!(id.canonical_symbol(), "NVDA");
    }
    #[test]
    fn equity_identity_canonicalizes_known_us_venue_aliases() {
        let nasdaq = InstrumentId::equity("nvda", Some("ngm")).expect("valid equity");
        let nyse = InstrumentId::equity("ibm", Some("nyq")).expect("valid equity");
        assert_eq!(
            nasdaq,
            InstrumentId::Equity {
                symbol: "NVDA".into(),
                venue: Some("NASDAQ".into())
            }
        );
        assert_eq!(
            nyse,
            InstrumentId::Equity {
                symbol: "IBM".into(),
                venue: Some("NYSE".into())
            }
        );
    }

    #[test]
    fn identity_reports_its_asset_class() {
        let equity = InstrumentId::equity("nvda", Some("NASDAQ")).expect("equity");
        let crypto = InstrumentId::crypto("btc", "usdt").expect("crypto");

        assert_eq!(equity.asset_class(), AssetClass::Equity);
        assert_eq!(crypto.asset_class(), AssetClass::Crypto);
    }
    #[test]
    fn identity_constructors_reject_invalid_assets() {
        assert!(InstrumentId::equity(" ", None).is_err());
        assert!(InstrumentId::crypto("BTC/ETH", "USDT").is_err());
        assert!(InstrumentId::crypto("BTC", "BTC").is_err());
        assert!(InstrumentId::crypto("BTC\u{0007}", "USDT").is_err());
    }
}
