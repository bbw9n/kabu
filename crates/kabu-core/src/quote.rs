use crate::instrument::InstrumentId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum ChartPeriod {
    OneDay,
    OneWeek,
    OneMonth,
    ThreeMonths,
    OneYear,
}
impl ChartPeriod {
    pub fn cache_ttl(self) -> Duration {
        match self {
            Self::OneDay => Duration::from_secs(60),
            Self::OneWeek => Duration::from_secs(120),
            Self::OneMonth => Duration::from_secs(300),
            Self::ThreeMonths => Duration::from_secs(900),
            Self::OneYear => Duration::from_secs(1800),
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum DataSource {
    Yahoo,
    Binance,
}
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum MarketState {
    PreMarket,
    Regular,
    PostMarket,
    Closed,
    Continuous,
    Unknown,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Quote {
    pub instrument_id: InstrumentId,
    pub last: f64,
    pub previous_close: Option<f64>,
    pub open: Option<f64>,
    pub high: Option<f64>,
    pub low: Option<f64>,
    pub volume: Option<f64>,
    pub currency: String,
    pub source: DataSource,
    pub market_state: MarketState,
    pub provider_timestamp: DateTime<Utc>,
    pub received_at: DateTime<Utc>,
    pub provider_delayed: bool,
}
impl Quote {
    pub fn absolute_change(&self) -> Option<f64> {
        self.previous_close
            .map(|previous_close| self.last - previous_close)
    }
    pub fn percent_change(&self) -> Option<f64> {
        self.previous_close
            .filter(|previous_close| *previous_close != 0.0)
            .map(|previous_close| (self.last - previous_close) / previous_close * 100.0)
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Candle {
    pub starts_at: DateTime<Utc>,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::instrument::InstrumentId;
    #[test]
    fn chart_periods_have_exact_cache_ttls() {
        assert_eq!(ChartPeriod::OneDay.cache_ttl(), Duration::from_secs(60));
        assert_eq!(ChartPeriod::OneMonth.cache_ttl(), Duration::from_secs(300));
        assert_eq!(ChartPeriod::OneYear.cache_ttl(), Duration::from_secs(1800));
    }
    #[test]
    fn quote_change_returns_none_without_a_nonzero_previous_close() {
        let quote = quote(Some(0.0));
        assert_eq!(quote.absolute_change(), Some(125.0));
        assert_eq!(quote.percent_change(), None);
    }
    #[test]
    fn quote_change_returns_none_without_a_previous_close() {
        let quote = quote(None);
        assert_eq!(quote.absolute_change(), None);
        assert_eq!(quote.percent_change(), None);
    }
    #[test]
    fn quote_change_uses_previous_close() {
        let quote = quote(Some(100.0));
        assert_eq!(quote.absolute_change(), Some(25.0));
        assert_eq!(quote.percent_change(), Some(25.0));
    }
    fn quote(previous_close: Option<f64>) -> Quote {
        Quote {
            instrument_id: InstrumentId::equity("nvda", None).expect("valid equity"),
            last: 125.0,
            previous_close,
            open: None,
            high: None,
            low: None,
            volume: None,
            currency: "USD".into(),
            source: DataSource::Yahoo,
            market_state: MarketState::Regular,
            provider_timestamp: Utc::now(),
            received_at: Utc::now(),
            provider_delayed: false,
        }
    }
}
