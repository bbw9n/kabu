use crate::{InstrumentId, MarketState, Quote};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Freshness {
    Live,
    Fresh,
    Aging,
    Delayed,
    Stale,
    Closed,
    Unavailable,
}

pub fn classify_freshness(quote: &Quote, now: DateTime<Utc>) -> Freshness {
    if quote.provider_delayed {
        return Freshness::Delayed;
    }
    if quote.market_state == MarketState::Closed {
        return Freshness::Closed;
    }

    let provider_age = now
        .signed_duration_since(quote.provider_timestamp)
        .num_seconds()
        .max(0);
    let receipt_age = now
        .signed_duration_since(quote.received_at)
        .num_seconds()
        .max(0);
    let age_seconds = provider_age.max(receipt_age);
    let (fresh_seconds, stale_seconds, fresh) = match &quote.instrument_id {
        InstrumentId::Crypto { .. } => (5, 15, Freshness::Live),
        InstrumentId::Equity { .. } => (30, 120, Freshness::Fresh),
    };

    if age_seconds <= fresh_seconds {
        fresh
    } else if age_seconds > stale_seconds {
        Freshness::Stale
    } else {
        Freshness::Aging
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DataSource, InstrumentId, MarketState, Quote};
    use chrono::{TimeZone, Utc};

    #[test]
    fn crypto_becomes_stale_after_fifteen_seconds() {
        let quote = crypto_quote_at(ts(1_000));
        assert_eq!(classify_freshness(&quote, ts(1_005)), Freshness::Live);
        assert_eq!(classify_freshness(&quote, ts(1_014)), Freshness::Aging);
        assert_eq!(classify_freshness(&quote, ts(1_016)), Freshness::Stale);
    }

    #[test]
    fn closed_equity_is_not_marked_stale() {
        let quote = closed_equity_quote_at(ts(1_000));
        assert_eq!(classify_freshness(&quote, ts(9_000)), Freshness::Closed);
    }

    #[test]
    fn provider_delayed_quote_is_delayed_regardless_of_age() {
        let mut quote = crypto_quote_at(ts(1_000));
        quote.provider_delayed = true;
        assert_eq!(classify_freshness(&quote, ts(1_001)), Freshness::Delayed);
    }

    #[test]
    fn old_provider_timestamp_is_stale_even_when_recently_received() {
        let mut quote = crypto_quote_at(ts(1_000));
        quote.received_at = ts(2_000);
        assert_eq!(classify_freshness(&quote, ts(2_001)), Freshness::Stale);
    }

    #[test]
    fn future_timestamps_are_clamped_to_a_live_zero_age() {
        let quote = crypto_quote_at(ts(2_000));
        assert_eq!(classify_freshness(&quote, ts(1_000)), Freshness::Live);
    }

    fn crypto_quote_at(timestamp: chrono::DateTime<Utc>) -> Quote {
        quote_at(
            timestamp,
            InstrumentId::crypto("BTC", "USDT").expect("instrument"),
            MarketState::Continuous,
        )
    }

    fn closed_equity_quote_at(timestamp: chrono::DateTime<Utc>) -> Quote {
        quote_at(
            timestamp,
            InstrumentId::equity("AAPL", Some("NASDAQ")).expect("instrument"),
            MarketState::Closed,
        )
    }

    fn quote_at(
        timestamp: chrono::DateTime<Utc>,
        instrument_id: InstrumentId,
        market_state: MarketState,
    ) -> Quote {
        Quote {
            instrument_id,
            last: 1.0,
            previous_close: None,
            open: None,
            high: None,
            low: None,
            volume: None,
            currency: "USD".into(),
            source: DataSource::Binance,
            market_state,
            provider_timestamp: timestamp,
            received_at: timestamp,
            provider_delayed: false,
        }
    }

    fn ts(seconds: i64) -> chrono::DateTime<Utc> {
        Utc.timestamp_opt(seconds, 0).single().expect("timestamp")
    }
}
