use kabu_app::views::{
    ChartDirection, QuoteDetailData, freshness_label, normalize_closes, series_direction,
};
use kabu_core::{Candle, DataSource, Freshness, Instrument, MarketState, Quote};
use std::time::Duration;

#[test]
fn flat_series_is_centered_and_finite() {
    let points = normalize_closes(&[candle(42.0), candle(42.0), candle(42.0)]);

    assert_eq!(points, vec![(0.0, 0.5), (0.5, 0.5), (1.0, 0.5)]);
    assert!(points.iter().all(|(x, y)| x.is_finite() && y.is_finite()));
}

#[test]
fn empty_and_single_series_are_safe() {
    assert!(normalize_closes(&[]).is_empty());
    assert_eq!(normalize_closes(&[candle(10.0)]), vec![(0.5, 0.5)]);
}

#[test]
fn varying_series_reserves_eight_percent_vertical_padding() {
    let points = normalize_closes(&[candle(10.0), candle(15.0), candle(20.0)]);

    assert_eq!(points, vec![(0.0, 0.92), (0.5, 0.5), (1.0, 0.08)]);
}

#[test]
fn non_finite_closes_are_discarded_before_normalizing_and_choosing_color() {
    let candles = [
        candle(f64::NAN),
        candle(20.0),
        candle(f64::INFINITY),
        candle(10.0),
    ];

    assert_eq!(normalize_closes(&candles), vec![(0.0, 0.08), (1.0, 0.92)]);
    assert_eq!(series_direction(&candles), Some(ChartDirection::Loss));
}

#[test]
fn chart_direction_uses_absolute_first_to_last_series_change() {
    assert_eq!(
        series_direction(&[candle(100.0), candle(90.0), candle(110.0)]),
        Some(ChartDirection::Gain)
    );
    assert_eq!(series_direction(&[]), None);
}

#[test]
fn freshness_labels_are_exact_and_aging_includes_the_age() {
    let age = Duration::from_secs(47);
    let cases = [
        (Freshness::Live, "Live"),
        (Freshness::Fresh, "Fresh"),
        (Freshness::Aging, "Aging · 47s"),
        (Freshness::Delayed, "Delayed"),
        (Freshness::Stale, "Stale"),
        (Freshness::Closed, "Closed"),
        (Freshness::Unavailable, "Unavailable"),
    ];

    for (freshness, expected) in cases {
        assert_eq!(freshness_label(freshness, age), expected);
    }
}

#[test]
fn quote_detail_uses_em_dashes_for_missing_metrics() {
    let instrument =
        Instrument::equity("AAPL", Some("NASDAQ"), "Apple", "USD").expect("instrument");
    let timestamp = chrono::DateTime::from_timestamp(1_700_000_000, 0).expect("timestamp");
    let quote = Quote {
        instrument_id: instrument.id.clone(),
        last: 101.5,
        previous_close: None,
        open: None,
        high: None,
        low: None,
        volume: None,
        currency: "USD".into(),
        source: DataSource::Yahoo,
        market_state: MarketState::Regular,
        provider_timestamp: timestamp,
        received_at: timestamp,
        provider_delayed: false,
    };

    let data = QuoteDetailData::from_quote(&instrument, Some(&quote), timestamp);

    assert_eq!(data.price, "101.50 USD");
    assert_eq!(data.change, "—");
    assert_eq!(data.open, "—");
    assert_eq!(data.high, "—");
    assert_eq!(data.low, "—");
    assert_eq!(data.previous_close, "—");
    assert_eq!(data.volume, "—");
}

fn candle(close: f64) -> Candle {
    Candle {
        starts_at: chrono::DateTime::from_timestamp(1_700_000_000, 0).expect("timestamp"),
        open: close,
        high: close,
        low: close,
        close,
        volume: 1_000.0,
    }
}
