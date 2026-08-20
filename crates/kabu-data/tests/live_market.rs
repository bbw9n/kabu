use chrono::{DateTime, Duration, Utc};
use kabu_core::{DataSource, Instrument, InstrumentId, Quote, QuoteSnapshots};
use kabu_data::{BinanceClient, YahooClient};

const YEAR_2000_UNIX_SECONDS: i64 = 946_684_800;

#[tokio::test]
#[ignore = "requires KABU_LIVE_TESTS=1 and public Yahoo access"]
async fn live_yahoo_snapshot_contract() {
    if !live_tests_enabled() {
        eprintln!("skipped: set KABU_LIVE_TESTS=1 to contact Yahoo");
        return;
    }

    let instrument = Instrument::equity("AAPL", Some("NASDAQ"), "Apple Inc.", "USD")
        .expect("valid AAPL instrument");
    let client = YahooClient::production().expect("production Yahoo client");
    let quotes = client
        .snapshots(std::slice::from_ref(&instrument))
        .await
        .expect("Yahoo AAPL snapshot contract");

    let quote = quotes.first().expect("Yahoo returned an AAPL quote");
    assert_live_quote(quote, &instrument.id, DataSource::Yahoo, "USD");
}

#[tokio::test]
#[ignore = "requires KABU_LIVE_TESTS=1 and public Binance access"]
async fn live_binance_snapshot_contract() {
    if !live_tests_enabled() {
        eprintln!("skipped: set KABU_LIVE_TESTS=1 to contact Binance");
        return;
    }

    let instrument =
        Instrument::crypto("BTC", "USDT", "Bitcoin").expect("valid BTC/USDT instrument");
    let client = BinanceClient::production(None).expect("production Binance client");
    let quotes = client
        .snapshots(std::slice::from_ref(&instrument))
        .await
        .expect("Binance BTC/USDT snapshot contract");

    let quote = quotes.first().expect("Binance returned a BTC/USDT quote");
    assert_live_quote(quote, &instrument.id, DataSource::Binance, "USDT");
}

fn live_tests_enabled() -> bool {
    std::env::var("KABU_LIVE_TESTS").as_deref() == Ok("1")
}

fn assert_live_quote(
    quote: &Quote,
    expected_instrument: &InstrumentId,
    expected_source: DataSource,
    expected_currency: &str,
) {
    assert_eq!(&quote.instrument_id, expected_instrument);
    assert_eq!(quote.source, expected_source);
    assert_eq!(quote.currency, expected_currency);
    assert!(quote.last.is_finite(), "last price must be finite");
    assert!(quote.last > 0.0, "last price must be positive");
    assert_meaningful_timestamp("provider", quote.provider_timestamp);
    assert_meaningful_timestamp("receipt", quote.received_at);
    assert!(
        quote.provider_timestamp <= quote.received_at + Duration::minutes(5),
        "provider timestamp must not be implausibly ahead of receipt time"
    );
}

fn assert_meaningful_timestamp(label: &str, timestamp: DateTime<Utc>) {
    assert!(
        timestamp.timestamp() >= YEAR_2000_UNIX_SECONDS,
        "{label} timestamp must be from 2000 or later"
    );
}
