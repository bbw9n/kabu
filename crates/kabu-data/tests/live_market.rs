use chrono::{DateTime, Duration, Utc};
use kabu_core::{
    AssetClass, DataSource, Instrument, InstrumentId, InstrumentSearch, Quote, QuoteSnapshots,
};
use kabu_data::{BinanceClient, YahooClient};

const YEAR_2000_UNIX_SECONDS: i64 = 946_684_800;
const CLOCK_TOLERANCE: Duration = Duration::seconds(5);
const CRYPTO_MAX_PROVIDER_AGE: Duration = Duration::minutes(10);
const EQUITY_MAX_PROVIDER_AGE: Duration = Duration::days(10);

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
    let request_started = Utc::now();
    let quotes = client
        .snapshots(std::slice::from_ref(&instrument))
        .await
        .expect("Yahoo AAPL snapshot contract");
    let request_finished = Utc::now();

    assert_eq!(quotes.len(), 1, "Yahoo must return exactly one AAPL quote");
    assert_live_quote(
        &quotes[0],
        &instrument.id,
        DataSource::Yahoo,
        "USD",
        request_started,
        request_finished,
        EQUITY_MAX_PROVIDER_AGE,
    );
}

#[tokio::test]
#[ignore = "requires KABU_LIVE_TESTS=1 and public Yahoo access"]
async fn live_yahoo_search_contract() {
    if !live_tests_enabled() {
        eprintln!("skipped: set KABU_LIVE_TESTS=1 to contact Yahoo");
        return;
    }

    let client = YahooClient::production().expect("production Yahoo client");
    let results = client
        .search("AAPL")
        .await
        .expect("Yahoo AAPL search contract");
    let aapl = results
        .iter()
        .find(|result| result.instrument.id.canonical_symbol() == "AAPL")
        .expect("Yahoo search returned AAPL");

    assert_eq!(aapl.instrument.id.asset_class(), AssetClass::Equity);
    assert_eq!(aapl.instrument.currency, "USD");
    assert_eq!(aapl.source, DataSource::Yahoo);
    assert!(!aapl.instrument.display_name.trim().is_empty());
    assert!(!aapl.venue_label.trim().is_empty());
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
    let request_started = Utc::now();
    let quotes = client
        .snapshots(std::slice::from_ref(&instrument))
        .await
        .expect("Binance BTC/USDT snapshot contract");
    let request_finished = Utc::now();

    assert_eq!(
        quotes.len(),
        1,
        "Binance must return exactly one BTC/USDT quote"
    );
    assert_live_quote(
        &quotes[0],
        &instrument.id,
        DataSource::Binance,
        "USDT",
        request_started,
        request_finished,
        CRYPTO_MAX_PROVIDER_AGE,
    );
}

fn live_tests_enabled() -> bool {
    std::env::var("KABU_LIVE_TESTS").as_deref() == Ok("1")
}

fn assert_live_quote(
    quote: &Quote,
    expected_instrument: &InstrumentId,
    expected_source: DataSource,
    expected_currency: &str,
    request_started: DateTime<Utc>,
    request_finished: DateTime<Utc>,
    max_provider_age: Duration,
) {
    assert_eq!(&quote.instrument_id, expected_instrument);
    assert_eq!(quote.source, expected_source);
    assert_eq!(quote.currency, expected_currency);
    assert!(quote.last.is_finite(), "last price must be finite");
    assert!(quote.last > 0.0, "last price must be positive");
    assert_meaningful_timestamp("provider", quote.provider_timestamp);
    assert_meaningful_timestamp("receipt", quote.received_at);
    assert!(
        quote.received_at >= request_started - CLOCK_TOLERANCE,
        "receipt timestamp must not precede the request interval"
    );
    assert!(
        quote.received_at <= request_finished + CLOCK_TOLERANCE,
        "receipt timestamp must not follow the request interval"
    );
    assert!(
        quote.provider_timestamp <= request_finished + CLOCK_TOLERANCE,
        "provider timestamp must not be in the future"
    );
    assert!(
        quote.provider_timestamp >= request_finished - max_provider_age - CLOCK_TOLERANCE,
        "provider timestamp is older than the market-aware live bound"
    );
}

fn assert_meaningful_timestamp(label: &str, timestamp: DateTime<Utc>) {
    assert!(
        timestamp.timestamp() >= YEAR_2000_UNIX_SECONDS,
        "{label} timestamp must be from 2000 or later"
    );
}
