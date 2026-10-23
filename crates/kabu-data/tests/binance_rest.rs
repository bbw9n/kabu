use chrono::{Duration, Utc};
use kabu_core::{
    CachedInstrumentCatalog, ChartHistory, ChartPeriod, DataSource, Instrument, InstrumentSearch,
    MarketDataErrorKind, QuoteSnapshots,
};
use kabu_data::BinanceClient;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path, query_param},
};

#[tokio::test]
async fn base_search_prefers_active_usdt_pair() {
    let client = binance_with_fixture_server().await;

    client.refresh_catalog().await.expect("catalog");
    let results = client.search("btc").await.expect("search");

    assert_eq!(results[0].instrument.id.canonical_symbol(), "BTC/USDT");
    assert_eq!(results[0].source, DataSource::Binance);
}

#[tokio::test]
async fn exact_wire_symbol_search_matches_catalog_entry() {
    let client = binance_with_fixture_server().await;

    client.refresh_catalog().await.expect("catalog");
    let results = client.search("BTCUSDC").await.expect("search");

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].instrument.id.canonical_symbol(), "BTC/USDC");
}

#[tokio::test]
async fn exact_canonical_pair_search_matches_catalog_entry() {
    let client = binance_with_fixture_server().await;

    client.refresh_catalog().await.expect("catalog");
    let results = client.search("btc/usdc").await.expect("search");

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].instrument.id.canonical_symbol(), "BTC/USDC");
}

#[tokio::test]
async fn asset_name_substring_search_matches_catalog_entry() {
    let client = binance_with_fixture_server().await;

    client.refresh_catalog().await.expect("catalog");
    let results = client.search("coin").await.expect("search");

    assert!(
        results
            .iter()
            .any(|result| result.instrument.id.canonical_symbol() == "BTC/USDT")
    );
}

#[tokio::test]
async fn ticker_preserves_base_quote_identity() {
    let client = binance_with_fixture_server().await;
    let btc = btc_instrument();

    let quotes = client.snapshots(&[btc]).await.expect("quotes");

    assert_eq!(quotes.len(), 1);
    assert_eq!(quotes[0].instrument_id.canonical_symbol(), "BTC/USDT");
    assert_eq!(quotes[0].last, 62_345.67);
    assert_eq!(quotes[0].previous_close, Some(61_000.0));
    assert_eq!(quotes[0].open, Some(61_000.0));
    assert_eq!(quotes[0].high, Some(63_000.0));
    assert_eq!(quotes[0].low, Some(60_500.0));
    assert_eq!(quotes[0].volume, Some(1_234.567));
    assert_eq!(quotes[0].currency, "USDT");
    assert_eq!(quotes[0].market_state, kabu_core::MarketState::Continuous);
    assert_eq!(
        quotes[0].provider_timestamp.timestamp_millis(),
        1_724_171_400_000
    );
    assert_eq!(quotes[0].source, DataSource::Binance);
}

#[tokio::test]
async fn history_maps_klines_in_timestamp_order() {
    let client = binance_with_fixture_server().await;

    let candles = client
        .history(&btc_instrument(), ChartPeriod::OneDay)
        .await
        .expect("history");

    assert_eq!(candles.len(), 3);
    assert!(
        candles
            .windows(2)
            .all(|pair| pair[0].starts_at < pair[1].starts_at)
    );
    assert_eq!(candles[2].close, 62_345.67);
}

#[tokio::test]
async fn malformed_numeric_ticker_is_rejected() {
    let server = MockServer::start().await;
    mount_ticker(
        &server,
        r#"{"symbol":"BTCUSDT","lastPrice":"not-a-number"}"#,
    )
    .await;
    let client = client_with_catalog(&server, fresh_catalog());

    let error = client
        .snapshots(&[btc_instrument()])
        .await
        .expect_err("must reject malformed number");

    assert_eq!(error.kind, MarketDataErrorKind::MalformedResponse);
}

#[tokio::test]
async fn fresh_initial_catalog_avoids_exchange_info_request() {
    let server = MockServer::start().await;
    let client = client_with_catalog(&server, fresh_catalog());

    client
        .refresh_catalog()
        .await
        .expect("fresh catalog remains usable");
    let results = client.search("btc").await.expect("search cached catalog");

    assert_eq!(results[0].instrument.id.canonical_symbol(), "BTC/USDT");
}

#[tokio::test]
async fn stale_initial_catalog_refreshes_from_exchange_info() {
    let server = MockServer::start().await;
    mount_exchange_info(&server).await;
    let mut stale_catalog = fresh_catalog();
    stale_catalog.fetched_at = Utc::now() - Duration::hours(25);
    let client = client_with_catalog(&server, stale_catalog);

    client
        .refresh_catalog()
        .await
        .expect("stale catalog refreshes");
    let results = client
        .search("eth")
        .await
        .expect("search refreshed catalog");

    assert_eq!(results[0].instrument.id.canonical_symbol(), "ETH/USDT");
}

async fn binance_with_fixture_server() -> BinanceClient {
    let server = MockServer::start().await;
    mount_exchange_info(&server).await;
    mount_ticker(&server, include_str!("fixtures/binance_ticker.json")).await;
    Mock::given(method("GET"))
        .and(path("/api/v3/klines"))
        .and(query_param("symbol", "BTCUSDT"))
        .and(query_param("interval", "5m"))
        .and(query_param("limit", "288"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            include_str!("fixtures/binance_klines.json"),
            "application/json",
        ))
        .mount(&server)
        .await;
    BinanceClient::new(
        test_http(),
        server.uri().parse().expect("REST URL"),
        server.uri().parse().expect("stream URL"),
        None,
    )
}

fn client_with_catalog(server: &MockServer, catalog: CachedInstrumentCatalog) -> BinanceClient {
    BinanceClient::new(
        test_http(),
        server.uri().parse().expect("REST URL"),
        server.uri().parse().expect("stream URL"),
        Some(catalog),
    )
}

fn fresh_catalog() -> CachedInstrumentCatalog {
    CachedInstrumentCatalog {
        source: DataSource::Binance,
        fetched_at: Utc::now(),
        instruments: vec![btc_instrument()],
    }
}

fn btc_instrument() -> Instrument {
    Instrument::crypto("BTC", "USDT", "Bitcoin").expect("instrument")
}

async fn mount_exchange_info(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/api/v3/exchangeInfo"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            include_str!("fixtures/binance_exchange_info.json"),
            "application/json",
        ))
        .mount(server)
        .await;
}

async fn mount_ticker(server: &MockServer, body: &str) {
    Mock::given(method("GET"))
        .and(path("/api/v3/ticker/24hr"))
        .and(query_param("symbol", "BTCUSDT"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(body, "application/json"))
        .mount(server)
        .await;
}

fn test_http() -> reqwest::Client {
    reqwest::Client::builder().build().expect("test client")
}
