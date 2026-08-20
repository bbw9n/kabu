use std::time::Duration;

use kabu_core::{
    AssetClass, ChartHistory, ChartPeriod, DataSource, Instrument, InstrumentSearch,
    MarketDataErrorKind, QuoteSnapshots,
};
use kabu_data::YahooClient;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path, query_param},
};

#[tokio::test]
async fn search_maps_only_supported_us_equities() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/finance/search"))
        .and(query_param("q", "nvda"))
        .and(query_param("quotesCount", "12"))
        .and(query_param("newsCount", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            include_str!("fixtures/yahoo_search.json"),
            "application/json",
        ))
        .mount(&server)
        .await;
    let client = YahooClient::new(test_http(), server.uri().parse().expect("url"));

    let results = client.search("nvda").await.expect("search");

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].instrument.id.canonical_symbol(), "NVDA");
    assert_eq!(results[0].instrument.id.asset_class(), AssetClass::Equity);
    assert!(matches!(
        results[0].instrument.id,
        kabu_core::InstrumentId::Equity { ref venue, .. } if venue.as_deref() == Some("NASDAQ")
    ));
    assert_eq!(results[0].source, DataSource::Yahoo);
}

#[tokio::test]
async fn search_accepts_both_names_null_currency_and_sparse_unsupported_results() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/finance/search"))
        .and(query_param("q", "msft"))
        .and(query_param("quotesCount", "12"))
        .and(query_param("newsCount", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            include_str!("fixtures/yahoo_search_compatible.json"),
            "application/json",
        ))
        .mount(&server)
        .await;
    let client = YahooClient::new(test_http(), server.uri().parse().expect("url"));

    let results = client.search("msft").await.expect("search");

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].instrument.id.canonical_symbol(), "MSFT");
    assert_eq!(results[0].instrument.display_name, "Microsoft short name");
    assert_eq!(results[0].instrument.currency, "USD");
    assert_eq!(results[1].instrument.id.canonical_symbol(), "ACME");
    assert_eq!(results[1].instrument.display_name, "Acme Corporation");
    assert_eq!(results[1].instrument.currency, "USD");
}

#[tokio::test]
async fn history_maps_complete_candles_in_timestamp_order() {
    let server = MockServer::start().await;
    mount_chart_json(
        &server,
        "/v8/finance/chart/NVDA",
        "1d",
        "1m",
        include_str!("fixtures/yahoo_chart.json"),
    )
    .await;
    let client = YahooClient::new(test_http(), server.uri().parse().expect("url"));
    let instrument =
        Instrument::equity("NVDA", Some("NASDAQ"), "NVIDIA", "USD").expect("instrument");

    let candles = client
        .history(&instrument, ChartPeriod::OneDay)
        .await
        .expect("history");

    assert_eq!(candles.len(), 3);
    assert!(
        candles
            .windows(2)
            .all(|pair| pair[0].starts_at < pair[1].starts_at)
    );
    assert_eq!(candles[2].close, 123.6);
}

#[tokio::test]
async fn snapshots_use_newest_complete_candle_and_provider_metadata() {
    let server = MockServer::start().await;
    mount_chart_json(
        &server,
        "/v8/finance/chart/NVDA",
        "1d",
        "1m",
        include_str!("fixtures/yahoo_chart.json"),
    )
    .await;
    let client = YahooClient::new(test_http(), server.uri().parse().expect("url"));
    let instrument =
        Instrument::equity("NVDA", Some("NASDAQ"), "NVIDIA", "USD").expect("instrument");

    let quotes = client.snapshots(&[instrument]).await.expect("snapshot");

    assert_eq!(quotes.len(), 1);
    assert_eq!(quotes[0].last, 123.6);
    assert_eq!(quotes[0].previous_close, Some(123.0));
    assert_eq!(quotes[0].market_state, kabu_core::MarketState::Regular);
    assert_eq!(quotes[0].provider_timestamp.timestamp(), 1724171400);
    assert_eq!(quotes[0].currency, "USD");
    assert_eq!(quotes[0].source, DataSource::Yahoo);
}

#[tokio::test]
async fn snapshots_accept_absent_optional_metadata_and_chart_previous_close() {
    let server = MockServer::start().await;
    mount_chart_json(
        &server,
        "/v8/finance/chart/NVDA",
        "1d",
        "1m",
        include_str!("fixtures/yahoo_chart_optional_absent.json"),
    )
    .await;
    let client = YahooClient::new(test_http(), server.uri().parse().expect("url"));
    let instrument =
        Instrument::equity("NVDA", Some("NASDAQ"), "NVIDIA", "USD").expect("instrument");

    let quotes = client.snapshots(&[instrument]).await.expect("snapshot");

    assert_eq!(quotes.len(), 1);
    assert_eq!(quotes[0].previous_close, Some(122.25));
    assert_eq!(quotes[0].market_state, kabu_core::MarketState::Unknown);
}

#[tokio::test]
async fn snapshots_accept_null_optional_metadata_and_prefer_previous_close() {
    let server = MockServer::start().await;
    mount_chart_json(
        &server,
        "/v8/finance/chart/NVDA",
        "1d",
        "1m",
        include_str!("fixtures/yahoo_chart_optional_null.json"),
    )
    .await;
    let client = YahooClient::new(test_http(), server.uri().parse().expect("url"));
    let instrument =
        Instrument::equity("NVDA", Some("NASDAQ"), "NVIDIA", "USD").expect("instrument");

    let quotes = client.snapshots(&[instrument]).await.expect("snapshot");

    assert_eq!(quotes.len(), 1);
    assert_eq!(quotes[0].previous_close, Some(123.0));
    assert_eq!(quotes[0].market_state, kabu_core::MarketState::Unknown);
}

#[tokio::test]
async fn rate_limit_exposes_retry_after() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/finance/search"))
        .respond_with(
            ResponseTemplate::new(429)
                .insert_header("Retry-After", "17")
                .set_body_string("provider response must not escape in errors"),
        )
        .mount(&server)
        .await;
    let client = YahooClient::new(test_http(), server.uri().parse().expect("url"));

    let error = client
        .search("private query")
        .await
        .expect_err("rate limited");

    assert_eq!(error.kind, MarketDataErrorKind::RateLimited);
    assert_eq!(error.retry_after, Some(Duration::from_secs(17)));
    assert!(!error.message.contains("private query"));
    assert!(!error.message.contains("provider response"));
}

#[tokio::test]
async fn mismatched_chart_vectors_are_rejected() {
    let server = MockServer::start().await;
    mount_chart_json(
        &server,
        "/v8/finance/chart/NVDA",
        "1d",
        "1m",
        r#"{
          "chart": {
            "result": [{
              "meta": {
                "currency":"USD",
                "exchangeTimezoneName":"America/New_York",
                "marketState":"REGULAR",
                "regularMarketTime":1724171400,
                "dataGranularity":"1m"
              },
              "timestamp": [1724171220, 1724171280],
              "indicators": {"quote": [{"open":[123.1], "high":[123.5,123.6], "low":[123.0,123.1], "close":[123.4,123.5], "volume":[100,200]}]}
            }],
            "error": null
          }
        }"#,
    )
    .await;
    let client = YahooClient::new(test_http(), server.uri().parse().expect("url"));
    let instrument =
        Instrument::equity("NVDA", Some("NASDAQ"), "NVIDIA", "USD").expect("instrument");

    let error = client
        .history(&instrument, ChartPeriod::OneDay)
        .await
        .expect_err("mismatched vectors must be rejected");

    assert_eq!(error.kind, MarketDataErrorKind::MalformedResponse);
}

#[tokio::test]
async fn mismatched_indicator_vectors_are_rejected() {
    let server = MockServer::start().await;
    mount_chart_json(
        &server,
        "/v8/finance/chart/NVDA",
        "1d",
        "1m",
        r#"{
          "chart": {
            "result": [{
              "meta": {
                "currency":"USD",
                "regularMarketTime":1724171400
              },
              "timestamp": [1724171220],
              "indicators": {"quote": [{"open":[123.1], "high":[123.5,123.6], "low":[123.0], "close":[123.4], "volume":[100]}]}
            }],
            "error": null
          }
        }"#,
    )
    .await;
    let client = YahooClient::new(test_http(), server.uri().parse().expect("url"));
    let instrument =
        Instrument::equity("NVDA", Some("NASDAQ"), "NVIDIA", "USD").expect("instrument");

    let error = client
        .history(&instrument, ChartPeriod::OneDay)
        .await
        .expect_err("mismatched vectors must be rejected");

    assert_eq!(error.kind, MarketDataErrorKind::MalformedResponse);
}

fn test_http() -> reqwest::Client {
    reqwest::Client::builder().build().expect("test client")
}

async fn mount_chart_json(
    server: &MockServer,
    request_path: &str,
    range: &str,
    interval: &str,
    body: &str,
) {
    Mock::given(method("GET"))
        .and(path(request_path))
        .and(query_param("range", range))
        .and(query_param("interval", interval))
        .respond_with(ResponseTemplate::new(200).set_body_raw(body, "application/json"))
        .mount(server)
        .await;
}
