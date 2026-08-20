use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub(super) struct SearchResponse {
    pub quotes: Vec<SearchQuote>,
}

#[derive(Debug, Deserialize)]
pub(super) struct SearchQuote {
    pub symbol: Option<String>,
    #[serde(rename = "shortname")]
    pub short_name: Option<String>,
    #[serde(rename = "longname")]
    pub long_name: Option<String>,
    #[serde(rename = "quoteType")]
    pub quote_type: Option<String>,
    pub exchange: Option<String>,
    pub currency: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(super) struct ChartResponse {
    pub chart: ChartEnvelope,
}

#[derive(Debug, Deserialize)]
pub(super) struct ChartEnvelope {
    pub result: Option<Vec<ChartResult>>,
}

#[derive(Debug, Deserialize)]
pub(super) struct ChartResult {
    pub meta: ChartMeta,
    pub timestamp: Vec<i64>,
    pub indicators: ChartIndicators,
}

#[derive(Debug, Deserialize)]
pub(super) struct ChartMeta {
    pub currency: String,
    #[serde(rename = "exchangeTimezoneName")]
    pub _exchange_timezone_name: Option<String>,
    #[serde(rename = "marketState")]
    pub market_state: Option<String>,
    #[serde(rename = "regularMarketTime")]
    pub regular_market_time: i64,
    #[serde(rename = "dataGranularity")]
    pub _data_granularity: Option<String>,
    #[serde(rename = "previousClose")]
    pub previous_close: Option<f64>,
    #[serde(rename = "chartPreviousClose")]
    pub chart_previous_close: Option<f64>,
}

#[derive(Debug, Deserialize)]
pub(super) struct ChartIndicators {
    pub quote: Vec<ChartQuote>,
}

#[derive(Debug, Deserialize)]
pub(super) struct ChartQuote {
    pub open: Vec<Option<f64>>,
    pub high: Vec<Option<f64>>,
    pub low: Vec<Option<f64>>,
    pub close: Vec<Option<f64>>,
    pub volume: Vec<Option<f64>>,
}
