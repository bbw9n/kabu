mod wire;

use chrono::{DateTime, Utc};
use kabu_core::{
    Candle, ChartHistory, ChartPeriod, DataSource, Instrument, InstrumentSearch, MarketDataError,
    MarketDataErrorKind, MarketState, Quote, QuoteSnapshots, SearchResult,
};
use reqwest::Url;

use crate::http::{classify_request_error, decode_json, malformed_response};
use wire::{ChartMeta, ChartQuote, ChartResponse, ChartResult, SearchQuote, SearchResponse};

const SEARCH_ENDPOINT: &str = "search";
const CHART_ENDPOINT: &str = "chart";

#[derive(Clone, Debug)]
pub struct YahooClient {
    http: reqwest::Client,
    base_url: Url,
}

impl YahooClient {
    pub fn new(http: reqwest::Client, base_url: Url) -> Self {
        Self { http, base_url }
    }

    pub fn production() -> Result<Self, reqwest::Error> {
        Ok(Self::new(
            crate::http::client()?,
            "https://query1.finance.yahoo.com/"
                .parse()
                .expect("Yahoo base URL is valid"),
        ))
    }

    async fn search_response(&self, query: &str) -> Result<SearchResponse, MarketDataError> {
        let mut url = self.endpoint("v1/finance/search")?;
        url.query_pairs_mut()
            .append_pair("q", query)
            .append_pair("quotesCount", "12")
            .append_pair("newsCount", "0");
        let response = self
            .http
            .get(url)
            .send()
            .await
            .map_err(|error| classify_request_error(error, SEARCH_ENDPOINT))?;
        decode_json(response, SEARCH_ENDPOINT).await
    }

    async fn chart_response(
        &self,
        instrument: &Instrument,
        period: ChartPeriod,
    ) -> Result<ChartResult, MarketDataError> {
        let symbol = yahoo_symbol(instrument)?;
        let mut url = self.endpoint(&format!("v8/finance/chart/{symbol}"))?;
        let (range, interval) = chart_parameters(period);
        url.query_pairs_mut()
            .append_pair("range", range)
            .append_pair("interval", interval);
        let response = self
            .http
            .get(url)
            .send()
            .await
            .map_err(|error| classify_request_error(error, CHART_ENDPOINT))?;
        let response: ChartResponse = decode_json(response, CHART_ENDPOINT).await?;
        response
            .chart
            .result
            .and_then(|mut results| results.drain(..).next())
            .ok_or_else(|| malformed_response(CHART_ENDPOINT))
    }

    fn endpoint(&self, path: &str) -> Result<Url, MarketDataError> {
        self.base_url.join(path).map_err(|_| MarketDataError {
            kind: MarketDataErrorKind::Internal,
            message: "Yahoo endpoint configuration is invalid".into(),
            retry_after: None,
        })
    }
}

#[async_trait::async_trait]
impl InstrumentSearch for YahooClient {
    async fn search(&self, query: &str) -> Result<Vec<SearchResult>, MarketDataError> {
        self.search_response(query)
            .await?
            .quotes
            .into_iter()
            .filter(is_supported_equity)
            .map(map_search_quote)
            .collect()
    }
}

#[async_trait::async_trait]
impl QuoteSnapshots for YahooClient {
    async fn snapshots(&self, instruments: &[Instrument]) -> Result<Vec<Quote>, MarketDataError> {
        let mut quotes = Vec::with_capacity(instruments.len());
        for instrument in instruments {
            let chart = self.chart_response(instrument, ChartPeriod::OneDay).await?;
            quotes.push(map_snapshot(instrument, &chart)?);
        }
        Ok(quotes)
    }
}

#[async_trait::async_trait]
impl ChartHistory for YahooClient {
    async fn history(
        &self,
        instrument: &Instrument,
        period: ChartPeriod,
    ) -> Result<Vec<Candle>, MarketDataError> {
        let chart = self.chart_response(instrument, period).await?;
        map_candles(&chart)
    }
}

fn is_supported_equity(quote: &SearchQuote) -> bool {
    quote.quote_type.as_deref() == Some("EQUITY")
        && matches!(
            quote.exchange.as_deref(),
            Some("NMS" | "NYQ" | "NGM" | "NCM" | "ASE" | "PNK" | "OQX")
        )
}

fn map_search_quote(quote: SearchQuote) -> Result<SearchResult, MarketDataError> {
    let symbol = quote
        .symbol
        .as_deref()
        .ok_or_else(|| malformed_response(SEARCH_ENDPOINT))?;
    let name = quote
        .short_name
        .as_deref()
        .filter(|name| !name.trim().is_empty())
        .or_else(|| {
            quote
                .long_name
                .as_deref()
                .filter(|name| !name.trim().is_empty())
        })
        .ok_or_else(|| malformed_response(SEARCH_ENDPOINT))?;
    let exchange = quote
        .exchange
        .as_deref()
        .ok_or_else(|| malformed_response(SEARCH_ENDPOINT))?;
    // Kabu admits only the supported US equity venues above. Yahoo commonly
    // sends null currency in search responses, so USD is the provider-neutral
    // default for this already-restricted result set.
    let currency = quote
        .currency
        .as_deref()
        .filter(|currency| !currency.trim().is_empty())
        .unwrap_or("USD");
    let instrument = Instrument::equity(symbol, Some(exchange), name, currency)
        .map_err(|_| malformed_response(SEARCH_ENDPOINT))?;
    Ok(SearchResult {
        instrument,
        source: DataSource::Yahoo,
        venue_label: exchange.to_owned(),
    })
}

fn yahoo_symbol(instrument: &Instrument) -> Result<String, MarketDataError> {
    instrument
        .id
        .provider_symbol(DataSource::Yahoo)
        .ok_or_else(|| MarketDataError {
            kind: MarketDataErrorKind::UnsupportedInstrument,
            message: "chart only supports Yahoo equity instruments".into(),
            retry_after: None,
        })
}

fn chart_parameters(period: ChartPeriod) -> (&'static str, &'static str) {
    match period {
        ChartPeriod::OneDay => ("1d", "1m"),
        ChartPeriod::FiveDays => ("5d", "5m"),
        ChartPeriod::OneMonth => ("1mo", "30m"),
        ChartPeriod::SixMonths => ("6mo", "1d"),
        ChartPeriod::OneYear => ("1y", "1d"),
    }
}

fn map_snapshot(instrument: &Instrument, chart: &ChartResult) -> Result<Quote, MarketDataError> {
    let candles = map_candles(chart)?;
    let newest = candles
        .last()
        .ok_or_else(|| malformed_response(CHART_ENDPOINT))?;
    let provider_timestamp = timestamp(chart.meta.regular_market_time)?;
    Ok(Quote {
        instrument_id: instrument.id.clone(),
        last: newest.close,
        previous_close: previous_close(&chart.meta)?,
        open: Some(newest.open),
        high: Some(newest.high),
        low: Some(newest.low),
        volume: Some(newest.volume),
        currency: validated_text(&chart.meta.currency)?,
        source: DataSource::Yahoo,
        market_state: market_state(&chart.meta),
        provider_timestamp,
        received_at: Utc::now(),
        provider_delayed: false,
    })
}

fn map_candles(chart: &ChartResult) -> Result<Vec<Candle>, MarketDataError> {
    let quote = chart
        .indicators
        .quote
        .first()
        .ok_or_else(|| malformed_response(CHART_ENDPOINT))?;
    validate_chart_lengths(&chart.timestamp, quote)?;

    chart
        .timestamp
        .iter()
        .zip(&quote.open)
        .zip(&quote.high)
        .zip(&quote.low)
        .zip(&quote.close)
        .zip(&quote.volume)
        .filter_map(|(((((timestamp, open), high), low), close), volume)| {
            let (open, high, low, close, volume) =
                ((*open)?, (*high)?, (*low)?, (*close)?, (*volume)?);
            Some((*timestamp, open, high, low, close, volume))
        })
        .map(|(timestamp_value, open, high, low, close, volume)| {
            let starts_at = timestamp(timestamp_value)?;
            if [open, high, low, close, volume]
                .into_iter()
                .all(f64::is_finite)
            {
                Ok(Candle {
                    starts_at,
                    open,
                    high,
                    low,
                    close,
                    volume,
                })
            } else {
                Err(malformed_response(CHART_ENDPOINT))
            }
        })
        .collect()
}

fn validate_chart_lengths(timestamps: &[i64], quote: &ChartQuote) -> Result<(), MarketDataError> {
    let expected = timestamps.len();
    if [
        quote.open.len(),
        quote.high.len(),
        quote.low.len(),
        quote.close.len(),
        quote.volume.len(),
    ]
    .into_iter()
    .all(|length| length == expected)
    {
        Ok(())
    } else {
        Err(malformed_response(CHART_ENDPOINT))
    }
}

fn timestamp(value: i64) -> Result<DateTime<Utc>, MarketDataError> {
    DateTime::from_timestamp(value, 0).ok_or_else(|| malformed_response(CHART_ENDPOINT))
}

fn finite_optional(value: Option<f64>) -> Result<Option<f64>, MarketDataError> {
    match value {
        Some(value) if value.is_finite() => Ok(Some(value)),
        Some(_) => Err(malformed_response(CHART_ENDPOINT)),
        None => Ok(None),
    }
}

fn previous_close(meta: &ChartMeta) -> Result<Option<f64>, MarketDataError> {
    // Yahoo may emit both fields. `previousClose` is the existing quote-level
    // contract, so it wins when present; `chartPreviousClose` is its chart-v8
    // compatibility fallback.
    finite_optional(meta.previous_close.or(meta.chart_previous_close))
}

fn validated_text(value: &str) -> Result<String, MarketDataError> {
    let value = value.trim();
    if value.is_empty() {
        Err(malformed_response(CHART_ENDPOINT))
    } else {
        Ok(value.to_owned())
    }
}

fn market_state(meta: &ChartMeta) -> MarketState {
    match meta.market_state.as_deref() {
        Some("PRE" | "PREPRE") => MarketState::PreMarket,
        Some("REGULAR") => MarketState::Regular,
        Some("POST" | "POSTPOST") => MarketState::PostMarket,
        Some("CLOSED") => MarketState::Closed,
        _ => MarketState::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::chart_parameters;
    use kabu_core::ChartPeriod;

    #[test]
    fn chart_periods_use_yahoo_ranges_and_intervals() {
        assert_eq!(chart_parameters(ChartPeriod::OneDay), ("1d", "1m"));
        assert_eq!(chart_parameters(ChartPeriod::FiveDays), ("5d", "5m"));
        assert_eq!(chart_parameters(ChartPeriod::OneMonth), ("1mo", "30m"));
        assert_eq!(chart_parameters(ChartPeriod::SixMonths), ("6mo", "1d"));
        assert_eq!(chart_parameters(ChartPeriod::OneYear), ("1y", "1d"));
    }
}
