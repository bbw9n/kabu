use chrono::{DateTime, Utc};
use kabu_core::{
    Candle, ChartHistory, ChartPeriod, DataSource, Instrument, MarketDataError, MarketState, Quote,
    QuoteSnapshots,
};
use serde_json::Value;

use crate::http::{classify_request_error, decode_json, malformed_response};

use super::{
    BinanceClient,
    catalog::{endpoint, wire_symbol},
    wire::{Klines, Ticker24Hour},
};

const TICKER_ENDPOINT: &str = "Binance ticker";
const KLINES_ENDPOINT: &str = "Binance klines";

#[async_trait::async_trait]
impl QuoteSnapshots for BinanceClient {
    async fn snapshots(&self, instruments: &[Instrument]) -> Result<Vec<Quote>, MarketDataError> {
        let mut quotes = Vec::with_capacity(instruments.len());
        for instrument in instruments {
            quotes.push(snapshot(self, instrument).await?);
        }
        Ok(quotes)
    }
}

#[async_trait::async_trait]
impl ChartHistory for BinanceClient {
    async fn history(
        &self,
        instrument: &Instrument,
        period: ChartPeriod,
    ) -> Result<Vec<Candle>, MarketDataError> {
        let symbol = wire_symbol(instrument)?;
        let (interval, limit) = chart_parameters(period);
        let mut url = endpoint(self, "api/v3/klines")?;
        url.query_pairs_mut()
            .append_pair("symbol", &symbol)
            .append_pair("interval", interval)
            .append_pair("limit", &limit.to_string());
        let response = self
            .http
            .get(url)
            .send()
            .await
            .map_err(|error| classify_request_error(error, KLINES_ENDPOINT))?;
        let klines: Klines = decode_json(response, KLINES_ENDPOINT).await?;
        klines
            .0
            .into_iter()
            .map(map_kline)
            .collect::<Result<Vec<_>, _>>()
    }
}

async fn snapshot(
    client: &BinanceClient,
    instrument: &Instrument,
) -> Result<Quote, MarketDataError> {
    let symbol = wire_symbol(instrument)?;
    let mut url = endpoint(client, "api/v3/ticker/24hr")?;
    url.query_pairs_mut().append_pair("symbol", &symbol);
    let response = client
        .http
        .get(url)
        .send()
        .await
        .map_err(|error| classify_request_error(error, TICKER_ENDPOINT))?;
    let ticker: Ticker24Hour = decode_json(response, TICKER_ENDPOINT).await?;
    if ticker.symbol != symbol {
        return Err(malformed_response(TICKER_ENDPOINT));
    }
    let currency = match &instrument.id {
        kabu_core::InstrumentId::Crypto { quote, .. } => quote.clone(),
        kabu_core::InstrumentId::Equity { .. } => return Err(malformed_response(TICKER_ENDPOINT)),
    };
    Ok(Quote {
        instrument_id: instrument.id.clone(),
        last: finite_number(&ticker.last_price, TICKER_ENDPOINT)?,
        previous_close: Some(finite_number(
            &ticker.previous_close_price,
            TICKER_ENDPOINT,
        )?),
        open: Some(finite_number(&ticker.open_price, TICKER_ENDPOINT)?),
        high: Some(finite_number(&ticker.high_price, TICKER_ENDPOINT)?),
        low: Some(finite_number(&ticker.low_price, TICKER_ENDPOINT)?),
        volume: Some(finite_number(&ticker.volume, TICKER_ENDPOINT)?),
        currency,
        source: DataSource::Binance,
        market_state: MarketState::Continuous,
        provider_timestamp: timestamp(ticker.close_time, TICKER_ENDPOINT)?,
        received_at: Utc::now(),
        provider_delayed: false,
    })
}

fn chart_parameters(period: ChartPeriod) -> (&'static str, usize) {
    match period {
        ChartPeriod::OneDay => ("5m", 288),
        ChartPeriod::FiveDays => ("30m", 240),
        ChartPeriod::OneMonth => ("2h", 360),
        ChartPeriod::SixMonths => ("1d", 180),
        ChartPeriod::OneYear => ("1d", 365),
    }
}

fn map_kline(values: Vec<Value>) -> Result<Candle, MarketDataError> {
    if values.len() < 6 {
        return Err(malformed_response(KLINES_ENDPOINT));
    }
    Ok(Candle {
        starts_at: value_timestamp(&values[0])?,
        open: value_number(&values[1])?,
        high: value_number(&values[2])?,
        low: value_number(&values[3])?,
        close: value_number(&values[4])?,
        volume: value_number(&values[5])?,
    })
}

fn value_timestamp(value: &Value) -> Result<DateTime<Utc>, MarketDataError> {
    value
        .as_i64()
        .and_then(DateTime::from_timestamp_millis)
        .ok_or_else(|| malformed_response(KLINES_ENDPOINT))
}

fn value_number(value: &Value) -> Result<f64, MarketDataError> {
    value
        .as_str()
        .ok_or_else(|| malformed_response(KLINES_ENDPOINT))
        .and_then(|value| finite_number(value, KLINES_ENDPOINT))
}

fn finite_number(value: &str, endpoint: &str) -> Result<f64, MarketDataError> {
    value
        .parse::<f64>()
        .ok()
        .filter(|value| value.is_finite())
        .ok_or_else(|| malformed_response(endpoint))
}

fn timestamp(value: i64, endpoint: &str) -> Result<DateTime<Utc>, MarketDataError> {
    DateTime::from_timestamp_millis(value).ok_or_else(|| malformed_response(endpoint))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chart_periods_use_binance_intervals_and_limits() {
        assert_eq!(chart_parameters(ChartPeriod::OneDay), ("5m", 288));
        assert_eq!(chart_parameters(ChartPeriod::FiveDays), ("30m", 240));
        assert_eq!(chart_parameters(ChartPeriod::OneMonth), ("2h", 360));
        assert_eq!(chart_parameters(ChartPeriod::SixMonths), ("1d", 180));
        assert_eq!(chart_parameters(ChartPeriod::OneYear), ("1d", 365));
    }
}
