use chrono::{Duration, Utc};
use kabu_core::{
    CachedInstrumentCatalog, DataSource, Instrument, InstrumentId, InstrumentSearch,
    MarketDataError, MarketDataErrorKind, SearchResult,
};
use reqwest::Url;

use crate::http::{classify_request_error, decode_json};

use super::{BinanceClient, wire::ExchangeInfoResponse};

const EXCHANGE_INFO_ENDPOINT: &str = "Binance catalog";
const CATALOG_TTL: Duration = Duration::hours(24);

pub(super) async fn refresh(client: &BinanceClient) -> Result<(), MarketDataError> {
    if client
        .current_catalog()?
        .as_ref()
        .is_some_and(is_fresh_catalog)
    {
        return Ok(());
    }

    let url = endpoint(client, "api/v3/exchangeInfo")?;
    let response = client
        .http
        .get(url)
        .send()
        .await
        .map_err(|error| classify_request_error(error, EXCHANGE_INFO_ENDPOINT))?;
    let exchange_info: ExchangeInfoResponse = decode_json(response, EXCHANGE_INFO_ENDPOINT).await?;
    let catalog = map_catalog(exchange_info)?;
    client.replace_catalog(catalog)
}

#[async_trait::async_trait]
impl InstrumentSearch for BinanceClient {
    async fn search(&self, query: &str) -> Result<Vec<SearchResult>, MarketDataError> {
        let query = query.trim().to_ascii_uppercase();
        if query.is_empty() {
            return Ok(Vec::new());
        }

        let catalog = self.current_catalog()?.unwrap_or(CachedInstrumentCatalog {
            source: DataSource::Binance,
            fetched_at: Utc::now(),
            instruments: Vec::new(),
        });
        if catalog.source != DataSource::Binance {
            return Ok(Vec::new());
        }

        let mut results: Vec<_> = catalog
            .instruments
            .into_iter()
            .filter_map(|instrument| {
                search_rank(&instrument, &query).map(|rank| (rank, instrument))
            })
            .collect();
        results.sort_by(|(left_rank, left), (right_rank, right)| {
            left_rank
                .cmp(right_rank)
                .then_with(|| quote_rank(left).cmp(&quote_rank(right)))
                .then_with(|| left.id.canonical_symbol().cmp(&right.id.canonical_symbol()))
        });
        Ok(results
            .into_iter()
            .map(|(_, instrument)| SearchResult {
                instrument,
                source: DataSource::Binance,
                venue_label: "Binance Spot".into(),
            })
            .collect())
    }
}

impl BinanceClient {
    pub(super) fn current_catalog(
        &self,
    ) -> Result<Option<CachedInstrumentCatalog>, MarketDataError> {
        self.catalog
            .read()
            .map(|catalog| catalog.clone())
            .map_err(|_| internal_error())
    }

    pub(super) fn replace_catalog(
        &self,
        catalog: CachedInstrumentCatalog,
    ) -> Result<(), MarketDataError> {
        self.catalog
            .write()
            .map(|mut current| *current = Some(catalog))
            .map_err(|_| internal_error())
    }
}

pub(super) fn endpoint(client: &BinanceClient, path: &str) -> Result<Url, MarketDataError> {
    client.rest_base.join(path).map_err(|_| internal_error())
}

pub(super) fn wire_symbol(instrument: &Instrument) -> Result<String, MarketDataError> {
    match &instrument.id {
        InstrumentId::Crypto { base, quote } => Ok(format!("{base}{quote}")),
        InstrumentId::Equity { .. } => Err(MarketDataError {
            kind: MarketDataErrorKind::UnsupportedInstrument,
            message: "Binance supports crypto instruments only".into(),
            retry_after: None,
        }),
    }
}

fn map_catalog(response: ExchangeInfoResponse) -> Result<CachedInstrumentCatalog, MarketDataError> {
    let instruments = response
        .symbols
        .into_iter()
        .filter(|symbol| symbol.status == "TRADING" && symbol.is_spot_trading_allowed)
        .filter_map(|symbol| {
            let base_name = symbol
                .base_asset_name
                .as_deref()
                .filter(|name| !name.trim().is_empty())
                .unwrap_or(&symbol.base_asset);
            let instrument =
                Instrument::crypto(&symbol.base_asset, &symbol.quote_asset, base_name).ok()?;
            (symbol.symbol == wire_symbol_unchecked(&instrument)).then_some(instrument)
        })
        .collect();
    Ok(CachedInstrumentCatalog {
        source: DataSource::Binance,
        fetched_at: Utc::now(),
        instruments,
    })
}

fn is_fresh_catalog(catalog: &CachedInstrumentCatalog) -> bool {
    let age = Utc::now() - catalog.fetched_at;
    catalog.source == DataSource::Binance && age >= Duration::zero() && age < CATALOG_TTL
}

fn search_rank(instrument: &Instrument, query: &str) -> Option<u8> {
    let canonical = instrument.id.canonical_symbol();
    let wire = wire_symbol_unchecked(instrument);
    let base = match &instrument.id {
        InstrumentId::Crypto { base, .. } => base,
        InstrumentId::Equity { .. } => return None,
    };
    if canonical == query {
        Some(0)
    } else if wire == query {
        Some(1)
    } else if base.starts_with(query) {
        Some(2)
    } else if instrument.display_name.to_ascii_uppercase().contains(query) {
        Some(3)
    } else {
        None
    }
}

fn quote_rank(instrument: &Instrument) -> u8 {
    match &instrument.id {
        InstrumentId::Crypto { quote, .. } => match quote.as_str() {
            "USDT" => 0,
            "USDC" => 1,
            "BTC" => 2,
            "ETH" => 3,
            _ => 4,
        },
        InstrumentId::Equity { .. } => 5,
    }
}

fn wire_symbol_unchecked(instrument: &Instrument) -> String {
    match &instrument.id {
        InstrumentId::Crypto { base, quote } => format!("{base}{quote}"),
        InstrumentId::Equity { .. } => String::new(),
    }
}

fn internal_error() -> MarketDataError {
    MarketDataError {
        kind: MarketDataErrorKind::Internal,
        message: "Binance catalog state is unavailable".into(),
        retry_after: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_catalog_expires_at_twenty_four_hours() {
        let catalog = CachedInstrumentCatalog {
            source: DataSource::Binance,
            fetched_at: Utc::now() - Duration::hours(24),
            instruments: Vec::new(),
        };

        assert!(!is_fresh_catalog(&catalog));
    }

    #[test]
    fn future_catalog_timestamp_is_not_treated_as_fresh() {
        let catalog = CachedInstrumentCatalog {
            source: DataSource::Binance,
            fetched_at: Utc::now() + Duration::hours(1),
            instruments: Vec::new(),
        };

        assert!(!is_fresh_catalog(&catalog));
    }
}
