mod catalog;
mod rest;
mod stream;
mod wire;

use std::sync::{Arc, RwLock};

use kabu_core::{CachedInstrumentCatalog, MarketDataError};
use reqwest::Url;

use crate::http::client;

const REST_BASE: &str = "https://data-api.binance.vision/";
const STREAM_BASE: &str = "wss://data-stream.binance.vision";

#[derive(Clone, Debug)]
pub struct BinanceClient {
    pub(super) http: reqwest::Client,
    pub(super) rest_base: Url,
    pub(super) stream_base: Url,
    pub(super) catalog: Arc<RwLock<Option<CachedInstrumentCatalog>>>,
}

impl BinanceClient {
    pub fn new(
        http: reqwest::Client,
        rest_base: Url,
        stream_base: Url,
        initial_catalog: Option<CachedInstrumentCatalog>,
    ) -> Self {
        Self {
            http,
            rest_base,
            stream_base,
            catalog: Arc::new(RwLock::new(initial_catalog)),
        }
    }

    pub fn production(
        initial_catalog: Option<CachedInstrumentCatalog>,
    ) -> Result<Self, reqwest::Error> {
        Ok(Self::new(
            client()?,
            REST_BASE.parse().expect("Binance REST base URL is valid"),
            STREAM_BASE
                .parse()
                .expect("Binance stream base URL is valid"),
            initial_catalog,
        ))
    }

    pub async fn refresh_catalog(&self) -> Result<(), MarketDataError> {
        catalog::refresh(self).await
    }
}
