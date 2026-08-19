use crate::{
    CachedMarketState, Candle, ChartPeriod, Instrument, PersistedState, Quote, SearchResult,
};
use std::{pin::Pin, time::Duration};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarketDataErrorKind {
    Timeout,
    RateLimited,
    Unavailable,
    Disconnected,
    MalformedResponse,
    UnsupportedInstrument,
    Internal,
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("{kind:?}: {message}")]
pub struct MarketDataError {
    pub kind: MarketDataErrorKind,
    pub message: String,
    pub retry_after: Option<Duration>,
}

#[derive(Debug, thiserror::Error)]
pub enum PersistenceError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid persisted JSON: {0}")]
    InvalidJson(#[from] serde_json::Error),
    #[error("unsupported schema version {0}")]
    UnsupportedSchema(u32),
}

pub type QuoteStream =
    Pin<Box<dyn futures_util::Stream<Item = Result<Quote, MarketDataError>> + Send>>;

#[async_trait::async_trait]
pub trait InstrumentSearch: Send + Sync {
    async fn search(&self, query: &str) -> Result<Vec<SearchResult>, MarketDataError>;
}

#[async_trait::async_trait]
pub trait QuoteSnapshots: Send + Sync {
    async fn snapshots(&self, instruments: &[Instrument]) -> Result<Vec<Quote>, MarketDataError>;
}

#[async_trait::async_trait]
pub trait ChartHistory: Send + Sync {
    async fn history(
        &self,
        instrument: &Instrument,
        period: ChartPeriod,
    ) -> Result<Vec<Candle>, MarketDataError>;
}

#[async_trait::async_trait]
pub trait QuoteSubscription: Send + Sync {
    async fn subscribe(&self, instruments: &[Instrument]) -> Result<QuoteStream, MarketDataError>;
}

pub trait StateRepository: Send + Sync {
    fn load_state(&self) -> Result<PersistedState, PersistenceError>;
    fn save_state(&self, state: &PersistedState) -> Result<(), PersistenceError>;
    fn load_cache(&self) -> Result<CachedMarketState, PersistenceError>;
    fn save_cache(&self, cache: &CachedMarketState) -> Result<(), PersistenceError>;
}
