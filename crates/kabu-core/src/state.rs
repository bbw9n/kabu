use crate::{Candle, ChartPeriod, DataSource, Instrument, InstrumentId, Quote, Watchlist};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub const STATE_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum ThemePreference {
    #[default]
    System,
    Graphite,
    Midnight,
    Signal,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PersistedState {
    pub schema_version: u32,
    pub watchlist: Watchlist,
    pub window_width: f32,
    pub window_height: f32,
    pub sidebar_width: f32,
    pub theme: ThemePreference,
}

impl PersistedState {
    pub fn starter() -> Self {
        Self {
            schema_version: STATE_SCHEMA_VERSION,
            watchlist: Watchlist::starter(),
            window_width: 980.0,
            window_height: 640.0,
            sidebar_width: 280.0,
            theme: ThemePreference::System,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CachedMarketState {
    pub schema_version: u32,
    #[serde(with = "map_as_entries")]
    pub quotes: HashMap<InstrumentId, Quote>,
    #[serde(with = "map_as_entries")]
    pub charts: HashMap<ChartRequestKey, CachedChart>,
    pub instrument_catalog: Option<CachedInstrumentCatalog>,
}

mod map_as_entries {
    use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};
    use std::{collections::HashMap, hash::Hash};

    pub fn serialize<K, V, S>(map: &HashMap<K, V>, serializer: S) -> Result<S::Ok, S::Error>
    where
        K: Serialize,
        V: Serialize,
        S: Serializer,
    {
        map.iter().collect::<Vec<_>>().serialize(serializer)
    }

    pub fn deserialize<'de, K, V, D>(deserializer: D) -> Result<HashMap<K, V>, D::Error>
    where
        K: Deserialize<'de> + Eq + Hash,
        V: Deserialize<'de>,
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Representation<K, V> {
            Entries(Vec<(K, V)>),
            LegacyEmpty(HashMap<String, serde::de::IgnoredAny>),
        }

        match Representation::<K, V>::deserialize(deserializer)? {
            Representation::Entries(entries) => Ok(entries.into_iter().collect()),
            Representation::LegacyEmpty(map) if map.is_empty() => Ok(HashMap::new()),
            Representation::LegacyEmpty(_) => Err(D::Error::custom(
                "legacy cache maps must be empty because structured keys were unsupported",
            )),
        }
    }
}

impl Default for CachedMarketState {
    fn default() -> Self {
        Self {
            schema_version: STATE_SCHEMA_VERSION,
            quotes: HashMap::new(),
            charts: HashMap::new(),
            instrument_catalog: None,
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct ChartRequestKey {
    pub instrument_id: InstrumentId,
    pub period: ChartPeriod,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CachedChart {
    pub candles: Vec<Candle>,
    pub fetched_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CachedInstrumentCatalog {
    pub source: DataSource,
    pub fetched_at: DateTime<Utc>,
    pub instruments: Vec<Instrument>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct GenerationId(u64);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Generation {
    current: GenerationId,
}

impl Generation {
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> GenerationId {
        self.current.0 = self
            .current
            .0
            .checked_add(1)
            .unwrap_or_else(|| panic!("generation IDs exhausted"));
        self.current
    }

    pub fn is_current(&self, generation: GenerationId) -> bool {
        self.current == generation
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct QuoteState {
    quotes: HashMap<InstrumentId, Quote>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MergeOutcome {
    Applied,
    IgnoredOlder,
}

impl QuoteState {
    pub fn merge(&mut self, quote: Quote) -> MergeOutcome {
        if self
            .quotes
            .get(&quote.instrument_id)
            .is_some_and(|existing| existing.provider_timestamp > quote.provider_timestamp)
        {
            return MergeOutcome::IgnoredOlder;
        }

        self.quotes.insert(quote.instrument_id.clone(), quote);
        MergeOutcome::Applied
    }

    pub fn get(&self, instrument_id: &InstrumentId) -> Option<&Quote> {
        self.quotes.get(instrument_id)
    }

    pub fn quotes(&self) -> &HashMap<InstrumentId, Quote> {
        &self.quotes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DataSource, InstrumentId, MarketState, Quote};
    use chrono::{TimeZone, Utc};

    #[test]
    fn an_older_quote_cannot_replace_a_newer_quote() {
        let mut state = QuoteState::default();
        assert_eq!(
            state.merge(equity_quote_at(ts(2_000))),
            MergeOutcome::Applied
        );
        assert_eq!(
            state.merge(equity_quote_at(ts(1_000))),
            MergeOutcome::IgnoredOlder
        );
    }

    #[test]
    fn superseded_chart_result_is_rejected() {
        let mut generation = Generation::default();
        let old = generation.next();
        let current = generation.next();
        assert!(!generation.is_current(old));
        assert!(generation.is_current(current));
    }

    #[test]
    #[should_panic(expected = "generation IDs exhausted")]
    fn generation_exhaustion_does_not_wrap_and_revalidate_old_tokens() {
        let mut generation = Generation {
            current: GenerationId(u64::MAX),
        };
        generation.next();
    }

    #[test]
    fn persisted_state_starts_with_the_default_schema_and_watchlist() {
        let state = PersistedState::starter();
        assert_eq!(state.schema_version, 1);
        assert_eq!(state.watchlist.instruments().len(), 4);
    }

    #[test]
    fn cached_market_state_defaults_to_an_empty_current_schema() {
        let state = CachedMarketState::default();
        assert_eq!(state.schema_version, STATE_SCHEMA_VERSION);
        assert!(state.quotes.is_empty());
        assert!(state.charts.is_empty());
        assert_eq!(state.instrument_catalog, None);
    }

    #[test]
    fn cached_market_state_round_trips_structured_quote_and_chart_keys() {
        let quote = equity_quote_at(ts(2_000));
        let key = ChartRequestKey {
            instrument_id: quote.instrument_id.clone(),
            period: ChartPeriod::OneDay,
        };
        let mut state = CachedMarketState::default();
        state.quotes.insert(quote.instrument_id.clone(), quote);
        state.charts.insert(
            key,
            CachedChart {
                candles: Vec::new(),
                fetched_at: ts(2_000),
            },
        );

        let restored: CachedMarketState =
            serde_json::from_str(&serde_json::to_string(&state).expect("non-empty cache JSON"))
                .expect("round-tripped cache");

        assert_eq!(restored, state);
    }

    #[test]
    fn cached_market_state_accepts_legacy_empty_json_maps() {
        let restored: CachedMarketState = serde_json::from_str(
            r#"{"schema_version":1,"quotes":{},"charts":{},"instrument_catalog":null}"#,
        )
        .expect("legacy empty cache");

        assert!(restored.quotes.is_empty());
        assert!(restored.charts.is_empty());
    }

    fn equity_quote_at(timestamp: chrono::DateTime<Utc>) -> Quote {
        Quote {
            instrument_id: InstrumentId::equity("AAPL", Some("NASDAQ")).expect("instrument"),
            last: 1.0,
            previous_close: None,
            open: None,
            high: None,
            low: None,
            volume: None,
            currency: "USD".into(),
            source: DataSource::Yahoo,
            market_state: MarketState::Regular,
            provider_timestamp: timestamp,
            received_at: timestamp,
            provider_delayed: false,
        }
    }

    fn ts(seconds: i64) -> chrono::DateTime<Utc> {
        Utc.timestamp_opt(seconds, 0).single().expect("timestamp")
    }
}
