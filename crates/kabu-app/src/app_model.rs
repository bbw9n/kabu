use crate::runtime::{
    ProviderWarning, RuntimeCommand, RuntimeEvent, RuntimeHandle, RuntimeSendError,
};
use kabu_core::{
    AddOutcome, AssetClass, CachedChart, CachedMarketState, ChartPeriod, ChartRequestKey,
    Generation, GenerationId, Instrument, InstrumentId, PersistedState, PersistenceError, Quote,
    QuoteState, SearchResult, StateRepository, ThemePreference, Watchlist, WatchlistError,
};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

const SEARCH_DEBOUNCE: Duration = Duration::from_millis(250);

pub const DEFAULT_WINDOW_WIDTH: f32 = 980.0;
pub const DEFAULT_WINDOW_HEIGHT: f32 = 640.0;
pub const MIN_WINDOW_WIDTH: f32 = 760.0;
pub const MIN_WINDOW_HEIGHT: f32 = 500.0;
pub const DEFAULT_SIDEBAR_WIDTH: f32 = 280.0;
pub const MIN_SIDEBAR_WIDTH: f32 = 220.0;
pub const MAX_SIDEBAR_WIDTH: f32 = 360.0;

#[derive(Debug, thiserror::Error)]
pub enum AppModelError {
    #[error(transparent)]
    Persistence(#[from] PersistenceError),
    #[error(transparent)]
    Watchlist(#[from] WatchlistError),
    #[error("search result index {index} is outside a result set of length {len}")]
    SearchResultOutOfBounds { index: usize, len: usize },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppWarning {
    pub message: String,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct SearchState {
    query: String,
    results: Vec<SearchResult>,
    provider_warnings: Vec<ProviderWarning>,
    terminal_error: Option<String>,
    loading: bool,
}

impl SearchState {
    pub fn query(&self) -> &str {
        &self.query
    }

    pub fn results(&self) -> &[SearchResult] {
        &self.results
    }

    pub const fn is_loading(&self) -> bool {
        self.loading
    }

    pub fn provider_warnings(&self) -> &[ProviderWarning] {
        &self.provider_warnings
    }

    pub fn terminal_error(&self) -> Option<&str> {
        self.terminal_error.as_deref()
    }
}

#[derive(Clone, Debug)]
struct PendingSearch {
    generation: GenerationId,
    query: String,
    remaining: Duration,
}

pub struct AppModel {
    watchlist: Watchlist,
    selected_identity: Option<InstrumentId>,
    quotes: QuoteState,
    loading_quotes: HashSet<InstrumentId>,
    charts: HashMap<ChartRequestKey, CachedChart>,
    theme: ThemePreference,
    sidebar_width: f32,
    committed_sidebar_width: f32,
    window_width: f32,
    window_height: f32,
    committed_window_width: f32,
    committed_window_height: f32,
    search: SearchState,
    search_generation: Generation,
    history_generation: Generation,
    selected_chart_period: ChartPeriod,
    current_history: Option<(GenerationId, ChartRequestKey)>,
    pending_search: Option<PendingSearch>,
    runtime: RuntimeHandle,
    repository: Option<Arc<dyn StateRepository>>,
    warnings: Vec<AppWarning>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChartAvailability {
    Ready,
    Loading,
    Unavailable,
}

impl AppModel {
    pub fn new(state: PersistedState, cache: CachedMarketState, runtime: RuntimeHandle) -> Self {
        Self::from_parts(state, cache, runtime, None, Vec::new())
    }

    pub fn with_repository(
        repository: impl StateRepository + 'static,
        runtime: RuntimeHandle,
    ) -> Self {
        Self::with_shared_repository(Arc::new(repository), runtime)
    }

    pub fn with_shared_repository(
        repository: Arc<dyn StateRepository>,
        runtime: RuntimeHandle,
    ) -> Self {
        let mut warnings = Vec::new();
        let state = repository.load_state().unwrap_or_else(|error| {
            warnings.push(load_warning("settings", &error));
            PersistedState::starter()
        });
        let cache = repository.load_cache().unwrap_or_else(|error| {
            warnings.push(load_warning("cached market data", &error));
            CachedMarketState::default()
        });
        Self::from_parts(state, cache, runtime, Some(repository), warnings)
    }

    fn from_parts(
        state: PersistedState,
        cache: CachedMarketState,
        runtime: RuntimeHandle,
        repository: Option<Arc<dyn StateRepository>>,
        warnings: Vec<AppWarning>,
    ) -> Self {
        let mut quotes = QuoteState::default();
        for quote in cache.quotes.into_values() {
            quotes.merge(quote);
        }
        let selected_identity = state.watchlist.selected().map(|item| item.id.clone());
        let sidebar_width = clamp_sidebar_width(state.sidebar_width);
        let window_width =
            clamp_dimension(state.window_width, MIN_WINDOW_WIDTH, DEFAULT_WINDOW_WIDTH);
        let window_height = clamp_dimension(
            state.window_height,
            MIN_WINDOW_HEIGHT,
            DEFAULT_WINDOW_HEIGHT,
        );

        Self {
            watchlist: state.watchlist,
            selected_identity,
            quotes,
            loading_quotes: HashSet::new(),
            charts: cache.charts,
            theme: state.theme,
            sidebar_width,
            committed_sidebar_width: sidebar_width,
            window_width,
            window_height,
            committed_window_width: window_width,
            committed_window_height: window_height,
            search: SearchState::default(),
            search_generation: Generation::default(),
            history_generation: Generation::default(),
            selected_chart_period: ChartPeriod::OneDay,
            current_history: None,
            pending_search: None,
            runtime,
            repository,
            warnings,
        }
    }

    pub fn watchlist(&self) -> &Watchlist {
        &self.watchlist
    }

    pub fn selected(&self) -> Option<&Instrument> {
        self.watchlist.selected()
    }

    pub fn selected_identity(&self) -> Option<&InstrumentId> {
        self.selected_identity.as_ref()
    }

    pub fn quote(&self, instrument_id: &InstrumentId) -> Option<&Quote> {
        self.quotes.get(instrument_id)
    }

    pub fn quote_state(&self) -> &QuoteState {
        &self.quotes
    }

    pub fn quote_is_loading(&self, instrument_id: &InstrumentId) -> bool {
        self.loading_quotes.contains(instrument_id)
    }

    pub fn set_quote_loading(&mut self, instrument_id: InstrumentId, loading: bool) {
        if loading {
            self.loading_quotes.insert(instrument_id);
        } else {
            self.loading_quotes.remove(&instrument_id);
        }
    }

    pub fn charts(&self) -> &HashMap<ChartRequestKey, CachedChart> {
        &self.charts
    }

    pub const fn selected_chart_period(&self) -> ChartPeriod {
        self.selected_chart_period
    }

    pub fn selected_chart(&self) -> Option<&CachedChart> {
        self.selected_chart_key()
            .and_then(|key| self.charts.get(&key))
    }

    pub fn selected_chart_availability(&self) -> ChartAvailability {
        let Some(key) = self.selected_chart_key() else {
            return ChartAvailability::Unavailable;
        };
        if self
            .current_history
            .as_ref()
            .is_some_and(|(_, current_key)| current_key == &key)
        {
            ChartAvailability::Loading
        } else if self.charts.contains_key(&key) {
            ChartAvailability::Ready
        } else {
            ChartAvailability::Unavailable
        }
    }

    pub fn select_period(&mut self, period: ChartPeriod) -> Option<GenerationId> {
        self.select_period_at(period, chrono::Utc::now())
    }

    pub fn select_period_at(
        &mut self,
        period: ChartPeriod,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Option<GenerationId> {
        self.selected_chart_period = period;
        let Some(instrument) = self.selected().cloned() else {
            self.current_history = None;
            return None;
        };
        let key = ChartRequestKey {
            instrument_id: instrument.id.clone(),
            period,
        };
        if self
            .charts
            .get(&key)
            .is_some_and(|chart| chart_is_fresh(chart, period, now))
        {
            self.current_history = None;
            return None;
        }
        Some(self.request_history(instrument, period))
    }

    pub const fn theme(&self) -> ThemePreference {
        self.theme
    }

    pub const fn sidebar_width(&self) -> f32 {
        self.sidebar_width
    }

    pub const fn window_geometry(&self) -> (f32, f32) {
        (self.window_width, self.window_height)
    }

    pub const fn search(&self) -> &SearchState {
        &self.search
    }

    pub const fn runtime(&self) -> &RuntimeHandle {
        &self.runtime
    }

    pub fn warnings(&self) -> &[AppWarning] {
        &self.warnings
    }

    pub fn set_search_query(&mut self, query: impl AsRef<str>) -> GenerationId {
        let generation = self.begin_search(query);
        if self.search.query.is_empty() {
            self.search.loading = false;
            self.pending_search = None;
        } else {
            self.pending_search = Some(PendingSearch {
                generation,
                query: self.search.query.clone(),
                remaining: SEARCH_DEBOUNCE,
            });
        }
        generation
    }

    pub fn begin_search(&mut self, query: impl AsRef<str>) -> GenerationId {
        let generation = self.search_generation.next();
        self.pending_search = None;
        self.search.query = query.as_ref().trim().to_string();
        self.search.results.clear();
        self.search.provider_warnings.clear();
        self.search.terminal_error = None;
        self.search.loading = !self.search.query.is_empty();
        generation
    }

    pub fn cancel_pending_search(&mut self) {
        self.begin_search("");
    }

    pub fn advance_search_clock(&mut self, elapsed: Duration) {
        let Some(pending) = self.pending_search.as_mut() else {
            return;
        };
        pending.remaining = pending.remaining.saturating_sub(elapsed);
        if pending.remaining.is_zero() {
            let generation = pending.generation;
            self.dispatch_pending_search(generation);
        }
    }

    pub fn dispatch_pending_search(&mut self, generation: GenerationId) {
        if !self.search_generation.is_current(generation) {
            return;
        }
        let Some(pending) = self.pending_search.take() else {
            return;
        };
        if pending.generation != generation {
            self.pending_search = Some(pending);
            return;
        }
        if self
            .send_runtime(RuntimeCommand::Search {
                generation,
                query: pending.query,
            })
            .is_err()
        {
            self.search.loading = false;
            self.search.terminal_error = Some("Search is temporarily unavailable.".into());
        }
    }

    pub fn apply_search_results(
        &mut self,
        generation: GenerationId,
        results: Vec<SearchResult>,
        warnings: Vec<ProviderWarning>,
    ) {
        if !self.search_generation.is_current(generation) {
            return;
        }
        self.search.results = results;
        self.search.provider_warnings = warnings;
        self.search.terminal_error = None;
        self.search.loading = false;
    }

    pub fn add_search_result(&mut self, index: usize) -> Result<(), AppModelError> {
        self.add_search_result_at(index, chrono::Utc::now())
    }

    pub fn add_search_result_at(
        &mut self,
        index: usize,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), AppModelError> {
        let result = self.search.results.get(index).cloned().ok_or(
            AppModelError::SearchResultOutOfBounds {
                index,
                len: self.search.results.len(),
            },
        )?;
        let outcome = self.watchlist.add(result.instrument.clone());
        let selected_index = match outcome {
            AddOutcome::Added { index } | AddOutcome::AlreadyPresent { index } => index,
        };
        self.watchlist.select(selected_index)?;
        self.sync_selected_identity();
        let persistence_result = self.persist_state();

        if matches!(outcome, AddOutcome::Added { .. }) {
            _ = self.send_runtime(RuntimeCommand::Snapshot {
                instruments: self.watchlist.instruments().to_vec(),
            });
            if result.instrument.id.asset_class() == AssetClass::Crypto {
                _ = self.send_runtime(RuntimeCommand::SetCryptoWatchlist(
                    self.watchlist
                        .instruments()
                        .iter()
                        .filter(|instrument| instrument.id.asset_class() == AssetClass::Crypto)
                        .cloned()
                        .collect(),
                ));
            }
        }
        self.select_one_day_for_current_at(now);
        persistence_result
    }

    pub fn request_history(&mut self, instrument: Instrument, period: ChartPeriod) -> GenerationId {
        if self.selected_identity.as_ref() == Some(&instrument.id) {
            self.selected_chart_period = period;
        }
        let generation = self.history_generation.next();
        let key = ChartRequestKey {
            instrument_id: instrument.id.clone(),
            period,
        };
        self.current_history = Some((generation, key));
        if self
            .send_runtime(RuntimeCommand::History {
                generation,
                instrument,
                period,
            })
            .is_err()
        {
            self.current_history = None;
        }
        generation
    }

    pub fn apply_runtime_event(&mut self, event: RuntimeEvent) {
        self.apply_runtime_event_at(event, chrono::Utc::now());
    }

    pub fn apply_runtime_event_at(
        &mut self,
        event: RuntimeEvent,
        now: chrono::DateTime<chrono::Utc>,
    ) {
        match event {
            RuntimeEvent::SearchCompleted {
                generation,
                results,
                warnings,
            } => self.apply_search_results(generation, results, warnings),
            RuntimeEvent::Quotes(quotes) => {
                for quote in quotes {
                    self.loading_quotes.remove(&quote.instrument_id);
                    self.quotes.merge(quote);
                }
            }
            RuntimeEvent::QuoteError { instrument_ids, .. } => {
                for instrument_id in instrument_ids {
                    self.loading_quotes.remove(&instrument_id);
                }
                self.warnings.push(AppWarning {
                    message: "Some quotes are temporarily unavailable.".into(),
                });
            }
            RuntimeEvent::HistoryCompleted {
                generation,
                key,
                candles,
            } => {
                if self.history_is_current(generation, &key) {
                    self.charts.insert(
                        key,
                        CachedChart {
                            candles,
                            fetched_at: now,
                        },
                    );
                    self.current_history = None;
                }
            }
            RuntimeEvent::HistoryFailed {
                generation, key, ..
            } => {
                if self.history_is_current(generation, &key) {
                    self.current_history = None;
                    self.warnings.push(AppWarning {
                        message: "Chart history is temporarily unavailable.".into(),
                    });
                }
            }
            RuntimeEvent::StreamStatus(_) => {}
        }
    }

    pub fn select_index(&mut self, index: usize) -> Result<(), AppModelError> {
        self.select_index_at(index, chrono::Utc::now())
    }

    pub fn select_index_at(
        &mut self,
        index: usize,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), AppModelError> {
        let persistence_result = if self.watchlist.selected_index() == Some(index) {
            Ok(())
        } else {
            self.watchlist.select(index)?;
            self.sync_selected_identity();
            self.persist_state()
        };
        self.select_one_day_for_current_at(now);
        persistence_result
    }

    pub fn select_instrument(&mut self, instrument_id: &InstrumentId) -> Result<(), AppModelError> {
        let Some(index) = self
            .watchlist
            .instruments()
            .iter()
            .position(|instrument| &instrument.id == instrument_id)
        else {
            return Err(WatchlistError::SelectionOutOfBounds {
                index: self.watchlist.len(),
                len: self.watchlist.len(),
            }
            .into());
        };
        self.select_index(index)
    }

    pub fn select_next(&mut self) -> Result<(), AppModelError> {
        let before = self.watchlist.selected_index();
        self.watchlist.select_next();
        self.sync_selected_identity();
        let persistence_result = self.persist_if_selection_changed(before);
        if before != self.watchlist.selected_index() {
            self.select_one_day_for_current_at(chrono::Utc::now());
        }
        persistence_result
    }

    pub fn select_previous(&mut self) -> Result<(), AppModelError> {
        let before = self.watchlist.selected_index();
        self.watchlist.select_previous();
        self.sync_selected_identity();
        let persistence_result = self.persist_if_selection_changed(before);
        if before != self.watchlist.selected_index() {
            self.select_one_day_for_current_at(chrono::Utc::now());
        }
        persistence_result
    }

    pub fn remove_selected(&mut self) -> Result<(), AppModelError> {
        let Some(selected) = self.selected_identity.clone() else {
            return Ok(());
        };
        self.watchlist.remove(&selected);
        self.sync_selected_identity();
        let persistence_result = self.persist_state();
        self.select_one_day_for_current_at(chrono::Utc::now());
        persistence_result
    }

    pub fn set_theme(&mut self, theme: ThemePreference) -> Result<(), AppModelError> {
        if self.theme == theme {
            return Ok(());
        }
        self.theme = theme;
        self.persist_state()
    }

    pub fn set_sidebar_width(&mut self, width: f32) -> Result<(), AppModelError> {
        self.preview_sidebar_width(width);
        self.persist_sidebar_width()
    }

    pub fn preview_sidebar_width(&mut self, width: f32) {
        self.sidebar_width = clamp_sidebar_width(width);
    }

    pub fn persist_sidebar_width(&mut self) -> Result<(), AppModelError> {
        self.committed_sidebar_width = self.sidebar_width;
        self.persist_state()
    }

    pub fn set_window_geometry(&mut self, width: f32, height: f32) -> Result<(), AppModelError> {
        self.preview_window_geometry(width, height);
        self.persist_window_geometry()
    }

    pub fn preview_window_geometry(&mut self, width: f32, height: f32) {
        self.window_width = clamp_dimension(width, MIN_WINDOW_WIDTH, DEFAULT_WINDOW_WIDTH);
        self.window_height = clamp_dimension(height, MIN_WINDOW_HEIGHT, DEFAULT_WINDOW_HEIGHT);
    }

    pub fn persist_window_geometry(&mut self) -> Result<(), AppModelError> {
        self.committed_window_width = self.window_width;
        self.committed_window_height = self.window_height;
        self.persist_state()
    }

    pub fn set_restored_window_geometry(&mut self, width: f32, height: f32) {
        self.preview_window_geometry(width, height);
        self.committed_window_width = self.window_width;
        self.committed_window_height = self.window_height;
    }

    fn persist_if_selection_changed(&mut self, before: Option<usize>) -> Result<(), AppModelError> {
        if before == self.watchlist.selected_index() {
            Ok(())
        } else {
            self.persist_state()
        }
    }

    fn sync_selected_identity(&mut self) {
        self.selected_identity = self.watchlist.selected().map(|item| item.id.clone());
    }

    fn select_one_day_for_current_at(&mut self, now: chrono::DateTime<chrono::Utc>) {
        self.select_period_at(ChartPeriod::OneDay, now);
    }

    fn persist_state(&mut self) -> Result<(), AppModelError> {
        let Some(repository) = self.repository.clone() else {
            return Ok(());
        };
        let state = self.persisted_state();
        if let Err(error) = repository.save_state(&state) {
            self.warnings.push(AppWarning {
                message: "Your change is kept in this session, but Kabu could not save it to disk."
                    .into(),
            });
            return Err(error.into());
        }
        Ok(())
    }

    fn persisted_state(&self) -> PersistedState {
        PersistedState {
            schema_version: kabu_core::STATE_SCHEMA_VERSION,
            watchlist: self.watchlist.clone(),
            window_width: self.committed_window_width,
            window_height: self.committed_window_height,
            sidebar_width: self.committed_sidebar_width,
            theme: self.theme,
        }
    }

    fn history_is_current(&self, generation: GenerationId, key: &ChartRequestKey) -> bool {
        self.current_history
            .as_ref()
            .is_some_and(|current| current.0 == generation && &current.1 == key)
    }

    fn selected_chart_key(&self) -> Option<ChartRequestKey> {
        Some(ChartRequestKey {
            instrument_id: self.selected_identity.clone()?,
            period: self.selected_chart_period,
        })
    }

    fn send_runtime(&mut self, command: RuntimeCommand) -> Result<(), RuntimeSendError> {
        let result = self.runtime.send(command);
        if result.is_err() {
            self.warnings.push(AppWarning {
                message: "Background market-data work is temporarily unavailable.".into(),
            });
        }
        result
    }
}

fn chart_is_fresh(
    chart: &CachedChart,
    period: ChartPeriod,
    now: chrono::DateTime<chrono::Utc>,
) -> bool {
    now >= chart.fetched_at
        && now
            .signed_duration_since(chart.fetched_at)
            .to_std()
            .is_ok_and(|age| age <= period.cache_ttl())
}

fn clamp_sidebar_width(width: f32) -> f32 {
    if width.is_finite() {
        width.clamp(MIN_SIDEBAR_WIDTH, MAX_SIDEBAR_WIDTH)
    } else {
        DEFAULT_SIDEBAR_WIDTH
    }
}

fn clamp_dimension(value: f32, minimum: f32, fallback: f32) -> f32 {
    if value.is_finite() {
        value.max(minimum)
    } else {
        fallback
    }
}

pub fn clamp_window_size_to_display(
    width: f32,
    height: f32,
    display_width: f32,
    display_height: f32,
) -> (f32, f32) {
    let width = clamp_dimension(width, MIN_WINDOW_WIDTH, DEFAULT_WINDOW_WIDTH);
    let height = clamp_dimension(height, MIN_WINDOW_HEIGHT, DEFAULT_WINDOW_HEIGHT);
    (
        clamp_to_display(width, display_width, MIN_WINDOW_WIDTH),
        clamp_to_display(height, display_height, MIN_WINDOW_HEIGHT),
    )
}

fn clamp_to_display(value: f32, display_extent: f32, minimum: f32) -> f32 {
    if display_extent.is_finite() && display_extent > 0.0 {
        value.min(display_extent).max(minimum.min(display_extent))
    } else {
        value
    }
}

fn load_warning(category: &str, error: &PersistenceError) -> AppWarning {
    AppWarning {
        message: format!("Kabu could not load {category}; safe defaults are in use ({error})."),
    }
}
