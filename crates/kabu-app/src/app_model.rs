use crate::runtime::RuntimeHandle;
use kabu_core::{
    CachedChart, CachedMarketState, ChartRequestKey, Instrument, InstrumentId, PersistedState,
    PersistenceError, Quote, QuoteState, SearchResult, StateRepository, ThemePreference, Watchlist,
    WatchlistError,
};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

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
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppWarning {
    pub message: String,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct SearchState {
    query: String,
    results: Vec<SearchResult>,
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
}

pub struct AppModel {
    watchlist: Watchlist,
    selected_identity: Option<InstrumentId>,
    quotes: QuoteState,
    loading_quotes: HashSet<InstrumentId>,
    charts: HashMap<ChartRequestKey, CachedChart>,
    theme: ThemePreference,
    sidebar_width: f32,
    window_width: f32,
    window_height: f32,
    search: SearchState,
    runtime: RuntimeHandle,
    repository: Option<Arc<dyn StateRepository>>,
    warnings: Vec<AppWarning>,
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

        Self {
            watchlist: state.watchlist,
            selected_identity,
            quotes,
            loading_quotes: HashSet::new(),
            charts: cache.charts,
            theme: state.theme,
            sidebar_width: clamp_sidebar_width(state.sidebar_width),
            window_width: clamp_dimension(
                state.window_width,
                MIN_WINDOW_WIDTH,
                DEFAULT_WINDOW_WIDTH,
            ),
            window_height: clamp_dimension(
                state.window_height,
                MIN_WINDOW_HEIGHT,
                DEFAULT_WINDOW_HEIGHT,
            ),
            search: SearchState::default(),
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

    pub fn select_index(&mut self, index: usize) -> Result<(), AppModelError> {
        if self.watchlist.selected_index() == Some(index) {
            return Ok(());
        }
        self.watchlist.select(index)?;
        self.sync_selected_identity();
        self.persist_state()
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
        self.persist_if_selection_changed(before)
    }

    pub fn select_previous(&mut self) -> Result<(), AppModelError> {
        let before = self.watchlist.selected_index();
        self.watchlist.select_previous();
        self.sync_selected_identity();
        self.persist_if_selection_changed(before)
    }

    pub fn remove_selected(&mut self) -> Result<(), AppModelError> {
        let Some(selected) = self.selected_identity.clone() else {
            return Ok(());
        };
        self.watchlist.remove(&selected);
        self.sync_selected_identity();
        self.persist_state()
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
        self.persist_state()
    }

    pub fn preview_sidebar_width(&mut self, width: f32) {
        self.sidebar_width = clamp_sidebar_width(width);
    }

    pub fn persist_sidebar_width(&mut self) -> Result<(), AppModelError> {
        self.persist_state()
    }

    pub fn set_window_geometry(&mut self, width: f32, height: f32) -> Result<(), AppModelError> {
        self.preview_window_geometry(width, height);
        self.persist_state()
    }

    pub fn preview_window_geometry(&mut self, width: f32, height: f32) {
        self.window_width = clamp_dimension(width, MIN_WINDOW_WIDTH, DEFAULT_WINDOW_WIDTH);
        self.window_height = clamp_dimension(height, MIN_WINDOW_HEIGHT, DEFAULT_WINDOW_HEIGHT);
    }

    pub fn persist_window_geometry(&mut self) -> Result<(), AppModelError> {
        self.persist_state()
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
            window_width: self.window_width,
            window_height: self.window_height,
            sidebar_width: self.sidebar_width,
            theme: self.theme,
        }
    }
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
