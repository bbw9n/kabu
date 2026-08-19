use kabu_app::{
    app_model::{AppModel, AppModelError, clamp_window_size_to_display},
    runtime::RuntimeHandle,
};
use kabu_core::{
    CachedMarketState, DataSource, InstrumentId, MarketState, PersistedState, PersistenceError,
    Quote, StateRepository, ThemePreference, Watchlist,
};
use std::{
    io,
    sync::{Arc, Mutex},
};

#[test]
fn first_launch_uses_the_starter_watchlist() {
    let model = AppModel::new(
        PersistedState::starter(),
        CachedMarketState::default(),
        fake_runtime(),
    );

    assert_eq!(model.watchlist().len(), 4);
    assert_eq!(
        model.selected().expect("selection").id.canonical_symbol(),
        "AAPL"
    );
}

#[test]
fn launch_geometry_is_clamped_to_the_active_display() {
    assert_eq!(
        clamp_window_size_to_display(2_000.0, 1_200.0, 1_440.0, 900.0),
        (1_440.0, 900.0)
    );
    assert_eq!(
        clamp_window_size_to_display(200.0, 100.0, 1_440.0, 900.0),
        (760.0, 500.0)
    );
}

#[test]
fn deleting_selection_persists_and_selects_neighbor() {
    let repo = RecordingRepository::starter();
    let mut model = AppModel::with_repository(repo.clone(), fake_runtime());

    model.select_index(1).expect("select NVDA");
    model.remove_selected().expect("remove");

    assert_eq!(
        model.selected().expect("selection").id.canonical_symbol(),
        "BTC/USDT"
    );
    assert_eq!(
        repo.saved_states().last().expect("saved").watchlist.len(),
        3
    );
}

#[test]
fn theme_and_geometry_changes_are_clamped_and_persisted() {
    let repo = RecordingRepository::starter();
    let mut model = AppModel::with_repository(repo.clone(), fake_runtime());

    model.set_theme(ThemePreference::Signal).expect("theme");
    model.set_sidebar_width(999.0).expect("sidebar");
    model.set_window_geometry(900.0, 580.0).expect("geometry");

    let saved_states = repo.saved_states();
    let saved = saved_states.last().expect("saved");
    assert_eq!(saved.theme, ThemePreference::Signal);
    assert_eq!(saved.sidebar_width, 360.0);
    assert_eq!((saved.window_width, saved.window_height), (900.0, 580.0));
}

#[test]
fn restored_dimensions_are_clamped_before_the_model_is_observed() {
    let mut state = PersistedState::starter();
    state.window_width = 200.0;
    state.window_height = 100.0;
    state.sidebar_width = 9.0;

    let model = AppModel::new(state, CachedMarketState::default(), fake_runtime());

    assert_eq!(model.window_geometry(), (760.0, 500.0));
    assert_eq!(model.sidebar_width(), 220.0);
}

#[test]
fn sidebar_drag_previews_live_and_persists_only_when_committed() {
    let repo = RecordingRepository::starter();
    let mut model = AppModel::with_repository(repo.clone(), fake_runtime());

    model.preview_sidebar_width(999.0);
    assert_eq!(model.sidebar_width(), 360.0);
    assert!(repo.saved_states().is_empty());

    model.persist_sidebar_width().expect("mouse-up commit");
    assert_eq!(repo.saved_states().len(), 1);
    assert_eq!(repo.saved_states()[0].sidebar_width, 360.0);
}

#[test]
fn selection_during_geometry_debounce_serializes_committed_geometry() {
    let repo = RecordingRepository::starter();
    let mut model = AppModel::with_repository(repo.clone(), fake_runtime());

    model.preview_window_geometry(900.0, 580.0);
    model.select_index(1).expect("select during resize");

    let saved_states = repo.saved_states();
    let selection_save = saved_states.last().expect("selection save");
    assert_eq!(
        (selection_save.window_width, selection_save.window_height),
        (980.0, 640.0)
    );
    assert_eq!(model.window_geometry(), (900.0, 580.0));

    model
        .persist_window_geometry()
        .expect("debounced geometry commit");
    let saved_states = repo.saved_states();
    let geometry_save = saved_states.last().expect("geometry save");
    assert_eq!(
        (geometry_save.window_width, geometry_save.window_height),
        (900.0, 580.0)
    );
}

#[test]
fn theme_during_sidebar_drag_serializes_committed_sidebar_width() {
    let repo = RecordingRepository::starter();
    let mut model = AppModel::with_repository(repo.clone(), fake_runtime());

    model.preview_sidebar_width(350.0);
    model
        .set_theme(ThemePreference::Signal)
        .expect("theme during drag");

    let saved_states = repo.saved_states();
    let theme_save = saved_states.last().expect("theme save");
    assert_eq!(theme_save.theme, ThemePreference::Signal);
    assert_eq!(theme_save.sidebar_width, 280.0);
    assert_eq!(model.sidebar_width(), 350.0);

    model
        .persist_sidebar_width()
        .expect("mouse-up sidebar commit");
    let saved_states = repo.saved_states();
    assert_eq!(
        saved_states.last().expect("sidebar save").sidebar_width,
        350.0
    );
}

#[test]
fn repository_load_preserves_custom_order_and_non_first_selection() {
    let starter = Watchlist::starter();
    let mut custom = Watchlist::default();
    for index in [2, 0, 3, 1] {
        custom.add(starter.instruments()[index].clone());
    }
    custom.select(2).expect("select ETH");
    let mut state = PersistedState::starter();
    state.watchlist = custom;
    let repo = RecordingRepository::with_state(state);

    let model = AppModel::with_repository(repo, fake_runtime());

    assert_eq!(
        model
            .watchlist()
            .instruments()
            .iter()
            .map(|instrument| instrument.id.canonical_symbol())
            .collect::<Vec<_>>(),
        ["BTC/USDT", "AAPL", "ETH/USDT", "NVDA"]
    );
    assert_eq!(model.watchlist().selected_index(), Some(2));
    assert_eq!(
        model
            .selected_identity()
            .expect("selected identity")
            .canonical_symbol(),
        "ETH/USDT"
    );
}

#[test]
fn cached_quote_is_available_on_launch() {
    let aapl = InstrumentId::equity("AAPL", Some("NASDAQ")).expect("id");
    let quote = cached_quote(aapl.clone(), 201.25, 199.0);
    let mut cache = CachedMarketState::default();
    cache.quotes.insert(aapl.clone(), quote);

    let model = AppModel::new(PersistedState::starter(), cache, fake_runtime());

    assert_eq!(model.quote(&aapl).expect("cached quote").last, 201.25);
}

#[test]
fn persistence_failure_keeps_the_action_and_enqueues_a_warning() {
    let repo = RecordingRepository::failing();
    let mut model = AppModel::with_repository(repo, fake_runtime());

    let error = model.select_index(1).expect_err("save must fail");

    assert!(matches!(
        error,
        AppModelError::Persistence(PersistenceError::Io(_))
    ));
    assert_eq!(
        model
            .selected()
            .expect("selection remains")
            .id
            .canonical_symbol(),
        "NVDA"
    );
    assert_eq!(model.warnings().len(), 1);
    assert!(model.warnings()[0].message.contains("could not save"));
}

fn fake_runtime() -> RuntimeHandle {
    RuntimeHandle::default()
}

fn cached_quote(instrument_id: InstrumentId, last: f64, previous_close: f64) -> Quote {
    let timestamp = chrono::DateTime::from_timestamp(1_777_777_777, 0).expect("timestamp");
    Quote {
        instrument_id,
        last,
        previous_close: Some(previous_close),
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

#[derive(Clone)]
struct RecordingRepository {
    inner: Arc<Mutex<RecordingState>>,
}

struct RecordingState {
    state: PersistedState,
    cache: CachedMarketState,
    saved_states: Vec<PersistedState>,
    fail_saves: bool,
}

impl RecordingRepository {
    fn starter() -> Self {
        Self::new(false)
    }

    fn failing() -> Self {
        Self::new(true)
    }

    fn with_state(state: PersistedState) -> Self {
        Self {
            inner: Arc::new(Mutex::new(RecordingState {
                state,
                cache: CachedMarketState::default(),
                saved_states: Vec::new(),
                fail_saves: false,
            })),
        }
    }

    fn new(fail_saves: bool) -> Self {
        Self {
            inner: Arc::new(Mutex::new(RecordingState {
                state: PersistedState::starter(),
                cache: CachedMarketState::default(),
                saved_states: Vec::new(),
                fail_saves,
            })),
        }
    }

    fn saved_states(&self) -> Vec<PersistedState> {
        self.inner
            .lock()
            .expect("repository lock")
            .saved_states
            .clone()
    }
}

impl StateRepository for RecordingRepository {
    fn load_state(&self) -> Result<PersistedState, PersistenceError> {
        Ok(self.inner.lock().expect("repository lock").state.clone())
    }

    fn save_state(&self, state: &PersistedState) -> Result<(), PersistenceError> {
        let mut inner = self.inner.lock().expect("repository lock");
        if inner.fail_saves {
            return Err(io::Error::other("recording failure").into());
        }
        inner.saved_states.push(state.clone());
        inner.state = state.clone();
        Ok(())
    }

    fn load_cache(&self) -> Result<CachedMarketState, PersistenceError> {
        Ok(self.inner.lock().expect("repository lock").cache.clone())
    }

    fn save_cache(&self, cache: &CachedMarketState) -> Result<(), PersistenceError> {
        self.inner.lock().expect("repository lock").cache = cache.clone();
        Ok(())
    }
}
