use kabu_app::{
    app_model::{AppModel, AppModelError, ChartAvailability, clamp_window_size_to_display},
    runtime::{ProviderWarning, RuntimeCommand, RuntimeEvent, RuntimeHandle},
};
use kabu_core::{
    CachedMarketState, Candle, ChartPeriod, ChartRequestKey, DataSource, Instrument, InstrumentId,
    MarketDataError, MarketDataErrorKind, MarketState, PersistedState, PersistenceError, Quote,
    SearchResult, StateRepository, ThemePreference, Watchlist,
};
use std::{
    io,
    sync::{Arc, Mutex},
    time::Duration,
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

#[test]
fn search_waits_exactly_250_milliseconds() {
    let runtime = RuntimeHandle::default();
    let mut model = AppModel::new(
        PersistedState::starter(),
        CachedMarketState::default(),
        runtime.clone(),
    );

    model.set_search_query("nv");
    model.advance_search_clock(Duration::from_millis(249));
    assert!(runtime.recorded_commands().is_empty());

    model.advance_search_clock(Duration::from_millis(1));
    assert!(matches!(
        runtime.recorded_commands().last(),
        Some(RuntimeCommand::Search { query, .. }) if query == "nv"
    ));
}

#[test]
fn empty_search_cancels_a_pending_provider_call_and_clears_results() {
    let runtime = RuntimeHandle::default();
    let mut model = AppModel::new(
        PersistedState::starter(),
        CachedMarketState::default(),
        runtime.clone(),
    );
    let generation = model.begin_search("nv");
    model.apply_search_results(generation, vec![nvda_result()], Vec::new());

    model.set_search_query("new query");
    model.set_search_query("   ");
    model.advance_search_clock(Duration::from_secs(1));

    assert!(runtime.recorded_commands().is_empty());
    assert!(model.search().results().is_empty());
    assert!(!model.search().is_loading());
}

#[test]
fn old_search_generation_cannot_replace_new_results() {
    let mut model = AppModel::new(
        PersistedState::starter(),
        CachedMarketState::default(),
        fake_runtime(),
    );

    let old = model.begin_search("nv");
    let current = model.begin_search("btc");
    model.apply_search_results(old, vec![nvda_result()], Vec::new());
    assert!(model.search().results().is_empty());

    model.apply_search_results(current, vec![btc_result()], Vec::new());
    assert_eq!(
        model.search().results()[0].instrument.id.canonical_symbol(),
        "BTC/USDT"
    );
}

#[test]
fn current_search_completion_replaces_results_and_provider_warning() {
    let mut model = AppModel::new(
        PersistedState::starter(),
        CachedMarketState::default(),
        fake_runtime(),
    );
    let generation = model.begin_search("nv");
    let warning = ProviderWarning {
        source: DataSource::Binance,
        kind: MarketDataErrorKind::Unavailable,
        message: "Binance search is temporarily unavailable.".into(),
    };

    model.apply_search_results(generation, vec![nvda_result()], vec![warning.clone()]);

    assert_eq!(model.search().results(), &[nvda_result()]);
    assert_eq!(model.search().provider_warnings(), &[warning]);
    assert!(!model.search().is_loading());
}

#[test]
fn adding_search_result_appends_selects_persists_and_requests_market_data() {
    let repository = RecordingRepository::starter();
    let runtime = RuntimeHandle::default();
    let mut model = AppModel::with_repository(repository.clone(), runtime.clone());
    let generation = model.begin_search("msft");
    let result = msft_result();
    model.apply_search_results(generation, vec![result.clone()], Vec::new());

    model.add_search_result(0).expect("add MSFT");

    assert_eq!(model.watchlist().len(), 5);
    assert_eq!(
        model.selected().expect("selected MSFT").id,
        result.instrument.id
    );
    assert_eq!(
        repository
            .saved_states()
            .last()
            .expect("persisted add")
            .watchlist
            .selected_index(),
        Some(4)
    );
    let commands = runtime.recorded_commands();
    assert!(matches!(
        &commands[0],
        RuntimeCommand::Snapshot { instruments } if instruments.len() == 5
    ));
    assert!(matches!(
        &commands[1],
        RuntimeCommand::History {
            instrument,
            period: ChartPeriod::OneDay,
            ..
        } if instrument.id == result.instrument.id
    ));
}

#[test]
fn adding_duplicate_selects_existing_without_reordering_or_refreshing_watchlists() {
    let mut state = PersistedState::starter();
    state.watchlist.select(2).expect("select BTC first");
    let repository = RecordingRepository::with_state(state);
    let runtime = RuntimeHandle::default();
    let mut model = AppModel::with_repository(repository.clone(), runtime.clone());
    let generation = model.begin_search("nvda");
    model.apply_search_results(generation, vec![nvda_result()], Vec::new());

    model.add_search_result(0).expect("select existing NVDA");

    assert_eq!(model.watchlist().len(), 4);
    assert_eq!(
        model
            .watchlist()
            .instruments()
            .iter()
            .map(|instrument| instrument.id.canonical_symbol())
            .collect::<Vec<_>>(),
        ["AAPL", "NVDA", "BTC/USDT", "ETH/USDT"]
    );
    assert_eq!(model.watchlist().selected_index(), Some(1));
    assert_eq!(
        repository
            .saved_states()
            .last()
            .expect("persisted duplicate selection")
            .watchlist
            .selected_index(),
        Some(1)
    );
    let commands = runtime.recorded_commands();
    assert_eq!(commands.len(), 1);
    assert!(matches!(
        &commands[0],
        RuntimeCommand::History { instrument, .. }
            if instrument.id.canonical_symbol() == "NVDA"
    ));
}

#[test]
fn add_persistence_failure_keeps_selection_and_still_requests_live_data() {
    let repository = RecordingRepository::failing();
    let runtime = RuntimeHandle::default();
    let mut model = AppModel::with_repository(repository, runtime.clone());
    let generation = model.begin_search("msft");
    model.apply_search_results(generation, vec![msft_result()], Vec::new());

    let error = model.add_search_result(0).expect_err("save failure");

    assert!(matches!(error, AppModelError::Persistence(_)));
    assert_eq!(
        model
            .selected()
            .expect("session selection retained")
            .id
            .canonical_symbol(),
        "MSFT"
    );
    assert!(matches!(
        runtime.recorded_commands().as_slice(),
        [
            RuntimeCommand::Snapshot { .. },
            RuntimeCommand::History { .. }
        ]
    ));
}

#[test]
fn full_runtime_queue_finishes_search_with_a_sanitized_terminal_error() {
    let runtime = RuntimeHandle::default();
    for _ in 0..64 {
        runtime
            .send(RuntimeCommand::SetVisible(true))
            .expect("fill recording queue");
    }
    let mut model = AppModel::new(
        PersistedState::starter(),
        CachedMarketState::default(),
        runtime,
    );

    model.set_search_query("private-full-query");
    model.advance_search_clock(Duration::from_millis(250));

    assert!(!model.search().is_loading());
    assert_eq!(
        model.search().terminal_error(),
        Some("Search is temporarily unavailable.")
    );
    assert!(
        !model
            .warnings()
            .last()
            .expect("runtime warning")
            .message
            .contains("private-full-query")
    );
}

#[test]
fn closed_runtime_finishes_search_with_a_sanitized_terminal_error() {
    let runtime = RuntimeHandle::default();
    runtime.shutdown();
    let mut model = AppModel::new(
        PersistedState::starter(),
        CachedMarketState::default(),
        runtime,
    );

    model.set_search_query("private-closed-query");
    model.advance_search_clock(Duration::from_millis(250));

    assert!(!model.search().is_loading());
    assert_eq!(
        model.search().terminal_error(),
        Some("Search is temporarily unavailable.")
    );
    assert!(
        !model
            .warnings()
            .last()
            .expect("runtime warning")
            .message
            .contains("private-closed-query")
    );
}

#[test]
fn current_history_completion_populates_the_matching_chart_cache() {
    let mut model = AppModel::new(
        PersistedState::starter(),
        CachedMarketState::default(),
        fake_runtime(),
    );
    let instrument = model.selected().expect("selected instrument").clone();
    let generation = model.request_history(instrument.clone(), ChartPeriod::OneMonth);
    let key = ChartRequestKey {
        instrument_id: instrument.id,
        period: ChartPeriod::OneMonth,
    };
    let candles = vec![history_candle(101.0)];

    model.apply_runtime_event(RuntimeEvent::HistoryCompleted {
        generation,
        key: key.clone(),
        candles: candles.clone(),
    });

    assert_eq!(
        model.charts().get(&key).expect("cached chart").candles,
        candles
    );
}

#[test]
fn chart_defaults_to_one_day() {
    let model = AppModel::new(
        PersistedState::starter(),
        CachedMarketState::default(),
        fake_runtime(),
    );

    assert_eq!(model.selected_chart_period(), ChartPeriod::OneDay);
}

#[test]
fn fresh_period_cache_avoids_duplicate_request_at_the_ttl_boundary() {
    for age_seconds in [299, 300] {
        let (mut model, runtime, now) =
            model_with_cached_chart(ChartPeriod::OneMonth, Duration::from_secs(age_seconds));

        model.select_period_at(ChartPeriod::OneMonth, now);

        assert!(runtime.recorded_commands().is_empty());
        assert_eq!(model.selected_chart_period(), ChartPeriod::OneMonth);
        assert_eq!(
            model.selected_chart_availability(),
            ChartAvailability::Ready
        );
    }
}

#[test]
fn expired_period_cache_requests_history_and_marks_the_chart_loading() {
    let (mut model, runtime, now) =
        model_with_cached_chart(ChartPeriod::OneMonth, Duration::from_secs(301));

    model.select_period_at(ChartPeriod::OneMonth, now);

    assert!(matches!(
        runtime.recorded_commands().last(),
        Some(RuntimeCommand::History {
            period: ChartPeriod::OneMonth,
            ..
        })
    ));
    assert_eq!(
        model.selected_chart_availability(),
        ChartAvailability::Loading
    );
}

#[test]
fn every_period_action_targets_the_selected_instrument_and_period() {
    let runtime = RuntimeHandle::default();
    let mut model = AppModel::new(
        PersistedState::starter(),
        CachedMarketState::default(),
        runtime.clone(),
    );
    let selected = model.selected().expect("selected instrument").id.clone();

    for period in [
        ChartPeriod::OneDay,
        ChartPeriod::FiveDays,
        ChartPeriod::OneMonth,
        ChartPeriod::SixMonths,
        ChartPeriod::OneYear,
    ] {
        model.select_period_at(period, ts(1_800_000_000));
        assert!(matches!(
            runtime.recorded_commands().last(),
            Some(RuntimeCommand::History {
                instrument,
                period: sent_period,
                ..
            }) if instrument.id == selected && *sent_period == period
        ));
        assert_eq!(model.selected_chart_period(), period);
    }
}

#[test]
fn selecting_an_instrument_resets_to_one_day_and_requests_missing_history() {
    let runtime = RuntimeHandle::default();
    let mut model = AppModel::new(
        PersistedState::starter(),
        CachedMarketState::default(),
        runtime.clone(),
    );
    model.select_period_at(ChartPeriod::SixMonths, ts(1_800_000_000));
    let command_count = runtime.recorded_commands().len();

    model
        .select_index_at(1, ts(1_800_000_001))
        .expect("select NVDA");

    assert_eq!(model.selected_chart_period(), ChartPeriod::OneDay);
    assert!(matches!(
        runtime.recorded_commands().get(command_count),
        Some(RuntimeCommand::History {
            instrument,
            period: ChartPeriod::OneDay,
            ..
        }) if instrument.id.canonical_symbol() == "NVDA"
    ));
}

#[test]
fn selecting_an_instrument_uses_its_fresh_one_day_cache_without_a_request() {
    let mut state = PersistedState::starter();
    state.watchlist.select(0).expect("select AAPL");
    let nvda = state.watchlist.instruments()[1].clone();
    let now = ts(1_800_000_000);
    let mut cache = CachedMarketState::default();
    cache.charts.insert(
        ChartRequestKey {
            instrument_id: nvda.id,
            period: ChartPeriod::OneDay,
        },
        kabu_core::CachedChart {
            candles: vec![history_candle(201.0)],
            fetched_at: now - chrono::Duration::seconds(59),
        },
    );
    let runtime = RuntimeHandle::default();
    let mut model = AppModel::new(state, cache, runtime.clone());

    model.select_index_at(1, now).expect("select cached NVDA");

    assert!(runtime.recorded_commands().is_empty());
    assert_eq!(model.selected_chart_period(), ChartPeriod::OneDay);
    assert_eq!(
        model.selected_chart_availability(),
        ChartAvailability::Ready
    );
}

#[test]
fn history_completion_requires_the_complete_current_key() {
    let mut model = AppModel::new(
        PersistedState::starter(),
        CachedMarketState::default(),
        fake_runtime(),
    );
    let instrument = model.selected().expect("selected instrument").clone();
    let generation = model.request_history(instrument.clone(), ChartPeriod::SixMonths);
    let wrong_key = ChartRequestKey {
        instrument_id: instrument.id.clone(),
        period: ChartPeriod::OneMonth,
    };
    let current_key = ChartRequestKey {
        instrument_id: instrument.id,
        period: ChartPeriod::SixMonths,
    };

    model.apply_runtime_event(RuntimeEvent::HistoryCompleted {
        generation,
        key: wrong_key.clone(),
        candles: vec![history_candle(90.0)],
    });
    assert!(!model.charts().contains_key(&wrong_key));

    model.apply_runtime_event(RuntimeEvent::HistoryCompleted {
        generation,
        key: current_key.clone(),
        candles: vec![history_candle(110.0)],
    });
    assert_eq!(
        model
            .charts()
            .get(&current_key)
            .expect("matching chart")
            .candles,
        vec![history_candle(110.0)]
    );
}

#[test]
fn history_failure_requires_the_complete_current_key() {
    let mut model = AppModel::new(
        PersistedState::starter(),
        CachedMarketState::default(),
        fake_runtime(),
    );
    let instrument = model.selected().expect("selected instrument").clone();
    let generation = model.request_history(instrument.clone(), ChartPeriod::SixMonths);
    let wrong_key = ChartRequestKey {
        instrument_id: instrument.id,
        period: ChartPeriod::OneMonth,
    };

    model.apply_runtime_event(RuntimeEvent::HistoryFailed {
        generation,
        key: wrong_key,
        error: market_error(MarketDataErrorKind::Timeout),
    });

    assert_eq!(
        model.selected_chart_availability(),
        ChartAvailability::Loading
    );
    assert!(model.warnings().is_empty());
}

#[test]
fn superseded_history_completion_and_failure_are_ignored() {
    let mut model = AppModel::new(
        PersistedState::starter(),
        CachedMarketState::default(),
        fake_runtime(),
    );
    let instrument = model.selected().expect("selected instrument").clone();
    let old = model.request_history(instrument.clone(), ChartPeriod::OneDay);
    let old_key = ChartRequestKey {
        instrument_id: instrument.id.clone(),
        period: ChartPeriod::OneDay,
    };
    let _current = model.request_history(instrument, ChartPeriod::FiveDays);

    model.apply_runtime_event(RuntimeEvent::HistoryCompleted {
        generation: old,
        key: old_key.clone(),
        candles: vec![history_candle(99.0)],
    });
    model.apply_runtime_event(RuntimeEvent::HistoryFailed {
        generation: old,
        key: old_key.clone(),
        error: market_error(MarketDataErrorKind::Timeout),
    });

    assert!(!model.charts().contains_key(&old_key));
    assert!(model.warnings().is_empty());
}

#[test]
fn current_history_failure_finishes_without_mutating_cached_chart() {
    let mut cache = CachedMarketState::default();
    let instrument = PersistedState::starter()
        .watchlist
        .selected()
        .expect("starter selection")
        .clone();
    let key = ChartRequestKey {
        instrument_id: instrument.id.clone(),
        period: ChartPeriod::OneYear,
    };
    let original = kabu_core::CachedChart {
        candles: vec![history_candle(88.0)],
        fetched_at: chrono::DateTime::from_timestamp(1_700_000_000, 0).expect("timestamp"),
    };
    cache.charts.insert(key.clone(), original.clone());
    let mut model = AppModel::new(PersistedState::starter(), cache, fake_runtime());
    let generation = model.request_history(instrument, ChartPeriod::OneYear);

    model.apply_runtime_event(RuntimeEvent::HistoryFailed {
        generation,
        key: key.clone(),
        error: market_error(MarketDataErrorKind::Unavailable),
    });

    assert_eq!(model.charts().get(&key), Some(&original));
    assert_eq!(
        model.warnings().last().expect("history warning").message,
        "Chart history is temporarily unavailable."
    );
}

#[test]
fn adding_new_crypto_requests_snapshot_complete_crypto_watchlist_and_one_day_history() {
    let runtime = RuntimeHandle::default();
    let mut model = AppModel::new(
        PersistedState::starter(),
        CachedMarketState::default(),
        runtime.clone(),
    );
    let generation = model.begin_search("sol");
    let result = sol_result();
    model.apply_search_results(generation, vec![result.clone()], Vec::new());

    model.add_search_result(0).expect("add SOL");

    let commands = runtime.recorded_commands();
    assert!(matches!(
        &commands[0],
        RuntimeCommand::Snapshot { instruments }
            if instruments.last().map(|instrument| &instrument.id) == Some(&result.instrument.id)
    ));
    assert!(matches!(
        &commands[1],
        RuntimeCommand::SetCryptoWatchlist(instruments)
            if instruments
                .iter()
                .map(|instrument| instrument.id.canonical_symbol())
                .collect::<Vec<_>>() == ["BTC/USDT", "ETH/USDT", "SOL/USDT"]
    ));
    assert!(matches!(
        &commands[2],
        RuntimeCommand::History { instrument, period: ChartPeriod::OneDay, .. }
            if instrument.id == result.instrument.id
    ));
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

fn nvda_result() -> SearchResult {
    SearchResult {
        instrument: Instrument::equity("NVDA", Some("NASDAQ"), "NVIDIA Corporation", "USD")
            .expect("NVDA"),
        source: DataSource::Yahoo,
        venue_label: "NASDAQ".into(),
    }
}

fn btc_result() -> SearchResult {
    SearchResult {
        instrument: Instrument::crypto("BTC", "USDT", "Bitcoin").expect("BTC"),
        source: DataSource::Binance,
        venue_label: "Binance".into(),
    }
}

fn msft_result() -> SearchResult {
    SearchResult {
        instrument: Instrument::equity("MSFT", Some("NASDAQ"), "Microsoft", "USD").expect("MSFT"),
        source: DataSource::Yahoo,
        venue_label: "NASDAQ".into(),
    }
}

fn sol_result() -> SearchResult {
    SearchResult {
        instrument: Instrument::crypto("SOL", "USDT", "Solana").expect("SOL"),
        source: DataSource::Binance,
        venue_label: "Binance".into(),
    }
}

fn history_candle(close: f64) -> Candle {
    Candle {
        starts_at: chrono::DateTime::from_timestamp(1_700_000_000, 0).expect("timestamp"),
        open: close - 1.0,
        high: close + 1.0,
        low: close - 2.0,
        close,
        volume: 1_000.0,
    }
}

fn model_with_cached_chart(
    period: ChartPeriod,
    age: Duration,
) -> (AppModel, RuntimeHandle, chrono::DateTime<chrono::Utc>) {
    let state = PersistedState::starter();
    let instrument = state
        .watchlist
        .selected()
        .expect("starter selection")
        .clone();
    let now = ts(1_800_000_000);
    let mut cache = CachedMarketState::default();
    cache.charts.insert(
        ChartRequestKey {
            instrument_id: instrument.id,
            period,
        },
        kabu_core::CachedChart {
            candles: vec![history_candle(101.0)],
            fetched_at: now - chrono::Duration::from_std(age).expect("test age"),
        },
    );
    let runtime = RuntimeHandle::default();
    let model = AppModel::new(state, cache, runtime.clone());
    (model, runtime, now)
}

fn ts(seconds: i64) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::from_timestamp(seconds, 0).expect("timestamp")
}

fn market_error(kind: MarketDataErrorKind) -> MarketDataError {
    MarketDataError {
        kind,
        message: "controlled history error".into(),
        retry_after: None,
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
