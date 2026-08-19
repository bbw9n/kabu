use gpui::{AppContext as _, Entity, Modifiers, WindowAppearance, px, size};
use gpui_component::Root;
use kabu_app::runtime::{RuntimeCommand, RuntimeHandle};
use kabu_app::{
    app_model::AppModel,
    commands::{
        RemoveSelectedInstrument, SelectMidnightTheme, SelectNextWatchlist,
        SelectPreviousWatchlist, SelectSignalTheme, SelectSystemTheme,
    },
    theme::{KabuTheme, ThemeSelection},
    views::{
        AppShell, DEFAULT_WINDOW_HEIGHT, DEFAULT_WINDOW_WIDTH, MIN_WINDOW_HEIGHT, MIN_WINDOW_WIDTH,
        SIDEBAR_WIDTH, SelectedQuoteAvailability,
    },
};
use kabu_core::{
    CachedMarketState, DataSource, Instrument, InstrumentId, MarketState, PersistedState, Quote,
    SearchResult, StateRepository, ThemePreference,
};
use kabu_data::JsonRepository;
use std::{cell::RefCell, rc::Rc, sync::Arc, time::Duration};

#[test]
fn window_shell_uses_approved_geometry() {
    assert_eq!(
        (DEFAULT_WINDOW_WIDTH, DEFAULT_WINDOW_HEIGHT),
        (980.0, 640.0)
    );
    assert_eq!((MIN_WINDOW_WIDTH, MIN_WINDOW_HEIGHT), (760.0, 500.0));
    assert_eq!(SIDEBAR_WIDTH, 280.0);
}

#[gpui::test]
fn watchlist_actions_require_focus_and_remove_the_selected_row(cx: &mut gpui::TestAppContext) {
    cx.update(|cx| {
        gpui_component::init(cx);
        kabu_app::commands::init(cx);
    });
    let (shell, cx) = cx.add_window_view(AppShell::new);
    let model = shell.update(cx, |shell, _| shell.model());

    cx.refresh().expect("render shell");
    cx.dispatch_action(SelectNextWatchlist);
    model.read_with(cx, |model, _| {
        assert_eq!(
            model.selected().expect("selection").id.canonical_symbol(),
            "AAPL"
        );
    });

    cx.update(|window, cx| {
        shell.update(cx, |shell, _| shell.focus_watchlist(window));
    });
    cx.refresh().expect("render focused shell");
    cx.dispatch_action(SelectNextWatchlist);
    model.read_with(cx, |model, _| {
        assert_eq!(
            model.selected().expect("selection").id.canonical_symbol(),
            "NVDA"
        );
    });

    cx.dispatch_action(RemoveSelectedInstrument);
    model.read_with(cx, |model, _| {
        assert_eq!(model.watchlist().len(), 3);
        assert_eq!(
            model.selected().expect("selection").id.canonical_symbol(),
            "BTC/USDT"
        );
    });

    cx.dispatch_action(SelectPreviousWatchlist);
    model.read_with(cx, |model, _| {
        assert_eq!(
            model.selected().expect("selection").id.canonical_symbol(),
            "AAPL"
        );
    });
}

#[gpui::test]
fn clicking_a_watchlist_row_selects_it(cx: &mut gpui::TestAppContext) {
    cx.update(|cx| {
        gpui_component::init(cx);
        kabu_app::commands::init(cx);
    });
    let model = cached_watchlist_model(cx);
    let (shell, cx) =
        cx.add_window_view(move |window, cx| AppShell::with_model(model.clone(), window, cx));
    let model = shell.update(cx, |shell, _| shell.model());
    cx.refresh().expect("render shell");

    shell.update(cx, |shell, cx| {
        let header = shell.selected_quote_header(cx);
        assert_eq!(header.symbol, "AAPL");
        assert_eq!(header.price, "100.00 USD");
    });

    let row = cx.debug_bounds("watchlist-row-1").expect("NVDA row bounds");
    cx.simulate_click(row.center(), Modifiers::none());

    model.read_with(cx, |model, _| {
        assert_eq!(
            model.selected().expect("selection").id.canonical_symbol(),
            "NVDA"
        );
    });
    shell.update(cx, |shell, cx| {
        let header = shell.selected_quote_header(cx);
        assert_eq!(header.symbol, "NVDA");
        assert_eq!(header.price, "200.00 USD");
        assert_eq!(header.change, "−10.00 (−4.76%)");
        assert_eq!(header.source, "Yahoo");
        assert_eq!(header.provider_timestamp, "2023-11-14T22:13:20+00:00");
        assert_eq!(header.availability, SelectedQuoteAvailability::Available);
    });
}

#[gpui::test]
fn keyboard_selection_updates_the_cached_quote_header_without_runtime_work(
    cx: &mut gpui::TestAppContext,
) {
    cx.update(|cx| {
        gpui_component::init(cx);
        kabu_app::commands::init(cx);
    });
    let model = cached_watchlist_model(cx);
    let (shell, cx) =
        cx.add_window_view(move |window, cx| AppShell::with_model(model.clone(), window, cx));
    cx.update(|window, cx| {
        shell.update(cx, |shell, _| shell.focus_watchlist(window));
    });
    cx.refresh().expect("render focused cached shell");

    cx.dispatch_action(SelectNextWatchlist);

    shell.update(cx, |shell, cx| {
        let header = shell.selected_quote_header(cx);
        assert_eq!(header.symbol, "NVDA");
        assert_eq!(header.price, "200.00 USD");
        assert_eq!(header.change, "−10.00 (−4.76%)");
        assert_eq!(header.source, "Yahoo");
        assert_eq!(header.provider_timestamp, "2023-11-14T22:13:20+00:00");
    });
}

#[gpui::test]
fn selected_quote_header_distinguishes_missing_and_loading_without_zero_values(
    cx: &mut gpui::TestAppContext,
) {
    cx.update(|cx| {
        gpui_component::init(cx);
        kabu_app::commands::init(cx);
    });
    let (shell, cx) = cx.add_window_view(AppShell::new);
    let model = shell.update(cx, |shell, _| shell.model());

    shell.update(cx, |shell, cx| {
        let header = shell.selected_quote_header(cx);
        assert_eq!(header.price, "—");
        assert_eq!(header.change, "—");
        assert_eq!(header.status, "No cached quote");
        assert_eq!(header.availability, SelectedQuoteAvailability::Unavailable);
    });

    model.update(cx, |model, cx| {
        let selected = model.selected_identity().expect("selection").clone();
        model.set_quote_loading(selected, true);
        cx.notify();
    });

    shell.update(cx, |shell, cx| {
        let header = shell.selected_quote_header(cx);
        assert_eq!(header.price, "—");
        assert_eq!(header.change, "—");
        assert_eq!(header.status, "Loading quote");
        assert_eq!(header.availability, SelectedQuoteAvailability::Loading);
    });
}

#[gpui::test]
fn window_geometry_persists_after_exactly_250_milliseconds(cx: &mut gpui::TestAppContext) {
    cx.update(|cx| {
        gpui_component::init(cx);
        kabu_app::commands::init(cx);
    });
    let directory = tempfile::tempdir().expect("temporary repository");
    let repository = Arc::new(JsonRepository::at(directory.path()));
    let model = cx.update(|cx| {
        cx.new(|_| AppModel::with_shared_repository(repository.clone(), RuntimeHandle::default()))
    });
    let executor = cx.executor();
    let (_, window_cx) =
        cx.add_window_view(move |window, cx| AppShell::with_model(model.clone(), window, cx));

    window_cx.simulate_resize(size(px(900.0), px(580.0)));
    window_cx.run_until_parked();
    executor.advance_clock(Duration::from_millis(249));
    assert_eq!(
        repository
            .load_state()
            .expect("state before debounce")
            .window_width,
        980.0
    );

    executor.advance_clock(Duration::from_millis(1));
    window_cx.run_until_parked();
    let saved = repository.load_state().expect("debounced state");
    assert_eq!((saved.window_width, saved.window_height), (900.0, 580.0));

    window_cx.simulate_resize(size(px(840.0), px(540.0)));
    window_cx.run_until_parked();
    assert!(window_cx.simulate_close());
    let flushed = repository.load_state().expect("close-flushed state");
    assert_eq!(
        (flushed.window_width, flushed.window_height),
        (840.0, 540.0)
    );
}

#[test]
fn manual_theme_preferences_ignore_window_appearance() {
    let cases = [
        (ThemePreference::Graphite, KabuTheme::Graphite),
        (ThemePreference::Midnight, KabuTheme::Midnight),
        (ThemePreference::Signal, KabuTheme::Signal),
    ];

    for (preference, expected) in cases {
        assert_eq!(
            KabuTheme::resolve(preference, WindowAppearance::Light),
            expected
        );
        assert_eq!(
            KabuTheme::resolve(preference, WindowAppearance::Dark),
            expected
        );
    }
}

#[test]
fn system_selection_stays_selected_while_resolving_light() {
    let selection = ThemeSelection::new(ThemePreference::System);

    assert_eq!(selection.preference(), ThemePreference::System);
    assert_eq!(
        selection.resolve(WindowAppearance::Light),
        KabuTheme::Graphite
    );
}

#[test]
fn system_selection_re_resolves_when_appearance_changes() {
    let selection = ThemeSelection::new(ThemePreference::System);

    assert_eq!(
        selection.resolve(WindowAppearance::Light),
        KabuTheme::Graphite
    );
    assert_eq!(
        selection.resolve(WindowAppearance::Dark),
        KabuTheme::Midnight
    );
    assert_eq!(selection.preference(), ThemePreference::System);
}

#[gpui::test]
fn theme_actions_update_the_shell(cx: &mut gpui::TestAppContext) {
    cx.update(|cx| {
        gpui_component::init(cx);
        kabu_app::commands::init(cx);
    });
    let (shell, cx) = cx.add_window_view(AppShell::new);

    cx.dispatch_action(SelectSignalTheme);
    shell.update(cx, |shell, _| {
        assert_eq!(shell.active_theme(), KabuTheme::Signal);
    });

    cx.dispatch_action(SelectMidnightTheme);
    shell.update(cx, |shell, _| {
        assert_eq!(shell.active_theme(), KabuTheme::Midnight);
    });

    cx.dispatch_action(SelectSystemTheme);
    shell.update(cx, |shell, _| {
        assert_eq!(shell.theme_preference(), ThemePreference::System);
        assert_eq!(shell.active_theme(), KabuTheme::Graphite);
    });
}

#[gpui::test]
fn theme_action_target_moves_to_recreated_shell_and_clears_on_release(
    cx: &mut gpui::TestAppContext,
) {
    cx.update(|cx| {
        gpui_component::init(cx);
        kabu_app::commands::init(cx);
    });
    let (first_shell, cx) = cx.add_window_view(AppShell::new);

    cx.dispatch_action(SelectSignalTheme);
    first_shell.update(cx, |shell, _| {
        assert_eq!(shell.theme_preference(), ThemePreference::Signal);
    });

    let second_shell = cx.update(|window, cx| window.replace_root(cx, AppShell::new));
    cx.dispatch_action(SelectMidnightTheme);
    first_shell.update(cx, |shell, _| {
        assert_eq!(shell.theme_preference(), ThemePreference::Signal);
    });
    second_shell.update(cx, |shell, _| {
        assert_eq!(shell.theme_preference(), ThemePreference::Midnight);
    });

    cx.update(|window, cx| {
        window.replace_root(cx, |_, _| gpui::Empty);
    });
    drop(second_shell);
    cx.run_until_parked();
    cx.dispatch_action(SelectSystemTheme);
    first_shell.update(cx, |shell, _| {
        assert_eq!(shell.theme_preference(), ThemePreference::Signal);
    });
}

#[gpui::test]
fn command_k_opens_search_and_focuses_its_input(cx: &mut gpui::TestAppContext) {
    cx.update(|cx| {
        gpui_component::init(cx);
        kabu_app::commands::init(cx);
    });
    let shell_slot = Rc::new(RefCell::new(None));
    let shell_capture = shell_slot.clone();
    let (_, cx) = cx.add_window_view(move |window, cx| {
        let shell = cx.new(|cx| AppShell::new(window, cx));
        *shell_capture.borrow_mut() = Some(shell.clone());
        Root::new(shell, window, cx)
    });
    let shell: Entity<AppShell> = shell_slot.borrow().clone().expect("shell");

    cx.simulate_keystrokes("cmd-k");
    cx.refresh().expect("render search overlay");

    shell.update(cx, |shell, _| assert!(shell.search_is_open()));
    cx.update(|window, cx| {
        let focus = shell.update(cx, |shell, cx| shell.search_input_focus_handle(cx));
        assert!(focus.is_focused(window));
    });
    assert!(cx.debug_bounds("search-overlay").is_some());
}

#[gpui::test]
fn search_input_debounces_for_exactly_250_milliseconds(cx: &mut gpui::TestAppContext) {
    cx.update(|cx| {
        gpui_component::init(cx);
        kabu_app::commands::init(cx);
    });
    let runtime = RuntimeHandle::default();
    let model = cx.update(|cx| {
        cx.new(|_| {
            AppModel::new(
                PersistedState::starter(),
                CachedMarketState::default(),
                runtime.clone(),
            )
        })
    });
    let executor = cx.executor();
    let shell_slot = Rc::new(RefCell::new(None));
    let shell_capture = shell_slot.clone();
    let (_, window_cx) = cx.add_window_view(move |window, cx| {
        let shell = cx.new(|cx| AppShell::with_model(model.clone(), window, cx));
        *shell_capture.borrow_mut() = Some(shell.clone());
        Root::new(shell, window, cx)
    });

    window_cx.simulate_keystrokes("cmd-k");
    window_cx.refresh().expect("render search overlay");
    window_cx.simulate_input("nv");
    window_cx.run_until_parked();
    executor.advance_clock(Duration::from_millis(249));
    window_cx.run_until_parked();
    assert!(runtime.recorded_commands().is_empty());

    executor.advance_clock(Duration::from_millis(1));
    window_cx.run_until_parked();
    assert!(matches!(
        runtime.recorded_commands().last(),
        Some(RuntimeCommand::Search { query, .. }) if query == "nv"
    ));
}

#[gpui::test]
fn arrows_and_return_select_an_existing_result_and_close_search(cx: &mut gpui::TestAppContext) {
    cx.update(|cx| {
        gpui_component::init(cx);
        kabu_app::commands::init(cx);
    });
    let runtime = RuntimeHandle::default();
    let model = cx.update(|cx| {
        cx.new(|_| {
            let mut model = AppModel::new(
                PersistedState::starter(),
                CachedMarketState::default(),
                runtime.clone(),
            );
            model.select_index(2).expect("select BTC first");
            let generation = model.begin_search("n");
            model.apply_search_results(generation, vec![aapl_result(), nvda_result()], Vec::new());
            model
        })
    });
    let shell_slot = Rc::new(RefCell::new(None));
    let shell_capture = shell_slot.clone();
    let (_, cx) = cx.add_window_view(move |window, cx| {
        let shell = cx.new(|cx| AppShell::with_model(model.clone(), window, cx));
        *shell_capture.borrow_mut() = Some(shell.clone());
        Root::new(shell, window, cx)
    });
    let shell: Entity<AppShell> = shell_slot.borrow().clone().expect("shell");
    let model = shell.update(cx, |shell, _| shell.model());

    cx.simulate_keystrokes("cmd-k down enter");
    cx.run_until_parked();

    shell.update(cx, |shell, _| assert!(!shell.search_is_open()));
    model.read_with(cx, |model, _| {
        assert_eq!(model.watchlist().len(), 4);
        assert_eq!(
            model
                .selected()
                .expect("existing NVDA selected")
                .id
                .canonical_symbol(),
            "NVDA"
        );
    });
    assert_eq!(runtime.recorded_commands().len(), 1);
    assert!(matches!(
        runtime.recorded_commands().first(),
        Some(RuntimeCommand::History { instrument, .. })
            if instrument.id.canonical_symbol() == "NVDA"
    ));
}

#[gpui::test]
fn escape_closes_search_and_restores_watchlist_focus(cx: &mut gpui::TestAppContext) {
    cx.update(|cx| {
        gpui_component::init(cx);
        kabu_app::commands::init(cx);
    });
    let shell_slot = Rc::new(RefCell::new(None));
    let shell_capture = shell_slot.clone();
    let (_, cx) = cx.add_window_view(move |window, cx| {
        let shell = cx.new(|cx| AppShell::new(window, cx));
        *shell_capture.borrow_mut() = Some(shell.clone());
        Root::new(shell, window, cx)
    });
    let shell: Entity<AppShell> = shell_slot.borrow().clone().expect("shell");

    cx.simulate_keystrokes("cmd-k escape");
    cx.run_until_parked();

    shell.update(cx, |shell, _| assert!(!shell.search_is_open()));
    cx.dispatch_action(SelectNextWatchlist);
    let model = shell.update(cx, |shell, _| shell.model());
    model.read_with(cx, |model, _| {
        assert_eq!(
            model
                .selected()
                .expect("watchlist focus restored")
                .id
                .canonical_symbol(),
            "NVDA"
        );
    });
}

fn cached_watchlist_model(cx: &mut gpui::TestAppContext) -> gpui::Entity<AppModel> {
    let aapl = InstrumentId::equity("AAPL", Some("NASDAQ")).expect("AAPL id");
    let nvda = InstrumentId::equity("NVDA", Some("NASDAQ")).expect("NVDA id");
    let mut cache = CachedMarketState::default();
    cache
        .quotes
        .insert(aapl.clone(), cached_quote(aapl, 100.0, 95.0));
    cache
        .quotes
        .insert(nvda.clone(), cached_quote(nvda, 200.0, 210.0));
    cx.update(|cx| {
        cx.new(|_| AppModel::new(PersistedState::starter(), cache, RuntimeHandle::default()))
    })
}

fn cached_quote(instrument_id: InstrumentId, last: f64, previous_close: f64) -> Quote {
    let timestamp = chrono::DateTime::from_timestamp(1_700_000_000, 0).expect("timestamp");
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

fn aapl_result() -> SearchResult {
    SearchResult {
        instrument: Instrument::equity("AAPL", Some("NASDAQ"), "Apple", "USD").expect("AAPL"),
        source: DataSource::Yahoo,
        venue_label: "NASDAQ".into(),
    }
}

fn nvda_result() -> SearchResult {
    SearchResult {
        instrument: Instrument::equity("NVDA", Some("NASDAQ"), "NVIDIA", "USD").expect("NVDA"),
        source: DataSource::Yahoo,
        venue_label: "NASDAQ".into(),
    }
}
