use gpui::{AppContext as _, Modifiers, WindowAppearance, px, size};
use kabu_app::runtime::RuntimeHandle;
use kabu_app::{
    app_model::AppModel,
    commands::{
        RemoveSelectedInstrument, SelectMidnightTheme, SelectNextWatchlist,
        SelectPreviousWatchlist, SelectSignalTheme, SelectSystemTheme,
    },
    theme::{KabuTheme, ThemeSelection},
    views::{
        AppShell, DEFAULT_WINDOW_HEIGHT, DEFAULT_WINDOW_WIDTH, MIN_WINDOW_HEIGHT, MIN_WINDOW_WIDTH,
        SIDEBAR_WIDTH,
    },
};
use kabu_core::{StateRepository, ThemePreference};
use kabu_data::JsonRepository;
use std::{sync::Arc, time::Duration};

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
    let (shell, cx) = cx.add_window_view(AppShell::new);
    let model = shell.update(cx, |shell, _| shell.model());
    cx.refresh().expect("render shell");

    let row = cx.debug_bounds("watchlist-row-2").expect("BTC row bounds");
    cx.simulate_click(row.center(), Modifiers::none());

    model.read_with(cx, |model, _| {
        assert_eq!(
            model.selected().expect("selection").id.canonical_symbol(),
            "BTC/USDT"
        );
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
