use gpui::WindowAppearance;
use kabu_app::{
    commands::{SelectMidnightTheme, SelectSignalTheme, SelectSystemTheme},
    theme::{KabuTheme, ThemeSelection},
    views::{
        AppShell, DEFAULT_WINDOW_HEIGHT, DEFAULT_WINDOW_WIDTH, MIN_WINDOW_HEIGHT, MIN_WINDOW_WIDTH,
        SIDEBAR_WIDTH,
    },
};
use kabu_core::ThemePreference;

#[test]
fn window_shell_uses_approved_geometry() {
    assert_eq!(
        (DEFAULT_WINDOW_WIDTH, DEFAULT_WINDOW_HEIGHT),
        (980.0, 640.0)
    );
    assert_eq!((MIN_WINDOW_WIDTH, MIN_WINDOW_HEIGHT), (760.0, 500.0));
    assert_eq!(SIDEBAR_WIDTH, 280.0);
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
