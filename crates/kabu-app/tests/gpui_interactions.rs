use gpui::WindowAppearance;
use kabu_app::{
    commands::{SelectMidnightTheme, SelectSignalTheme},
    theme::KabuTheme,
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

#[gpui::test]
fn theme_actions_update_the_shell(cx: &mut gpui::TestAppContext) {
    cx.update(|cx| {
        gpui_component::init(cx);
        kabu_app::commands::init(cx);
    });
    let window = cx.add_window(AppShell::new);
    cx.refresh().expect("test window refreshes");
    cx.run_until_parked();

    cx.dispatch_action(*window, SelectSignalTheme);
    window
        .update(cx, |shell, _, _| {
            assert_eq!(shell.active_theme(), KabuTheme::Signal);
        })
        .expect("test window remains open");

    cx.dispatch_action(*window, SelectMidnightTheme);
    window
        .update(cx, |shell, _, _| {
            assert_eq!(shell.active_theme(), KabuTheme::Midnight);
        })
        .expect("test window remains open");
}
