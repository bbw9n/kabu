use gpui::{App, Global, KeyBinding, WeakEntity, actions};
use kabu_core::ThemePreference;

use crate::views::AppShell;

actions!(
    kabu,
    [
        SelectSystemTheme,
        SelectGraphiteTheme,
        SelectMidnightTheme,
        SelectSignalTheme,
        SelectPreviousWatchlist,
        SelectNextWatchlist,
        RemoveSelectedInstrument,
        OpenSearch,
        CloseSearch,
        SelectPreviousSearchResult,
        SelectNextSearchResult
    ]
);

#[derive(Default)]
struct ShellActionTarget {
    generation: u64,
    shell: Option<WeakEntity<AppShell>>,
}

impl Global for ShellActionTarget {}

pub fn init(cx: &mut App) {
    if cx.has_global::<ShellActionTarget>() {
        return;
    }

    cx.bind_keys([
        KeyBinding::new("up", SelectPreviousWatchlist, Some("Watchlist")),
        KeyBinding::new("down", SelectNextWatchlist, Some("Watchlist")),
        KeyBinding::new("backspace", RemoveSelectedInstrument, Some("Watchlist")),
        KeyBinding::new("delete", RemoveSelectedInstrument, Some("Watchlist")),
        KeyBinding::new("cmd-k", OpenSearch, None),
        KeyBinding::new("escape", CloseSearch, Some("SearchOverlay")),
        KeyBinding::new("up", SelectPreviousSearchResult, Some("SearchOverlay")),
        KeyBinding::new("down", SelectNextSearchResult, Some("SearchOverlay")),
    ]);
    cx.set_global(ShellActionTarget::default());
    cx.on_action(|_: &SelectSystemTheme, cx| {
        dispatch_theme(ThemePreference::System, cx);
    });
    cx.on_action(|_: &SelectGraphiteTheme, cx| {
        dispatch_theme(ThemePreference::Graphite, cx);
    });
    cx.on_action(|_: &SelectMidnightTheme, cx| {
        dispatch_theme(ThemePreference::Midnight, cx);
    });
    cx.on_action(|_: &SelectSignalTheme, cx| {
        dispatch_theme(ThemePreference::Signal, cx);
    });
    cx.on_action(|_: &OpenSearch, cx| {
        let shell = cx.global::<ShellActionTarget>().shell.clone();
        if let Some(shell) = shell {
            _ = shell.update(cx, |shell, cx| shell.open_search(cx));
        }
    });
}

pub(crate) fn register_theme_action_target(shell: WeakEntity<AppShell>, cx: &mut App) -> u64 {
    init(cx);
    let target = cx.global_mut::<ShellActionTarget>();
    target.generation = target.generation.wrapping_add(1);
    target.shell = Some(shell);
    target.generation
}

pub(crate) fn clear_theme_action_target(generation: u64, cx: &mut App) {
    let target = cx.global_mut::<ShellActionTarget>();
    if target.generation == generation {
        target.shell = None;
    }
}

fn dispatch_theme(preference: ThemePreference, cx: &mut App) {
    let shell = cx.global::<ShellActionTarget>().shell.clone();
    if let Some(shell) = shell {
        _ = shell.update(cx, |shell, cx| {
            shell.select_theme_preference(preference, cx);
        });
    }
}
