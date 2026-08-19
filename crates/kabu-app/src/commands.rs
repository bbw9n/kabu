use gpui::{App, Global, WeakEntity, actions};
use kabu_core::ThemePreference;

use crate::views::AppShell;

actions!(
    kabu,
    [
        SelectSystemTheme,
        SelectGraphiteTheme,
        SelectMidnightTheme,
        SelectSignalTheme
    ]
);

#[derive(Default)]
struct ThemeActionTarget {
    generation: u64,
    shell: Option<WeakEntity<AppShell>>,
}

impl Global for ThemeActionTarget {}

pub fn init(cx: &mut App) {
    if cx.has_global::<ThemeActionTarget>() {
        return;
    }

    cx.set_global(ThemeActionTarget::default());
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
}

pub(crate) fn register_theme_action_target(shell: WeakEntity<AppShell>, cx: &mut App) -> u64 {
    init(cx);
    let target = cx.global_mut::<ThemeActionTarget>();
    target.generation = target.generation.wrapping_add(1);
    target.shell = Some(shell);
    target.generation
}

pub(crate) fn clear_theme_action_target(generation: u64, cx: &mut App) {
    let target = cx.global_mut::<ThemeActionTarget>();
    if target.generation == generation {
        target.shell = None;
    }
}

fn dispatch_theme(preference: ThemePreference, cx: &mut App) {
    let shell = cx.global::<ThemeActionTarget>().shell.clone();
    if let Some(shell) = shell {
        _ = shell.update(cx, |shell, cx| {
            shell.select_theme_preference(preference, cx);
        });
    }
}
