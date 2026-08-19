use gpui::{App, Window, WindowAppearance, rgb};
use gpui_component::{Theme, ThemeMode};
use kabu_core::ThemePreference;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Palette {
    pub surface: u32,
    pub background: u32,
    pub text: u32,
    pub gain: u32,
    pub loss: u32,
    pub border: u32,
}

impl Palette {
    pub const fn new(
        surface: u32,
        background: u32,
        text: u32,
        gain: u32,
        loss: u32,
        border: u32,
    ) -> Self {
        Self {
            surface,
            background,
            text,
            gain,
            loss,
            border,
        }
    }
}

pub const GRAPHITE: Palette =
    Palette::new(0xFFFFFF, 0xF7F8FA, 0x1C2430, 0x13875D, 0xCD454B, 0xE8EBF0);
pub const MIDNIGHT: Palette =
    Palette::new(0x171E27, 0x121820, 0xEAF0F5, 0x52D6A0, 0xFF7B78, 0x222B36);
pub const SIGNAL: Palette =
    Palette::new(0xFBFBFF, 0xEDF0FF, 0x19213C, 0x0C9B82, 0xE45261, 0xDCE2FF);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KabuTheme {
    Graphite,
    Midnight,
    Signal,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ThemeSelection {
    preference: ThemePreference,
}

impl ThemeSelection {
    pub const fn new(preference: ThemePreference) -> Self {
        Self { preference }
    }

    pub const fn preference(self) -> ThemePreference {
        self.preference
    }

    pub fn set_preference(&mut self, preference: ThemePreference) {
        self.preference = preference;
    }

    pub fn resolve(self, appearance: WindowAppearance) -> KabuTheme {
        KabuTheme::resolve(self.preference, appearance)
    }
}

impl KabuTheme {
    pub fn resolve(preference: ThemePreference, appearance: WindowAppearance) -> Self {
        match preference {
            ThemePreference::System => match appearance {
                WindowAppearance::Light | WindowAppearance::VibrantLight => Self::Graphite,
                WindowAppearance::Dark | WindowAppearance::VibrantDark => Self::Midnight,
            },
            ThemePreference::Graphite => Self::Graphite,
            ThemePreference::Midnight => Self::Midnight,
            ThemePreference::Signal => Self::Signal,
        }
    }

    pub const fn palette(self) -> Palette {
        match self {
            Self::Graphite => GRAPHITE,
            Self::Midnight => MIDNIGHT,
            Self::Signal => SIGNAL,
        }
    }
}

pub fn apply_theme(theme: KabuTheme, window: Option<&mut Window>, cx: &mut App) {
    let mode = match theme {
        KabuTheme::Graphite | KabuTheme::Signal => ThemeMode::Light,
        KabuTheme::Midnight => ThemeMode::Dark,
    };
    Theme::change(mode, window, cx);

    let palette = theme.palette();
    let colors = &mut Theme::global_mut(cx).colors;
    colors.background = rgb(palette.background).into();
    colors.foreground = rgb(palette.text).into();
    colors.border = rgb(palette.border).into();
    colors.muted = rgb(palette.border).into();
    colors.primary = rgb(palette.gain).into();
    colors.popover = rgb(palette.surface).into();
    colors.sidebar = rgb(palette.surface).into();
    colors.selection = rgb(palette.border).into();

    cx.refresh_windows();
}

#[cfg(test)]
mod tests {
    use super::*;
    use gpui::WindowAppearance;
    use kabu_core::ThemePreference;

    #[test]
    fn system_theme_maps_to_window_appearance() {
        assert_eq!(
            KabuTheme::resolve(ThemePreference::System, WindowAppearance::Light),
            KabuTheme::Graphite
        );
        assert_eq!(
            KabuTheme::resolve(ThemePreference::System, WindowAppearance::Dark),
            KabuTheme::Midnight
        );
    }

    #[test]
    fn each_theme_keeps_gain_and_loss_distinct() {
        for theme in [KabuTheme::Graphite, KabuTheme::Midnight, KabuTheme::Signal] {
            let palette = theme.palette();
            assert_ne!(palette.gain, palette.loss);
            assert_ne!(palette.surface, palette.text);
        }
    }
}
