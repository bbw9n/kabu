use gpui::{App, IntoElement, ParentElement as _, RenderOnce, Styled as _, Window, div};
use gpui_component::{Selectable as _, button::Button};

use crate::commands::{
    SelectGraphiteTheme, SelectMidnightTheme, SelectSignalTheme, SelectSystemTheme,
};
use kabu_core::ThemePreference;

#[derive(IntoElement)]
pub struct ThemeSwitcher {
    preference: ThemePreference,
}

impl ThemeSwitcher {
    pub const fn new(preference: ThemePreference) -> Self {
        Self { preference }
    }
}

impl RenderOnce for ThemeSwitcher {
    fn render(self, _: &mut Window, _: &mut App) -> impl IntoElement {
        div()
            .flex()
            .items_center()
            .gap_1()
            .child(
                Button::new("theme-system")
                    .label("System")
                    .compact()
                    .selected(self.preference == ThemePreference::System)
                    .tooltip("Follow system appearance")
                    .on_click(|_, window, cx| {
                        window.dispatch_action(Box::new(SelectSystemTheme), cx);
                    }),
            )
            .child(
                Button::new("theme-graphite")
                    .label("Graphite")
                    .compact()
                    .selected(self.preference == ThemePreference::Graphite)
                    .tooltip("Use Graphite theme")
                    .on_click(|_, window, cx| {
                        window.dispatch_action(Box::new(SelectGraphiteTheme), cx);
                    }),
            )
            .child(
                Button::new("theme-midnight")
                    .label("Midnight")
                    .compact()
                    .selected(self.preference == ThemePreference::Midnight)
                    .tooltip("Use Midnight theme")
                    .on_click(|_, window, cx| {
                        window.dispatch_action(Box::new(SelectMidnightTheme), cx);
                    }),
            )
            .child(
                Button::new("theme-signal")
                    .label("Signal")
                    .compact()
                    .selected(self.preference == ThemePreference::Signal)
                    .tooltip("Use Signal theme")
                    .on_click(|_, window, cx| {
                        window.dispatch_action(Box::new(SelectSignalTheme), cx);
                    }),
            )
    }
}
