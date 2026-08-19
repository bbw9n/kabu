use gpui::{App, IntoElement, ParentElement as _, RenderOnce, Styled as _, Window, div};
use gpui_component::{Selectable as _, button::Button};

use crate::{
    commands::{SelectGraphiteTheme, SelectMidnightTheme, SelectSignalTheme},
    theme::KabuTheme,
};

#[derive(IntoElement)]
pub struct ThemeSwitcher {
    active: KabuTheme,
}

impl ThemeSwitcher {
    pub const fn new(active: KabuTheme) -> Self {
        Self { active }
    }
}

impl RenderOnce for ThemeSwitcher {
    fn render(self, _: &mut Window, _: &mut App) -> impl IntoElement {
        div()
            .flex()
            .items_center()
            .gap_1()
            .child(
                Button::new("theme-graphite")
                    .label("Graphite")
                    .compact()
                    .selected(self.active == KabuTheme::Graphite)
                    .tooltip("Use Graphite theme")
                    .on_click(|_, window, cx| {
                        window.dispatch_action(Box::new(SelectGraphiteTheme), cx);
                    }),
            )
            .child(
                Button::new("theme-midnight")
                    .label("Midnight")
                    .compact()
                    .selected(self.active == KabuTheme::Midnight)
                    .tooltip("Use Midnight theme")
                    .on_click(|_, window, cx| {
                        window.dispatch_action(Box::new(SelectMidnightTheme), cx);
                    }),
            )
            .child(
                Button::new("theme-signal")
                    .label("Signal")
                    .compact()
                    .selected(self.active == KabuTheme::Signal)
                    .tooltip("Use Signal theme")
                    .on_click(|_, window, cx| {
                        window.dispatch_action(Box::new(SelectSignalTheme), cx);
                    }),
            )
    }
}
