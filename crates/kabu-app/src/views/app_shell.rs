use gpui::{App, Context, IntoElement, ParentElement as _, Render, Styled as _, Window, div, px};
use gpui_component::{ActiveTheme as _, IconName, StyledExt as _, TitleBar, button::Button};
use kabu_core::ThemePreference;

use crate::{
    commands::{SelectGraphiteTheme, SelectMidnightTheme, SelectSignalTheme},
    theme::{KabuTheme, apply_theme},
    views::ThemeSwitcher,
};

pub const DEFAULT_WINDOW_WIDTH: f32 = 980.0;
pub const DEFAULT_WINDOW_HEIGHT: f32 = 640.0;
pub const MIN_WINDOW_WIDTH: f32 = 760.0;
pub const MIN_WINDOW_HEIGHT: f32 = 500.0;
pub const SIDEBAR_WIDTH: f32 = 280.0;

type IconButton = Button;

pub struct AppShell {
    active_theme: KabuTheme,
}

impl AppShell {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let active_theme = KabuTheme::resolve(ThemePreference::System, window.appearance());
        apply_theme(active_theme, Some(window), cx);

        let shell = cx.entity().downgrade();
        std::borrow::BorrowMut::<App>::borrow_mut(cx).on_action(
            move |_: &SelectGraphiteTheme, cx| {
                _ = shell.update(cx, |shell, cx| {
                    shell.select_theme(KabuTheme::Graphite, cx);
                });
            },
        );
        let shell = cx.entity().downgrade();
        std::borrow::BorrowMut::<App>::borrow_mut(cx).on_action(
            move |_: &SelectMidnightTheme, cx| {
                _ = shell.update(cx, |shell, cx| {
                    shell.select_theme(KabuTheme::Midnight, cx);
                });
            },
        );
        let shell = cx.entity().downgrade();
        std::borrow::BorrowMut::<App>::borrow_mut(cx).on_action(
            move |_: &SelectSignalTheme, cx| {
                _ = shell.update(cx, |shell, cx| {
                    shell.select_theme(KabuTheme::Signal, cx);
                });
            },
        );

        Self { active_theme }
    }

    pub const fn active_theme(&self) -> KabuTheme {
        self.active_theme
    }

    fn select_theme(&mut self, theme: KabuTheme, cx: &mut Context<Self>) {
        self.active_theme = theme;
        apply_theme(theme, None, cx);
        cx.notify();
    }

    fn title_bar(&self) -> impl IntoElement {
        TitleBar::new().child(
            div()
                .flex()
                .items_center()
                .w_full()
                .gap_2()
                .child(div().text_lg().font_semibold().child("Kabu"))
                .child(div().flex_1())
                .child(
                    IconButton::new("search")
                        .icon(IconName::Search)
                        .tooltip("Search instruments")
                        .on_click(|_, _, _| {}),
                )
                .child(ThemeSwitcher::new(self.active_theme)),
        )
    }
}

impl Render for AppShell {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .flex()
            .flex_col()
            .size_full()
            .bg(cx.theme().background)
            .text_color(cx.theme().foreground)
            .child(self.title_bar())
            .child(
                div()
                    .flex()
                    .flex_1()
                    .min_h_0()
                    .child(
                        div()
                            .w(px(SIDEBAR_WIDTH))
                            .h_full()
                            .flex_shrink_0()
                            .border_r_1()
                            .border_color(cx.theme().border)
                            .bg(cx.theme().sidebar)
                            .p_4()
                            .child(div().font_semibold().child("Inspector"))
                            .child(
                                div()
                                    .mt_2()
                                    .text_sm()
                                    .text_color(cx.theme().muted_foreground)
                                    .child("Watchlist and search results"),
                            ),
                    )
                    .child(
                        div()
                            .flex()
                            .flex_1()
                            .h_full()
                            .items_center()
                            .justify_center()
                            .bg(cx.theme().background)
                            .text_color(cx.theme().muted_foreground)
                            .child("Select an instrument to inspect"),
                    ),
            )
    }
}
