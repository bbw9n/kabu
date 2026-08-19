use gpui::{
    App, Context, IntoElement, ParentElement as _, Render, Styled as _, Subscription, Window,
    WindowAppearance, div, px,
};
use gpui_component::{ActiveTheme as _, IconName, StyledExt as _, TitleBar, button::Button};
use kabu_core::ThemePreference;

use crate::{
    commands::{clear_theme_action_target, register_theme_action_target},
    theme::{KabuTheme, ThemeSelection, apply_theme},
    views::ThemeSwitcher,
};

pub const DEFAULT_WINDOW_WIDTH: f32 = 980.0;
pub const DEFAULT_WINDOW_HEIGHT: f32 = 640.0;
pub const MIN_WINDOW_WIDTH: f32 = 760.0;
pub const MIN_WINDOW_HEIGHT: f32 = 500.0;
pub const SIDEBAR_WIDTH: f32 = 280.0;

type IconButton = Button;

pub struct AppShell {
    theme_selection: ThemeSelection,
    active_theme: KabuTheme,
    window_appearance: WindowAppearance,
    _appearance_subscription: Subscription,
    _action_target_subscription: Subscription,
}

impl AppShell {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let theme_selection = ThemeSelection::new(ThemePreference::System);
        let window_appearance = window.appearance();
        let active_theme = theme_selection.resolve(window_appearance);
        apply_theme(active_theme, Some(window), cx);

        let appearance_subscription = cx.observe_window_appearance(window, |shell, window, cx| {
            shell.window_appearance_changed(window, cx);
        });
        let target_generation = register_theme_action_target(
            cx.entity().downgrade(),
            std::borrow::BorrowMut::<App>::borrow_mut(cx),
        );
        let action_target_subscription = cx.on_release(move |_, cx| {
            clear_theme_action_target(target_generation, cx);
        });

        Self {
            theme_selection,
            active_theme,
            window_appearance,
            _appearance_subscription: appearance_subscription,
            _action_target_subscription: action_target_subscription,
        }
    }

    pub const fn theme_preference(&self) -> ThemePreference {
        self.theme_selection.preference()
    }

    pub const fn active_theme(&self) -> KabuTheme {
        self.active_theme
    }

    pub(crate) fn select_theme_preference(
        &mut self,
        preference: ThemePreference,
        cx: &mut Context<Self>,
    ) {
        self.theme_selection.set_preference(preference);
        self.active_theme = self.theme_selection.resolve(self.window_appearance);
        apply_theme(self.active_theme, None, cx);
        cx.notify();
    }

    fn window_appearance_changed(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.window_appearance = window.appearance();
        if self.theme_preference() != ThemePreference::System {
            return;
        }

        self.active_theme = self.theme_selection.resolve(self.window_appearance);
        apply_theme(self.active_theme, Some(window), cx);
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
                .child(ThemeSwitcher::new(self.theme_preference())),
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
