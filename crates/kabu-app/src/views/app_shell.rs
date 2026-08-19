use gpui::{
    App, AppContext as _, Context, DragMoveEvent, Empty, Entity, FocusHandle,
    InteractiveElement as _, IntoElement, ParentElement as _, Render,
    StatefulInteractiveElement as _, Styled as _, Subscription, Window, WindowAppearance, div, px,
};
use gpui_component::{ActiveTheme as _, IconName, StyledExt as _, TitleBar, button::Button};
use kabu_core::{CachedMarketState, PersistedState, ThemePreference};
use std::{borrow::BorrowMut, time::Duration};

use crate::{
    app_model::{AppModel, DEFAULT_SIDEBAR_WIDTH},
    commands::{clear_theme_action_target, register_theme_action_target},
    runtime::RuntimeHandle,
    theme::{KabuTheme, ThemeSelection, apply_theme},
    views::{ThemeSwitcher, WatchlistPane},
};

type IconButton = Button;

#[derive(Clone)]
struct SidebarDrag;

impl Render for SidebarDrag {
    fn render(&mut self, _: &mut Window, _: &mut Context<Self>) -> impl IntoElement {
        Empty
    }
}

pub struct AppShell {
    model: Entity<AppModel>,
    watchlist_focus: FocusHandle,
    theme_selection: ThemeSelection,
    active_theme: KabuTheme,
    window_appearance: WindowAppearance,
    geometry_generation: u64,
    _model_subscription: Subscription,
    _appearance_subscription: Subscription,
    _bounds_subscription: Subscription,
    _action_target_subscription: Subscription,
}

impl AppShell {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let model = cx.new(|_| {
            AppModel::new(
                PersistedState::starter(),
                CachedMarketState::default(),
                RuntimeHandle::default(),
            )
        });
        Self::with_model(model, window, cx)
    }

    pub fn with_model(
        model: Entity<AppModel>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let theme_selection = ThemeSelection::new(model.read(cx).theme());
        let window_appearance = window.appearance();
        let active_theme = theme_selection.resolve(window_appearance);
        apply_theme(active_theme, Some(window), cx);

        let model_subscription = cx.observe(&model, |shell, model, cx| {
            shell.model_changed(&model, cx);
        });
        let appearance_subscription = cx.observe_window_appearance(window, |shell, window, cx| {
            shell.window_appearance_changed(window, cx);
        });
        let bounds_subscription = cx.observe_window_bounds(window, |shell, window, cx| {
            shell.window_bounds_changed(window, cx);
        });
        let target_generation =
            register_theme_action_target(cx.entity().downgrade(), BorrowMut::<App>::borrow_mut(cx));
        let action_target_subscription = cx.on_release(move |_, cx| {
            clear_theme_action_target(target_generation, cx);
        });

        let close_model = model.downgrade();
        window.on_window_should_close(cx, move |window, cx| {
            if let Some(model) = close_model.upgrade() {
                let size = window.window_bounds().get_bounds().size;
                model.update(cx, |model, cx| {
                    model.preview_window_geometry(f32::from(size.width), f32::from(size.height));
                    _ = model.persist_window_geometry();
                    cx.notify();
                });
            }
            true
        });

        Self {
            model,
            watchlist_focus: cx.focus_handle(),
            theme_selection,
            active_theme,
            window_appearance,
            geometry_generation: 0,
            _model_subscription: model_subscription,
            _appearance_subscription: appearance_subscription,
            _bounds_subscription: bounds_subscription,
            _action_target_subscription: action_target_subscription,
        }
    }

    pub fn model(&self) -> Entity<AppModel> {
        self.model.clone()
    }

    pub fn focus_watchlist(&self, window: &mut Window) {
        self.watchlist_focus.focus(window);
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
        self.model.update(cx, |model, cx| {
            _ = model.set_theme(preference);
            cx.notify();
        });
        self.theme_selection.set_preference(preference);
        self.active_theme = self.theme_selection.resolve(self.window_appearance);
        apply_theme(self.active_theme, None, cx);
        cx.notify();
    }

    fn model_changed(&mut self, model: &Entity<AppModel>, cx: &mut Context<Self>) {
        let preference = model.read(cx).theme();
        if self.theme_preference() != preference {
            self.theme_selection.set_preference(preference);
            self.active_theme = self.theme_selection.resolve(self.window_appearance);
            apply_theme(self.active_theme, None, cx);
        }
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

    fn window_bounds_changed(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let size = window.window_bounds().get_bounds().size;
        self.model.update(cx, |model, cx| {
            model.preview_window_geometry(f32::from(size.width), f32::from(size.height));
            cx.notify();
        });

        self.geometry_generation = self.geometry_generation.wrapping_add(1);
        let generation = self.geometry_generation;
        let debounce = cx.background_executor().timer(Duration::from_millis(250));
        cx.spawn(async move |shell, cx| {
            debounce.await;
            _ = cx.update(|cx| {
                if let Some(shell) = shell.upgrade() {
                    shell.update(cx, |shell, cx| {
                        if shell.geometry_generation == generation {
                            shell.model.update(cx, |model, cx| {
                                _ = model.persist_window_geometry();
                                cx.notify();
                            });
                        }
                    });
                }
            });
        })
        .detach();
    }

    fn preview_sidebar_width(
        &mut self,
        event: &DragMoveEvent<SidebarDrag>,
        cx: &mut Context<Self>,
    ) {
        let width = f32::from(event.event.position.x - event.bounds.left());
        self.model.update(cx, |model, cx| {
            model.preview_sidebar_width(width);
            cx.notify();
        });
    }

    fn persist_sidebar_width(&mut self, cx: &mut Context<Self>) {
        self.model.update(cx, |model, cx| {
            _ = model.persist_sidebar_width();
            cx.notify();
        });
    }

    fn reset_sidebar_width(&mut self, cx: &mut Context<Self>) {
        self.model.update(cx, |model, cx| {
            _ = model.set_sidebar_width(DEFAULT_SIDEBAR_WIDTH);
            cx.notify();
        });
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
        let sidebar_width = self.model.read(cx).sidebar_width();
        let selected_text = self
            .model
            .read(cx)
            .selected()
            .map(|instrument| {
                format!(
                    "{} · {}",
                    instrument.id.canonical_symbol(),
                    instrument.display_name
                )
            })
            .unwrap_or_else(|| "No instrument selected".into());
        let warning = self
            .model
            .read(cx)
            .warnings()
            .last()
            .map(|warning| warning.message.clone());

        div()
            .flex()
            .flex_col()
            .size_full()
            .bg(cx.theme().background)
            .text_color(cx.theme().foreground)
            .child(self.title_bar())
            .children(warning.map(|message| {
                div()
                    .border_b_1()
                    .border_color(cx.theme().warning)
                    .bg(cx.theme().warning.opacity(0.12))
                    .px_3()
                    .py_2()
                    .text_sm()
                    .child(message)
            }))
            .child(
                div()
                    .id("inspector-layout")
                    .relative()
                    .flex()
                    .flex_1()
                    .min_h_0()
                    .child(
                        div()
                            .w(px(sidebar_width))
                            .h_full()
                            .flex_shrink_0()
                            .bg(cx.theme().sidebar)
                            .child(WatchlistPane::new(
                                self.model.clone(),
                                self.watchlist_focus.clone(),
                            )),
                    )
                    .child(
                        div()
                            .id("sidebar-divider")
                            .w(px(8.0))
                            .h_full()
                            .flex_shrink_0()
                            .cursor_col_resize()
                            .border_l_1()
                            .border_color(cx.theme().border)
                            .on_drag(SidebarDrag, |drag, _, _, cx| {
                                cx.stop_propagation();
                                cx.new(|_| drag.clone())
                            })
                            .on_click(cx.listener(|shell, event: &gpui::ClickEvent, _, cx| {
                                if event.click_count() == 2 {
                                    shell.reset_sidebar_width(cx);
                                }
                            })),
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
                            .child(selected_text),
                    )
                    .on_drag_move(cx.listener(
                        |shell, event: &DragMoveEvent<SidebarDrag>, _, cx| {
                            shell.preview_sidebar_width(event, cx);
                        },
                    ))
                    .on_drop(cx.listener(|shell, _: &SidebarDrag, _, cx| {
                        shell.persist_sidebar_width(cx);
                    })),
            )
    }
}
