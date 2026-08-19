use gpui::{
    App, AppContext as _, Context, DragMoveEvent, Empty, Entity, FocusHandle,
    InteractiveElement as _, IntoElement, ParentElement as _, Render,
    StatefulInteractiveElement as _, Styled as _, Subscription, Task, Window, WindowAppearance,
    div, px,
};
use gpui_component::{
    ActiveTheme as _, IconName, StyledExt as _, TitleBar, WindowExt as _, button::Button,
    notification::Notification,
};
use kabu_core::{CachedMarketState, PersistedState, Quote, ThemePreference};
use std::{
    borrow::BorrowMut,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use crate::{
    app_model::{AppModel, DEFAULT_SIDEBAR_WIDTH},
    commands::{clear_theme_action_target, register_theme_action_target},
    macos_visibility::{AlwaysVisible, VisibilityProbe, VisibilityTracker},
    runtime::RuntimeHandle,
    theme::{KabuTheme, ThemeSelection, apply_theme},
    views::{QuoteDetail, SearchOverlay, SearchOverlayEvent, ThemeSwitcher, WatchlistPane},
};

type IconButton = Button;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SelectedQuoteAvailability {
    Available,
    Loading,
    Unavailable,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SelectedQuoteHeader {
    pub symbol: String,
    pub display_name: String,
    pub price: String,
    pub change: String,
    pub source: String,
    pub provider_timestamp: String,
    pub status: String,
    pub availability: SelectedQuoteAvailability,
}

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
    search_overlay: Entity<SearchOverlay>,
    search_open: bool,
    focus_search_on_render: bool,
    theme_selection: ThemeSelection,
    active_theme: KabuTheme,
    window_appearance: WindowAppearance,
    geometry_generation: u64,
    _model_subscription: Subscription,
    _appearance_subscription: Subscription,
    _bounds_subscription: Subscription,
    _action_target_subscription: Subscription,
    _search_subscription: Subscription,
    _runtime_event_forwarder: Task<()>,
    _runtime_event_task: Task<()>,
    visibility: VisibilityTracker<Box<dyn VisibilityProbe>>,
    _visibility_task: Task<()>,
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
        Self::with_model_and_visibility(model, Box::new(AlwaysVisible), window, cx)
    }

    pub fn with_model_and_visibility(
        model: Entity<AppModel>,
        probe: Box<dyn VisibilityProbe>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let theme_selection = ThemeSelection::new(model.read(cx).theme());
        let window_appearance = window.appearance();
        let active_theme = theme_selection.resolve(window_appearance);
        apply_theme(active_theme, Some(window), cx);
        let startup_notifications = model
            .read(cx)
            .warnings()
            .iter()
            .filter(|warning| warning.is_startup_notification())
            .map(|warning| warning.message.clone())
            .collect::<Vec<_>>();
        if !startup_notifications.is_empty() {
            cx.defer_in(window, move |_, window, cx| {
                for message in startup_notifications {
                    window.push_notification(Notification::warning(message), cx);
                }
            });
        }

        let model_subscription = cx.observe(&model, |shell, model, cx| {
            shell.model_changed(&model, cx);
        });
        let appearance_subscription = cx.observe_window_appearance(window, |shell, window, cx| {
            shell.window_appearance_changed(window, cx);
        });
        let bounds_subscription = cx.observe_window_bounds(window, |shell, window, cx| {
            shell.window_bounds_changed(window, cx);
        });
        let search_overlay = cx.new(|cx| SearchOverlay::new(model.clone(), window, cx));
        let search_subscription = cx.subscribe_in(
            &search_overlay,
            window,
            |shell, _, event: &SearchOverlayEvent, window, cx| match event {
                SearchOverlayEvent::Dismissed => shell.close_search(window, cx),
            },
        );
        let runtime_events = model.read(cx).runtime().events();
        let (main_event_sender, main_events) = async_channel::bounded(32);
        let runtime_event_forwarder = cx.background_spawn(async move {
            while let Ok(event) = runtime_events.recv().await {
                if main_event_sender.send(event).await.is_err() {
                    break;
                }
            }
        });
        let runtime_event_task = cx.spawn(async move |shell, cx| {
            while let Ok(event) = main_events.recv().await {
                if cx
                    .update(|cx| {
                        if let Some(shell) = shell.upgrade() {
                            shell.update(cx, |shell, cx| {
                                shell.model.update(cx, |model, cx| {
                                    model.apply_runtime_event(event);
                                    cx.notify();
                                });
                            });
                        }
                    })
                    .is_err()
                {
                    break;
                }
            }
        });
        let target_generation =
            register_theme_action_target(cx.entity().downgrade(), BorrowMut::<App>::borrow_mut(cx));
        let action_target_subscription = cx.on_release(move |_, cx| {
            clear_theme_action_target(target_generation, cx);
        });

        let mut visibility = VisibilityTracker::new(probe);
        if let Some(visible) = visibility.poll() {
            model.update(cx, |model, cx| {
                model.set_visible_at(visible, chrono::Utc::now());
                cx.notify();
            });
        }
        let visibility_timer = cx.spawn(async move |shell, cx| {
            loop {
                cx.background_executor().timer(Duration::from_secs(1)).await;
                if cx
                    .update(|cx| {
                        let Some(shell) = shell.upgrade() else {
                            return false;
                        };
                        shell.update(cx, |shell, cx| {
                            let visibility_change = shell.visibility.poll();
                            let now = chrono::Utc::now();
                            let freshness_changed = shell.model.update(cx, |model, _| {
                                if let Some(visible) = visibility_change {
                                    model.set_visible_at(visible, now);
                                }
                                model.tick_at(now)
                            });
                            if visibility_change.is_some() || freshness_changed {
                                cx.notify();
                            }
                        });
                        true
                    })
                    .is_err()
                {
                    break;
                }
            }
        });

        let close_model = model.downgrade();
        let close_started = Arc::new(AtomicBool::new(false));
        window.on_window_should_close(cx, move |window, cx| {
            let Some(model) = close_model.upgrade() else {
                return true;
            };
            let runtime = model.read(cx).runtime().clone();
            if runtime.is_shutdown_complete() {
                return true;
            }
            if close_started.swap(true, Ordering::SeqCst) {
                return false;
            }
            let size = window.window_bounds().get_bounds().size;
            model.update(cx, |model, cx| {
                model.preview_window_geometry(f32::from(size.width), f32::from(size.height));
                _ = model.persist_window_geometry();
                model.begin_flush_and_shutdown();
                cx.notify();
            });
            if runtime.is_shutdown_complete() {
                return true;
            }
            let mut async_window = window.to_async(cx);
            let shutdown_wait = cx.background_spawn(async move {
                loop {
                    if runtime.wait_for_shutdown(Duration::from_secs(2)) {
                        break;
                    }
                }
            });
            cx.spawn(async move |_| {
                shutdown_wait.await;
                _ = async_window.update(|window, _| window.remove_window());
            })
            .detach();
            false
        });

        Self {
            model,
            watchlist_focus: cx.focus_handle(),
            search_overlay,
            search_open: false,
            focus_search_on_render: false,
            theme_selection,
            active_theme,
            window_appearance,
            geometry_generation: 0,
            _model_subscription: model_subscription,
            _appearance_subscription: appearance_subscription,
            _bounds_subscription: bounds_subscription,
            _action_target_subscription: action_target_subscription,
            _search_subscription: search_subscription,
            _runtime_event_forwarder: runtime_event_forwarder,
            _runtime_event_task: runtime_event_task,
            visibility,
            _visibility_task: visibility_timer,
        }
    }

    pub fn model(&self) -> Entity<AppModel> {
        self.model.clone()
    }

    pub fn focus_watchlist(&self, window: &mut Window) {
        self.watchlist_focus.focus(window);
    }

    pub const fn search_is_open(&self) -> bool {
        self.search_open
    }

    pub fn search_input_focus_handle(&self, cx: &App) -> FocusHandle {
        self.search_overlay.read(cx).focus_handle(cx)
    }

    pub(crate) fn open_search(&mut self, cx: &mut Context<Self>) {
        self.search_open = true;
        self.focus_search_on_render = true;
        cx.notify();
    }

    fn close_search(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.search_open = false;
        self.focus_search_on_render = false;
        self.search_overlay
            .update(cx, |overlay, cx| overlay.cancel_pending_search(cx));
        self.watchlist_focus.focus(window);
        cx.notify();
    }

    pub fn selected_quote_header(&self, cx: &App) -> SelectedQuoteHeader {
        let model = self.model.read(cx);
        let Some(instrument) = model.selected() else {
            return SelectedQuoteHeader {
                symbol: String::new(),
                display_name: "No instrument selected".into(),
                price: "—".into(),
                change: "—".into(),
                source: "—".into(),
                provider_timestamp: "—".into(),
                status: "Unavailable".into(),
                availability: SelectedQuoteAvailability::Unavailable,
            };
        };

        if let Some(quote) = model.quote(&instrument.id) {
            return available_quote_header(
                instrument,
                quote,
                model.freshness(&instrument.id),
                model.quote_error_message(&instrument.id),
            );
        }

        let loading = model.quote_is_loading(&instrument.id);
        SelectedQuoteHeader {
            symbol: instrument.id.canonical_symbol(),
            display_name: instrument.display_name.clone(),
            price: "—".into(),
            change: "—".into(),
            source: if loading { "Loading" } else { "Unavailable" }.into(),
            provider_timestamp: "—".into(),
            status: if loading {
                "Loading quote"
            } else {
                "No cached quote"
            }
            .into(),
            availability: if loading {
                SelectedQuoteAvailability::Loading
            } else {
                SelectedQuoteAvailability::Unavailable
            },
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

    fn title_bar(&self, cx: &mut Context<Self>) -> impl IntoElement {
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
                        .on_click(cx.listener(|shell, _, _, cx| shell.open_search(cx))),
                )
                .child(ThemeSwitcher::new(self.theme_preference())),
        )
    }
}

impl Render for AppShell {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if self.search_open && self.focus_search_on_render {
            self.search_overlay
                .update(cx, |overlay, cx| overlay.focus(window, cx));
            self.focus_search_on_render = false;
        }
        let sidebar_width = self.model.read(cx).sidebar_width();
        let warnings = self
            .model
            .read(cx)
            .warnings()
            .iter()
            .filter(|warning| !warning.is_startup_notification())
            .map(|warning| warning.message.clone())
            .collect::<Vec<_>>();

        div()
            .flex()
            .flex_col()
            .size_full()
            .bg(cx.theme().background)
            .text_color(cx.theme().foreground)
            .child(self.title_bar(cx))
            .children(warnings.into_iter().map(|message| {
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
                            .flex_col()
                            .flex_1()
                            .h_full()
                            .bg(cx.theme().background)
                            .p_6()
                            .child(QuoteDetail::new(self.model.clone())),
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
            .children(self.search_open.then(|| self.search_overlay.clone()))
    }
}

fn available_quote_header(
    instrument: &kabu_core::Instrument,
    quote: &Quote,
    freshness: kabu_core::Freshness,
    error: Option<&str>,
) -> SelectedQuoteHeader {
    SelectedQuoteHeader {
        symbol: instrument.id.canonical_symbol(),
        display_name: instrument.display_name.clone(),
        price: format!("{:.2} {}", quote.last, quote.currency),
        change: format_quote_change(quote),
        source: format!("{:?}", quote.source),
        provider_timestamp: quote.provider_timestamp.to_rfc3339(),
        status: error
            .map(str::to_string)
            .unwrap_or_else(|| format!("{freshness:?}")),
        availability: SelectedQuoteAvailability::Available,
    }
}

fn format_quote_change(quote: &Quote) -> String {
    match (quote.absolute_change(), quote.percent_change()) {
        (Some(absolute), Some(percent)) => {
            format!("{} ({})", signed_value(absolute), signed_percent(percent))
        }
        (Some(absolute), None) => signed_value(absolute),
        (None, _) => "—".into(),
    }
}

fn signed_value(value: f64) -> String {
    if value > 0.0 {
        format!("+{value:.2}")
    } else if value < 0.0 {
        format!("−{:.2}", value.abs())
    } else {
        "0.00".into()
    }
}

fn signed_percent(value: f64) -> String {
    if value > 0.0 {
        format!("+{value:.2}%")
    } else if value < 0.0 {
        format!("−{:.2}%", value.abs())
    } else {
        "0.00%".into()
    }
}
