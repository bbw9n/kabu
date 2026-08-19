use crate::{
    app_model::AppModel,
    commands::{CloseSearch, SelectNextSearchResult, SelectPreviousSearchResult},
};
use gpui::prelude::FluentBuilder;
use gpui::{
    App, AppContext as _, Context, Entity, EventEmitter, FocusHandle, Focusable,
    InteractiveElement, IntoElement, ParentElement, Render, Styled, Subscription, Task, Window,
    div, px, size,
};
use gpui_component::{
    ActiveTheme as _, StyledExt as _,
    input::{Input, InputEvent, InputState},
    list::ListItem,
    skeleton::Skeleton,
    v_virtual_list,
};
use std::{ops::Range, rc::Rc, time::Duration};

pub const SEARCH_OVERLAY_KEY_CONTEXT: &str = "SearchOverlay";
const SEARCH_DEBOUNCE: Duration = Duration::from_millis(250);
const SEARCH_ROW_HEIGHT: f32 = 58.0;

pub enum SearchOverlayEvent {
    Dismissed,
}

pub struct SearchOverlay {
    model: Entity<AppModel>,
    input: Entity<InputState>,
    selected_index: usize,
    search_debounce_task: Task<()>,
    _input_subscription: Subscription,
}

impl EventEmitter<SearchOverlayEvent> for SearchOverlay {}

impl SearchOverlay {
    pub fn new(model: Entity<AppModel>, window: &mut Window, cx: &mut Context<Self>) -> Self {
        let input =
            cx.new(|cx| InputState::new(window, cx).placeholder("Search stocks and crypto"));
        let input_subscription = cx.subscribe_in(
            &input,
            window,
            |overlay, input, event: &InputEvent, window, cx| match event {
                InputEvent::Change => overlay.input_changed(input, cx),
                InputEvent::PressEnter { .. } => overlay.add_selected(window, cx),
                InputEvent::Focus | InputEvent::Blur => {}
            },
        );
        Self {
            model,
            input,
            selected_index: 0,
            search_debounce_task: Task::ready(()),
            _input_subscription: input_subscription,
        }
    }

    pub fn focus(&self, window: &mut Window, cx: &mut Context<Self>) {
        self.input.update(cx, |input, cx| input.focus(window, cx));
    }

    pub fn focus_handle(&self, cx: &App) -> FocusHandle {
        self.input.read(cx).focus_handle(cx)
    }

    fn input_changed(&mut self, input: &Entity<InputState>, cx: &mut Context<Self>) {
        self.search_debounce_task = Task::ready(());
        let query = input.read(cx).value();
        let generation = self
            .model
            .update(cx, |model, _| model.set_search_query(query.as_str()));
        self.selected_index = 0;
        if query.trim().is_empty() {
            cx.notify();
            return;
        }
        let debounce = cx.background_executor().timer(SEARCH_DEBOUNCE);
        self.search_debounce_task = cx.spawn(async move |overlay, cx| {
            debounce.await;
            _ = cx.update(|cx| {
                if let Some(overlay) = overlay.upgrade() {
                    overlay.update(cx, |overlay, cx| {
                        overlay.model.update(cx, |model, cx| {
                            model.dispatch_pending_search(generation);
                            cx.notify();
                        });
                    });
                }
            });
        });
        cx.notify();
    }

    pub fn cancel_pending_search(&mut self, cx: &mut Context<Self>) {
        self.search_debounce_task = Task::ready(());
        self.model.update(cx, |model, cx| {
            model.cancel_pending_search();
            cx.notify();
        });
    }

    fn select_previous(&mut self, cx: &mut Context<Self>) {
        self.selected_index = self.selected_index.saturating_sub(1);
        cx.notify();
    }

    fn select_next(&mut self, cx: &mut Context<Self>) {
        let result_count = self.model.read(cx).search().results().len();
        if result_count > 0 {
            self.selected_index = (self.selected_index + 1).min(result_count - 1);
        }
        cx.notify();
    }

    fn add_selected(&mut self, _: &mut Window, cx: &mut Context<Self>) {
        let has_result = self
            .model
            .read(cx)
            .search()
            .results()
            .get(self.selected_index)
            .is_some();
        _ = self
            .model
            .update(cx, |model, _| model.add_search_result(self.selected_index));
        if has_result {
            cx.emit(SearchOverlayEvent::Dismissed);
        }
    }

    fn render_rows(
        &mut self,
        visible_range: Range<usize>,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) -> Vec<ListItem> {
        let results = self.model.read(cx).search().results().to_vec();
        visible_range
            .filter_map(|index| {
                let result = results.get(index)?.clone();
                let symbol = result.instrument.id.canonical_symbol();
                let display_name = result.instrument.display_name;
                let venue = result.venue_label;
                Some(
                    ListItem::new(("search-result", index))
                        .selected(self.selected_index == index)
                        .py_2()
                        .child(
                            div()
                                .debug_selector(move || format!("search-result-{index}"))
                                .flex()
                                .w_full()
                                .items_center()
                                .justify_between()
                                .gap_4()
                                .child(
                                    div()
                                        .min_w_0()
                                        .flex()
                                        .flex_col()
                                        .child(div().font_semibold().child(symbol))
                                        .child(
                                            div()
                                                .truncate()
                                                .text_sm()
                                                .text_color(cx.theme().muted_foreground)
                                                .child(display_name),
                                        ),
                                )
                                .child(
                                    div()
                                        .text_xs()
                                        .text_color(cx.theme().muted_foreground)
                                        .child(venue),
                                ),
                        )
                        .on_click(cx.listener(move |overlay, _, window, cx| {
                            overlay.selected_index = index;
                            overlay.add_selected(window, cx);
                        })),
                )
            })
            .collect()
    }
}

impl Render for SearchOverlay {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let search = self.model.read(cx).search().clone();
        if !search.results().is_empty() {
            self.selected_index = self.selected_index.min(search.results().len() - 1);
        } else {
            self.selected_index = 0;
        }
        let item_sizes = Rc::new(vec![
            size(px(0.0), px(SEARCH_ROW_HEIGHT));
            search.results().len()
        ]);
        let warnings = search.provider_warnings().to_vec();
        let terminal_error = search.terminal_error().map(str::to_string);
        let has_terminal_error = terminal_error.is_some();

        div()
            .id("search-overlay")
            .debug_selector(|| "search-overlay".into())
            .key_context(SEARCH_OVERLAY_KEY_CONTEXT)
            .absolute()
            .inset_0()
            .flex()
            .items_start()
            .justify_center()
            .pt_16()
            .bg(cx.theme().overlay.opacity(0.58))
            .on_action(cx.listener(|_, _: &CloseSearch, _, cx| {
                cx.emit(SearchOverlayEvent::Dismissed);
            }))
            .on_action(
                cx.listener(|overlay, _: &SelectPreviousSearchResult, _, cx| {
                    overlay.select_previous(cx);
                }),
            )
            .on_action(cx.listener(|overlay, _: &SelectNextSearchResult, _, cx| {
                overlay.select_next(cx);
            }))
            .child(
                div()
                    .w(px(620.0))
                    .max_h(px(520.0))
                    .flex()
                    .flex_col()
                    .overflow_hidden()
                    .rounded_lg()
                    .border_1()
                    .border_color(cx.theme().border)
                    .bg(cx.theme().popover)
                    .shadow_lg()
                    .p_3()
                    .gap_2()
                    .child(Input::new(&self.input))
                    .children(warnings.into_iter().map(|warning| {
                        div()
                            .rounded_md()
                            .bg(cx.theme().warning.opacity(0.12))
                            .px_3()
                            .py_2()
                            .text_sm()
                            .text_color(cx.theme().warning)
                            .child(warning.message)
                    }))
                    .children(terminal_error.map(|message| {
                        div()
                            .rounded_md()
                            .bg(cx.theme().danger.opacity(0.12))
                            .px_3()
                            .py_2()
                            .text_sm()
                            .text_color(cx.theme().danger)
                            .child(message)
                    }))
                    .when(search.is_loading(), |this| {
                        this.child(div().flex().flex_col().gap_2().children(
                            (0..3).map(|_| Skeleton::new().h(px(SEARCH_ROW_HEIGHT)).w_full()),
                        ))
                    })
                    .when(
                        !search.is_loading()
                            && !search.query().is_empty()
                            && search.results().is_empty()
                            && !has_terminal_error,
                        |this| {
                            this.child(
                                div()
                                    .px_3()
                                    .py_6()
                                    .text_center()
                                    .text_color(cx.theme().muted_foreground)
                                    .child("No matching instruments."),
                            )
                        },
                    )
                    .when(!search.results().is_empty(), |this| {
                        this.child(
                            v_virtual_list(
                                cx.entity(),
                                "search-results",
                                item_sizes,
                                |overlay, range, window, cx| overlay.render_rows(range, window, cx),
                            )
                            .h(px(350.0)),
                        )
                    })
                    .child(
                        div()
                            .border_t_1()
                            .border_color(cx.theme().border)
                            .pt_2()
                            .text_xs()
                            .text_color(cx.theme().muted_foreground)
                            .child("↑↓ Navigate · Return Add · Esc Close"),
                    ),
            )
    }
}
