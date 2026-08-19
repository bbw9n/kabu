use crate::{
    app_model::AppModel,
    commands::{RemoveSelectedInstrument, SelectNextWatchlist, SelectPreviousWatchlist},
};
use gpui::{
    App, Entity, FocusHandle, FontFeatures, InteractiveElement as _, IntoElement,
    ParentElement as _, RenderOnce, SharedString, StatefulInteractiveElement as _, Styled as _,
    Window, div, font, px,
};
use gpui_component::{ActiveTheme as _, StyledExt as _, list::ListItem, skeleton::Skeleton};
use kabu_core::{Instrument, Quote};
use std::sync::Arc;

pub const WATCHLIST_KEY_CONTEXT: &str = "Watchlist";

#[derive(IntoElement)]
pub struct WatchlistPane {
    model: Entity<AppModel>,
    focus_handle: FocusHandle,
}

impl WatchlistPane {
    pub fn new(model: Entity<AppModel>, focus_handle: FocusHandle) -> Self {
        Self {
            model,
            focus_handle,
        }
    }

    fn mutate_model(model: &Entity<AppModel>, cx: &mut App, mutation: impl FnOnce(&mut AppModel)) {
        model.update(cx, |model, cx| {
            mutation(model);
            cx.notify();
        });
    }
}

impl RenderOnce for WatchlistPane {
    fn render(self, _: &mut Window, cx: &mut App) -> impl IntoElement {
        let selected_index = self.model.read(cx).watchlist().selected_index();
        let rows = self
            .model
            .read(cx)
            .watchlist()
            .instruments()
            .iter()
            .cloned()
            .enumerate()
            .map(|(index, instrument)| {
                let quote = self.model.read(cx).quote(&instrument.id).cloned();
                let loading = self.model.read(cx).quote_is_loading(&instrument.id);
                render_row(
                    RowSnapshot {
                        index,
                        instrument,
                        quote,
                        loading,
                        selected: selected_index == Some(index),
                    },
                    self.model.clone(),
                    self.focus_handle.clone(),
                    cx,
                )
            })
            .collect::<Vec<_>>();

        let next_model = self.model.clone();
        let previous_model = self.model.clone();
        let remove_model = self.model.clone();

        div()
            .id("watchlist-pane")
            .key_context(WATCHLIST_KEY_CONTEXT)
            .track_focus(&self.focus_handle)
            .focusable()
            .flex()
            .flex_col()
            .size_full()
            .child(
                div()
                    .flex()
                    .items_center()
                    .justify_between()
                    .px_3()
                    .py_2()
                    .child(div().font_semibold().child("Watchlist"))
                    .child(
                        div()
                            .text_xs()
                            .text_color(cx.theme().muted_foreground)
                            .child(format!("{} symbols", rows.len())),
                    ),
            )
            .child(
                div()
                    .id("watchlist-scroll")
                    .flex()
                    .flex_col()
                    .flex_1()
                    .min_h_0()
                    .overflow_y_scroll()
                    .px_1()
                    .children(rows),
            )
            .on_action(move |_: &SelectNextWatchlist, _, cx| {
                Self::mutate_model(&next_model, cx, |model| {
                    _ = model.select_next();
                });
            })
            .on_action(move |_: &SelectPreviousWatchlist, _, cx| {
                Self::mutate_model(&previous_model, cx, |model| {
                    _ = model.select_previous();
                });
            })
            .on_action(move |_: &RemoveSelectedInstrument, _, cx| {
                Self::mutate_model(&remove_model, cx, |model| {
                    _ = model.remove_selected();
                });
            })
    }
}

struct RowSnapshot {
    index: usize,
    instrument: Instrument,
    quote: Option<Quote>,
    loading: bool,
    selected: bool,
}

fn render_row(
    row: RowSnapshot,
    model: Entity<AppModel>,
    focus_handle: FocusHandle,
    cx: &App,
) -> ListItem {
    let RowSnapshot {
        index,
        instrument,
        quote,
        loading,
        selected,
    } = row;
    let symbol = instrument.id.canonical_symbol();
    let (price, change, status, change_color) = quote
        .as_ref()
        .map(|quote| quote_text(quote, cx))
        .unwrap_or_else(|| {
            (
                SharedString::from("—"),
                SharedString::from("Unavailable"),
                SharedString::from("No cached quote"),
                cx.theme().muted_foreground,
            )
        });

    let value = if loading {
        compact_quote_skeleton().into_any_element()
    } else {
        div()
            .flex()
            .flex_col()
            .items_end()
            .font(tabular_font())
            .child(div().font_medium().child(price))
            .child(div().text_xs().text_color(change_color).child(change))
            .child(
                div()
                    .text_xs()
                    .text_color(cx.theme().muted_foreground)
                    .child(status),
            )
            .into_any_element()
    };

    ListItem::new(("watchlist-row", index))
        .selected(selected)
        .py_2()
        .child(
            div()
                .debug_selector(move || format!("watchlist-row-{index}"))
                .flex()
                .w_full()
                .items_center()
                .justify_between()
                .gap_3()
                .child(
                    div()
                        .flex()
                        .min_w_0()
                        .flex_col()
                        .child(div().font_semibold().child(symbol))
                        .child(
                            div()
                                .truncate()
                                .text_xs()
                                .text_color(cx.theme().muted_foreground)
                                .child(instrument.display_name),
                        ),
                )
                .child(value),
        )
        .on_click(move |_, window, cx| {
            focus_handle.focus(window);
            model.update(cx, |model, cx| {
                _ = model.select_index(index);
                cx.notify();
            });
        })
}

fn quote_text(quote: &Quote, cx: &App) -> (SharedString, SharedString, SharedString, gpui::Hsla) {
    let price = format!("{:.2}", quote.last);
    let (change, color) = match quote.percent_change() {
        Some(value) if value > 0.0 => (format!("+{value:.2}%"), cx.theme().success),
        Some(value) if value < 0.0 => (format!("−{:.2}%", value.abs()), cx.theme().danger),
        Some(_) => ("0.00%".into(), cx.theme().muted_foreground),
        None => ("—".into(), cx.theme().muted_foreground),
    };
    (
        price.into(),
        change.into(),
        format!("{:?}", quote.market_state).into(),
        color,
    )
}

fn tabular_font() -> gpui::Font {
    let mut value = font(".SystemUIFont");
    value.features = FontFeatures(Arc::new(vec![("tnum".into(), 1)]));
    value
}

fn compact_quote_skeleton() -> impl IntoElement {
    div()
        .flex()
        .flex_col()
        .items_end()
        .gap_1()
        .child(Skeleton::new().h(px(16.0)).w(px(64.0)))
        .child(Skeleton::new().secondary().h(px(12.0)).w(px(44.0)))
}
