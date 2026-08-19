use crate::{
    app_model::{AppModel, ChartAvailability},
    views::{PriceChart, normalize_closes},
};
use gpui::{
    App, Entity, FontFeatures, InteractiveElement as _, IntoElement, ParentElement as _,
    RenderOnce, StatefulInteractiveElement as _, Styled as _, Window, div, font, px,
};
use gpui_component::{
    ActiveTheme as _, Selectable as _, StyledExt as _, button::Button, skeleton::Skeleton,
};
use kabu_core::{
    ChartPeriod, DataSource, Freshness, Instrument, InstrumentId, Quote, classify_freshness,
};
use std::{sync::Arc, time::Duration};

const MISSING_VALUE: &str = "—";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QuoteDetailData {
    pub symbol: String,
    pub display_name: String,
    pub asset_class: String,
    pub venue: String,
    pub price: String,
    pub change: String,
    pub open: String,
    pub high: String,
    pub low: String,
    pub previous_close: String,
    pub volume: String,
    pub source: String,
    pub provider_timestamp: String,
    pub provider_age: String,
    pub freshness: Freshness,
    pub freshness_label: String,
}

impl QuoteDetailData {
    pub fn from_quote(
        instrument: &Instrument,
        quote: Option<&Quote>,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Self {
        let (asset_class, venue) = instrument_labels(instrument);
        let Some(quote) = quote else {
            return Self {
                symbol: instrument.id.canonical_symbol(),
                display_name: instrument.display_name.clone(),
                asset_class,
                venue,
                price: MISSING_VALUE.into(),
                change: MISSING_VALUE.into(),
                open: MISSING_VALUE.into(),
                high: MISSING_VALUE.into(),
                low: MISSING_VALUE.into(),
                previous_close: MISSING_VALUE.into(),
                volume: MISSING_VALUE.into(),
                source: MISSING_VALUE.into(),
                provider_timestamp: MISSING_VALUE.into(),
                provider_age: MISSING_VALUE.into(),
                freshness: Freshness::Unavailable,
                freshness_label: "Unavailable".into(),
            };
        };

        let age = quote_age(quote, now);
        let freshness = classify_freshness(quote, now);
        Self {
            symbol: instrument.id.canonical_symbol(),
            display_name: instrument.display_name.clone(),
            asset_class,
            venue,
            price: format_price(Some(quote.last), &quote.currency),
            change: format_quote_change(quote),
            open: format_metric(quote.open),
            high: format_metric(quote.high),
            low: format_metric(quote.low),
            previous_close: format_metric(quote.previous_close),
            volume: format_volume(quote.volume),
            source: source_label(quote.source).into(),
            provider_timestamp: quote.provider_timestamp.to_rfc3339(),
            provider_age: format!("{} ago", format_age(age)),
            freshness,
            freshness_label: freshness_label(freshness, age),
        }
    }
}

pub fn freshness_label(freshness: Freshness, age: Duration) -> String {
    match freshness {
        Freshness::Live => "Live".into(),
        Freshness::Fresh => "Fresh".into(),
        Freshness::Aging => format!("Aging · {}", format_age(age)),
        Freshness::Delayed => "Delayed".into(),
        Freshness::Stale => "Stale".into(),
        Freshness::Closed => "Closed".into(),
        Freshness::Unavailable => "Unavailable".into(),
    }
}

fn format_age(age: Duration) -> String {
    format!("{}s", age.as_secs())
}

fn quote_age(quote: &Quote, now: chrono::DateTime<chrono::Utc>) -> Duration {
    let provider_age = now
        .signed_duration_since(quote.provider_timestamp)
        .to_std()
        .unwrap_or_default();
    let receipt_age = now
        .signed_duration_since(quote.received_at)
        .to_std()
        .unwrap_or_default();
    provider_age.max(receipt_age)
}

fn instrument_labels(instrument: &Instrument) -> (String, String) {
    match &instrument.id {
        InstrumentId::Equity { venue, .. } => (
            "US Equity".into(),
            venue.clone().unwrap_or_else(|| "US Market".into()),
        ),
        InstrumentId::Crypto { .. } => ("Crypto".into(), "Binance Spot".into()),
    }
}

fn source_label(source: DataSource) -> &'static str {
    match source {
        DataSource::Yahoo => "Yahoo",
        DataSource::Binance => "Binance",
    }
}

fn format_price(value: Option<f64>, currency: &str) -> String {
    value.filter(|value| value.is_finite()).map_or_else(
        || MISSING_VALUE.into(),
        |value| format!("{value:.2} {currency}"),
    )
}

fn format_metric(value: Option<f64>) -> String {
    value
        .filter(|value| value.is_finite())
        .map_or_else(|| MISSING_VALUE.into(), |value| format!("{value:.2}"))
}

fn format_volume(value: Option<f64>) -> String {
    value
        .filter(|value| value.is_finite())
        .map_or_else(|| MISSING_VALUE.into(), |value| format!("{value:.0}"))
}

fn format_quote_change(quote: &Quote) -> String {
    match (quote.absolute_change(), quote.percent_change()) {
        (Some(absolute), Some(percent)) if absolute.is_finite() && percent.is_finite() => {
            format!("{} ({})", signed_value(absolute), signed_percent(percent))
        }
        (Some(absolute), None) if absolute.is_finite() => signed_value(absolute),
        _ => MISSING_VALUE.into(),
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

#[derive(IntoElement)]
pub struct QuoteDetail {
    model: Entity<AppModel>,
    now: chrono::DateTime<chrono::Utc>,
}

impl QuoteDetail {
    pub fn new(model: Entity<AppModel>) -> Self {
        Self::at(model, chrono::Utc::now())
    }

    pub fn at(model: Entity<AppModel>, now: chrono::DateTime<chrono::Utc>) -> Self {
        Self { model, now }
    }
}

impl RenderOnce for QuoteDetail {
    fn render(self, _: &mut Window, cx: &mut App) -> impl IntoElement {
        let model = self.model.read(cx);
        let Some(instrument) = model.selected().cloned() else {
            return div()
                .debug_selector(|| "quote-detail".into())
                .flex()
                .flex_1()
                .items_center()
                .justify_center()
                .child("No instrument selected")
                .into_any_element();
        };
        let quote = model.quote(&instrument.id).cloned();
        let quote_loading = model.quote_is_loading(&instrument.id);
        let quote_error = model
            .quote_error_message(&instrument.id)
            .map(str::to_string);
        let history_error = model.history_error_message().map(str::to_string);
        let stream_status = model.stream_status();
        let retry_error = match (quote_error, history_error) {
            (Some(message), _) => Some((message, false)),
            (None, Some(message)) => Some((message, true)),
            (None, None) => None,
        };
        let data = QuoteDetailData::from_quote(&instrument, quote.as_ref(), self.now);
        let period = model.selected_chart_period();
        let chart_availability = model.selected_chart_availability();
        let chart = model.selected_chart().cloned();

        let change_color = if data.change.starts_with('+') {
            cx.theme().success
        } else if data.change.starts_with('−') {
            cx.theme().danger
        } else {
            cx.theme().muted_foreground
        };
        let freshness_color = match data.freshness {
            Freshness::Live | Freshness::Fresh | Freshness::Closed => cx.theme().success,
            Freshness::Aging | Freshness::Delayed => cx.theme().warning,
            Freshness::Stale | Freshness::Unavailable => cx.theme().danger,
        };

        div()
            .id("quote-detail-scroll")
            .debug_selector(|| "quote-detail".into())
            .flex()
            .flex_col()
            .size_full()
            .min_h_0()
            .overflow_y_scroll()
            .gap_4()
            .child(
                div()
                    .flex()
                    .items_end()
                    .justify_between()
                    .gap_4()
                    .child(
                        div()
                            .flex()
                            .flex_col()
                            .gap_1()
                            .child(
                                div()
                                    .debug_selector(|| "quote-detail-symbol".into())
                                    .text_2xl()
                                    .font_semibold()
                                    .child(data.symbol.clone()),
                            )
                            .child(
                                div()
                                    .text_sm()
                                    .text_color(cx.theme().muted_foreground)
                                    .child(data.display_name.clone()),
                            )
                            .child(
                                div()
                                    .text_xs()
                                    .text_color(cx.theme().muted_foreground)
                                    .child(format!("{} · {}", data.asset_class, data.venue)),
                            ),
                    )
                    .child(if quote_loading && quote.is_none() {
                        div()
                            .debug_selector(|| "quote-loading-skeleton".into())
                            .flex()
                            .flex_col()
                            .items_end()
                            .gap_1()
                            .child(Skeleton::new().h(px(28.0)).w(px(128.0)))
                            .child(Skeleton::new().secondary().h(px(14.0)).w(px(92.0)))
                            .into_any_element()
                    } else {
                        div()
                            .flex()
                            .flex_col()
                            .items_end()
                            .font(tabular_font())
                            .child(
                                div()
                                    .debug_selector(|| "quote-detail-price".into())
                                    .text_2xl()
                                    .font_semibold()
                                    .child(data.price.clone()),
                            )
                            .child(
                                div()
                                    .text_sm()
                                    .text_color(change_color)
                                    .child(data.change.clone()),
                            )
                            .children(quote_loading.then(|| {
                                div()
                                    .debug_selector(|| "quote-refreshing-indicator".into())
                                    .text_xs()
                                    .text_color(cx.theme().muted_foreground)
                                    .child("Refreshing…")
                            }))
                            .into_any_element()
                    }),
            )
            .child(period_tabs(self.model.clone(), period))
            .child(render_chart(chart_availability, chart, cx))
            .children(retry_error.map(|(message, retry_history)| {
                inline_retry(self.model.clone(), message, retry_history, cx)
            }))
            .child(div().flex().flex_wrap().gap_2().children([
                metric_card("Open", data.open.clone(), "quote-metric-open", cx),
                metric_card("High", data.high.clone(), "quote-metric-high", cx),
                metric_card("Low", data.low.clone(), "quote-metric-low", cx),
                metric_card(
                    "Previous Close",
                    data.previous_close.clone(),
                    "quote-metric-previous-close",
                    cx,
                ),
                metric_card("Volume", data.volume.clone(), "quote-metric-volume", cx),
            ]))
            .child(
                div()
                    .flex()
                    .items_center()
                    .justify_between()
                    .border_t_1()
                    .border_color(cx.theme().border)
                    .pt_3()
                    .text_xs()
                    .text_color(cx.theme().muted_foreground)
                    .child(
                        div()
                            .debug_selector(|| "quote-freshness".into())
                            .flex()
                            .items_center()
                            .gap_1()
                            .text_color(freshness_color)
                            .child("●")
                            .child(data.freshness_label),
                    )
                    .child(format!(
                        "{} · {} · {} · {}",
                        data.source,
                        source_status(&instrument.id, stream_status),
                        data.provider_timestamp,
                        data.provider_age
                    )),
            )
            .into_any_element()
    }
}

fn inline_retry(
    model: Entity<AppModel>,
    message: String,
    retry_history: bool,
    cx: &App,
) -> gpui::AnyElement {
    div()
        .debug_selector(|| "quote-retry".into())
        .flex()
        .items_center()
        .justify_between()
        .gap_3()
        .rounded_lg()
        .bg(cx.theme().warning.opacity(0.12))
        .px_3()
        .py_2()
        .text_sm()
        .text_color(cx.theme().warning)
        .child(div().flex_1().child(message))
        .child(
            div().debug_selector(|| "quote-retry-button".into()).child(
                Button::new("retry-market-data")
                    .label("Retry")
                    .compact()
                    .on_click(move |_, _, cx| {
                        model.update(cx, |model, cx| {
                            if retry_history {
                                model.retry_selected_history();
                            } else {
                                model.retry_selected_quote();
                            }
                            cx.notify();
                        });
                    }),
            ),
        )
        .into_any_element()
}

fn source_status(
    instrument_id: &InstrumentId,
    stream_status: crate::runtime::StreamStatus,
) -> &'static str {
    match instrument_id {
        InstrumentId::Equity { .. } => "Snapshot",
        InstrumentId::Crypto { .. } => match stream_status {
            crate::runtime::StreamStatus::Connecting => "Connecting",
            crate::runtime::StreamStatus::Live => "Live",
            crate::runtime::StreamStatus::Reconnecting => "Reconnecting",
            crate::runtime::StreamStatus::Paused => "Paused",
            crate::runtime::StreamStatus::Stopped => "Stopped",
        },
    }
}

fn period_tabs(model: Entity<AppModel>, selected: ChartPeriod) -> gpui::AnyElement {
    div()
        .flex()
        .gap_1()
        .children([
            period_tab(
                model.clone(),
                ChartPeriod::OneDay,
                "1D",
                "quote-period-1d",
                selected,
            ),
            period_tab(
                model.clone(),
                ChartPeriod::FiveDays,
                "5D",
                "quote-period-5d",
                selected,
            ),
            period_tab(
                model.clone(),
                ChartPeriod::OneMonth,
                "1M",
                "quote-period-1m",
                selected,
            ),
            period_tab(
                model.clone(),
                ChartPeriod::SixMonths,
                "6M",
                "quote-period-6m",
                selected,
            ),
            period_tab(
                model,
                ChartPeriod::OneYear,
                "1Y",
                "quote-period-1y",
                selected,
            ),
        ])
        .into_any_element()
}

fn period_tab(
    model: Entity<AppModel>,
    period: ChartPeriod,
    label: &'static str,
    selector: &'static str,
    selected: ChartPeriod,
) -> gpui::AnyElement {
    div()
        .debug_selector(move || selector.into())
        .child(
            Button::new(selector)
                .label(label)
                .compact()
                .selected(period == selected)
                .on_click(move |_, _, cx| {
                    model.update(cx, |model, cx| {
                        model.select_period(period);
                        cx.notify();
                    });
                }),
        )
        .into_any_element()
}

fn render_chart(
    availability: ChartAvailability,
    chart: Option<kabu_core::CachedChart>,
    cx: &App,
) -> gpui::AnyElement {
    match availability {
        ChartAvailability::Loading => div()
            .debug_selector(|| "chart-loading".into())
            .flex()
            .flex_1()
            .min_h(px(160.0))
            .child(Skeleton::new().size_full())
            .into_any_element(),
        ChartAvailability::Ready => {
            let Some(chart) = chart else {
                return unavailable_chart(cx);
            };
            if normalize_closes(&chart.candles).is_empty() {
                return unavailable_chart(cx);
            }
            let (start_label, end_label) = chart_labels(&chart.candles);
            PriceChart::new(chart.candles, start_label, end_label).into_any_element()
        }
        ChartAvailability::Unavailable => unavailable_chart(cx),
    }
}

fn unavailable_chart(cx: &App) -> gpui::AnyElement {
    div()
        .debug_selector(|| "chart-unavailable".into())
        .flex()
        .flex_1()
        .min_h(px(160.0))
        .items_center()
        .justify_center()
        .rounded_lg()
        .border_1()
        .border_color(cx.theme().border)
        .text_sm()
        .text_color(cx.theme().muted_foreground)
        .child("Chart data unavailable")
        .into_any_element()
}

fn chart_labels(candles: &[kabu_core::Candle]) -> (String, String) {
    let mut finite = candles.iter().filter(|candle| candle.close.is_finite());
    let Some(first) = finite.next() else {
        return (String::new(), String::new());
    };
    let last = finite.next_back().unwrap_or(first);
    (
        first.starts_at.format("%b %-d, %H:%M").to_string(),
        last.starts_at.format("%b %-d, %H:%M").to_string(),
    )
}

fn metric_card(
    label: &'static str,
    value: String,
    selector: &'static str,
    cx: &App,
) -> gpui::AnyElement {
    div()
        .debug_selector(move || selector.into())
        .flex()
        .flex_col()
        .flex_1()
        .min_w(px(88.0))
        .gap_1()
        .rounded_lg()
        .border_1()
        .border_color(cx.theme().border)
        .p_3()
        .child(
            div()
                .text_xs()
                .text_color(cx.theme().muted_foreground)
                .child(label),
        )
        .child(div().font(tabular_font()).font_semibold().child(value))
        .into_any_element()
}

fn tabular_font() -> gpui::Font {
    let mut value = font(".SystemUIFont");
    value.features = FontFeatures(Arc::new(vec![("tnum".into(), 1)]));
    value
}
