use gpui::{
    App, InteractiveElement as _, IntoElement, ParentElement as _, PathBuilder, RenderOnce,
    SharedString, Styled as _, Window, canvas, div, point, px,
};
use gpui_component::ActiveTheme as _;
use kabu_core::Candle;

pub const CHART_VERTICAL_PADDING: f32 = 0.08;
pub const CHART_STROKE_WIDTH: f32 = 2.0;
pub const MIN_CHART_CONTAINER_HEIGHT: f32 = 200.0;
const GUIDE_POSITIONS: [f32; 3] = [0.25, 0.5, 0.75];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChartDirection {
    Gain,
    Loss,
}

pub fn normalize_closes(candles: &[Candle]) -> Vec<(f32, f32)> {
    let closes = candles
        .iter()
        .map(|candle| candle.close)
        .filter(|close| close.is_finite())
        .collect::<Vec<_>>();
    match closes.len() {
        0 => return Vec::new(),
        1 => return vec![(0.5, 0.5)],
        _ => {}
    }

    let minimum = closes.iter().copied().fold(f64::INFINITY, f64::min);
    let maximum = closes.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    let last_index = (closes.len() - 1) as f32;
    if minimum == maximum {
        return closes
            .iter()
            .enumerate()
            .map(|(index, _)| (index as f32 / last_index, 0.5))
            .collect();
    }

    let scale = minimum.abs().max(maximum.abs());
    let scaled_minimum = minimum / scale;
    let scaled_range = maximum / scale - scaled_minimum;
    let drawable_height = 1.0 - CHART_VERTICAL_PADDING * 2.0;
    closes
        .iter()
        .enumerate()
        .map(|(index, close)| {
            let vertical_fraction =
                ((*close / scale - scaled_minimum) / scaled_range).clamp(0.0, 1.0) as f32;
            (
                index as f32 / last_index,
                CHART_VERTICAL_PADDING + (1.0 - vertical_fraction) * drawable_height,
            )
        })
        .collect()
}

pub fn series_direction(candles: &[Candle]) -> Option<ChartDirection> {
    let mut closes = candles
        .iter()
        .map(|candle| candle.close)
        .filter(|close| close.is_finite());
    let first = closes.next()?;
    let last = closes.next_back().unwrap_or(first);
    Some(if last - first >= 0.0 {
        ChartDirection::Gain
    } else {
        ChartDirection::Loss
    })
}

#[derive(IntoElement)]
pub struct PriceChart {
    candles: Vec<Candle>,
    start_label: SharedString,
    end_label: SharedString,
}

impl PriceChart {
    pub fn new(
        candles: Vec<Candle>,
        start_label: impl Into<SharedString>,
        end_label: impl Into<SharedString>,
    ) -> Self {
        Self {
            candles,
            start_label: start_label.into(),
            end_label: end_label.into(),
        }
    }
}

impl RenderOnce for PriceChart {
    fn render(self, _: &mut Window, cx: &mut App) -> impl IntoElement {
        let points = normalize_closes(&self.candles);
        let direction = series_direction(&self.candles).unwrap_or(ChartDirection::Gain);
        let signal_color = match direction {
            ChartDirection::Gain => cx.theme().success,
            ChartDirection::Loss => cx.theme().danger,
        };
        let guide_color = cx.theme().border.opacity(0.7);

        div()
            .debug_selector(|| "price-chart".into())
            .flex()
            .flex_col()
            .flex_1()
            .min_h(px(MIN_CHART_CONTAINER_HEIGHT))
            .child(
                canvas(
                    |_, _, _| (),
                    move |bounds, (), window, _| {
                        for guide in GUIDE_POSITIONS {
                            let y = bounds.top() + bounds.size.height * guide;
                            let mut builder = PathBuilder::stroke(px(1.0));
                            builder.move_to(point(bounds.left(), y));
                            builder.line_to(point(bounds.right(), y));
                            if let Ok(path) = builder.build() {
                                window.paint_path(path, guide_color);
                            }
                        }

                        let chart_points = points
                            .iter()
                            .map(|(x, y)| {
                                point(
                                    bounds.left() + bounds.size.width * *x,
                                    bounds.top() + bounds.size.height * *y,
                                )
                            })
                            .collect::<Vec<_>>();

                        if chart_points.len() >= 2 {
                            let mut fill = PathBuilder::fill();
                            fill.move_to(point(chart_points[0].x, bounds.bottom()));
                            for chart_point in &chart_points {
                                fill.line_to(*chart_point);
                            }
                            fill.line_to(point(
                                chart_points.last().map_or(bounds.right(), |point| point.x),
                                bounds.bottom(),
                            ));
                            fill.close();
                            if let Ok(path) = fill.build() {
                                window.paint_path(path, signal_color.opacity(0.14));
                            }

                            let mut line = PathBuilder::stroke(px(CHART_STROKE_WIDTH));
                            for (index, chart_point) in chart_points.into_iter().enumerate() {
                                if index == 0 {
                                    line.move_to(chart_point);
                                } else {
                                    line.line_to(chart_point);
                                }
                            }
                            if let Ok(path) = line.build() {
                                // GPUI tessellates and antialiases painted paths in the renderer.
                                window.paint_path(path, signal_color);
                            }
                        }
                    },
                )
                .flex_1()
                .min_h(px(160.0))
                .w_full(),
            )
            .child(
                div()
                    .flex()
                    .justify_between()
                    .pt_2()
                    .text_xs()
                    .text_color(cx.theme().muted_foreground)
                    .child(self.start_label)
                    .child(self.end_label),
            )
    }
}
