mod app_shell;
mod price_chart;
mod quote_detail;
mod search;
mod theme_switcher;
mod watchlist;

pub use crate::app_model::{
    DEFAULT_SIDEBAR_WIDTH as SIDEBAR_WIDTH, DEFAULT_WINDOW_HEIGHT, DEFAULT_WINDOW_WIDTH,
    MIN_WINDOW_HEIGHT, MIN_WINDOW_WIDTH,
};
pub use app_shell::{AppShell, SelectedQuoteAvailability, SelectedQuoteHeader};
pub use price_chart::{
    CHART_STROKE_WIDTH, CHART_VERTICAL_PADDING, ChartDirection, PriceChart, normalize_closes,
    series_direction,
};
pub use quote_detail::{QuoteDetail, QuoteDetailData, freshness_label};
pub use search::{SEARCH_OVERLAY_KEY_CONTEXT, SearchOverlay, SearchOverlayEvent};
pub use theme_switcher::ThemeSwitcher;
pub use watchlist::{WATCHLIST_KEY_CONTEXT, WatchlistPane};
