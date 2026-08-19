mod app_shell;
mod search;
mod theme_switcher;
mod watchlist;

pub use crate::app_model::{
    DEFAULT_SIDEBAR_WIDTH as SIDEBAR_WIDTH, DEFAULT_WINDOW_HEIGHT, DEFAULT_WINDOW_WIDTH,
    MIN_WINDOW_HEIGHT, MIN_WINDOW_WIDTH,
};
pub use app_shell::{AppShell, SelectedQuoteAvailability, SelectedQuoteHeader};
pub use search::{SEARCH_OVERLAY_KEY_CONTEXT, SearchOverlay, SearchOverlayEvent};
pub use theme_switcher::ThemeSwitcher;
pub use watchlist::{WATCHLIST_KEY_CONTEXT, WatchlistPane};
