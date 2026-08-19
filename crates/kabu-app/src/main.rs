use gpui::{AppContext as _, Bounds, WindowBounds, WindowOptions, px, size};
use gpui_component::{Root, TitleBar};
use kabu_app::views::{
    AppShell, DEFAULT_WINDOW_HEIGHT, DEFAULT_WINDOW_WIDTH, MIN_WINDOW_HEIGHT, MIN_WINDOW_WIDTH,
};

fn main() {
    gpui::Application::new()
        .with_assets(gpui_component_assets::Assets)
        .run(|cx| {
            gpui_component::init(cx);
            kabu_app::commands::init(cx);
            cx.spawn(async move |cx| {
                let bounds = cx.update(|cx| {
                    Bounds::centered(
                        None,
                        size(px(DEFAULT_WINDOW_WIDTH), px(DEFAULT_WINDOW_HEIGHT)),
                        cx,
                    )
                })?;
                cx.open_window(
                    WindowOptions {
                        window_bounds: Some(WindowBounds::Windowed(bounds)),
                        window_min_size: Some(size(px(MIN_WINDOW_WIDTH), px(MIN_WINDOW_HEIGHT))),
                        titlebar: Some(TitleBar::title_bar_options()),
                        ..Default::default()
                    },
                    |window, cx| {
                        let shell = cx.new(|cx| AppShell::new(window, cx));
                        cx.new(|cx| Root::new(shell, window, cx))
                    },
                )?;
                Ok::<_, anyhow::Error>(())
            })
            .detach();
        });
}
