use gpui::{AppContext as _, Bounds, WindowBounds, WindowOptions, px, size};
use gpui_component::{Root, TitleBar};
use kabu_app::views::{
    AppShell, DEFAULT_WINDOW_HEIGHT, DEFAULT_WINDOW_WIDTH, MIN_WINDOW_HEIGHT, MIN_WINDOW_WIDTH,
};
use kabu_app::{
    app_model::{AppModel, clamp_window_size_to_display},
    runtime::RuntimeHandle,
};
use kabu_data::JsonRepository;
use std::sync::Arc;

fn main() {
    gpui::Application::new()
        .with_assets(gpui_component_assets::Assets)
        .run(|cx| {
            gpui_component::init(cx);
            kabu_app::commands::init(cx);
            cx.spawn(async move |cx| {
                let (model, bounds) = cx.update(|cx| {
                    let mut model = AppModel::with_shared_repository(
                        Arc::new(JsonRepository::default_macos()),
                        RuntimeHandle::default(),
                    );
                    let (persisted_width, persisted_height) = model.window_geometry();
                    let (display_width, display_height) = cx
                        .primary_display()
                        .map(|display| {
                            let size = display.default_bounds().size;
                            (f32::from(size.width), f32::from(size.height))
                        })
                        .unwrap_or((DEFAULT_WINDOW_WIDTH, DEFAULT_WINDOW_HEIGHT));
                    let (width, height) = clamp_window_size_to_display(
                        persisted_width,
                        persisted_height,
                        display_width,
                        display_height,
                    );
                    model.set_restored_window_geometry(width, height);
                    let bounds = Bounds::centered(None, size(px(width), px(height)), cx);
                    (cx.new(|_| model), bounds)
                })?;
                cx.open_window(
                    WindowOptions {
                        window_bounds: Some(WindowBounds::Windowed(bounds)),
                        window_min_size: Some(size(px(MIN_WINDOW_WIDTH), px(MIN_WINDOW_HEIGHT))),
                        titlebar: Some(TitleBar::title_bar_options()),
                        ..Default::default()
                    },
                    move |window, cx| {
                        let shell = cx.new(|cx| AppShell::with_model(model.clone(), window, cx));
                        cx.new(|cx| Root::new(shell, window, cx))
                    },
                )?;
                Ok::<_, anyhow::Error>(())
            })
            .detach();
        });
}
