use gpui::{AppContext as _, Bounds, WindowBounds, WindowOptions, px, size};
use gpui_component::{Root, TitleBar};
use kabu_app::views::{
    AppShell, DEFAULT_WINDOW_HEIGHT, DEFAULT_WINDOW_WIDTH, MIN_WINDOW_HEIGHT, MIN_WINDOW_WIDTH,
};
use kabu_app::{
    app_model::{AppModel, clamp_window_size_to_display},
    macos_visibility::MacOsVisibilityProbe,
    runtime::ProductionComponents,
    startup::StartupFailureView,
};

fn main() {
    gpui::Application::new()
        .with_assets(gpui_component_assets::Assets)
        .run(|cx| {
            gpui_component::init(cx);
            kabu_app::commands::init(cx);
            kabu_app::lifecycle::quit_after_last_window_closes(cx);
            cx.spawn(async move |cx| {
                let launch = match ProductionComponents::production()
                    .and_then(ProductionComponents::start)
                {
                    Ok(launch) => launch,
                    Err(error) => {
                        let category = error.category();
                        tracing::error!(
                            category = ?category,
                            "Kabu launch failed before the main window was ready"
                        );
                        let bounds =
                            cx.update(|cx| Bounds::centered(None, size(px(540.0), px(300.0)), cx))?;
                        if cx
                            .open_window(
                                WindowOptions {
                                    window_bounds: Some(WindowBounds::Windowed(bounds)),
                                    window_min_size: Some(size(px(420.0), px(260.0))),
                                    titlebar: Some(TitleBar::title_bar_options()),
                                    ..Default::default()
                                },
                                move |window, cx| {
                                    let view = cx.new(|_| StartupFailureView::new(category));
                                    cx.new(|cx| Root::new(view, window, cx))
                                },
                            )
                            .is_err()
                        {
                            tracing::error!("Kabu could not open the launch-failure window");
                            _ = cx.update(|cx| cx.quit());
                        }
                        return Ok::<_, anyhow::Error>(());
                    }
                };
                let (model, bounds) = cx.update(|cx| {
                    let mut model =
                        AppModel::from_recoveries(launch.state, launch.cache, launch.runtime);
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
                        let probe = MacOsVisibilityProbe::new(window)
                            .expect("production GPUI window exposes an AppKit view");
                        let shell = cx.new(|cx| {
                            AppShell::with_model_and_visibility(
                                model.clone(),
                                Box::new(probe),
                                window,
                                cx,
                            )
                        });
                        cx.new(|cx| Root::new(shell, window, cx))
                    },
                )?;
                Ok::<_, anyhow::Error>(())
            })
            .detach();
        });
}
