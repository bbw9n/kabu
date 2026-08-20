use crate::runtime::StartupFailureCategory;
use gpui::{
    Context, InteractiveElement as _, IntoElement, ParentElement as _, Render, Styled as _, Window,
    div, px,
};
use gpui_component::{ActiveTheme as _, StyledExt as _, button::Button};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StartupFailurePresentation {
    pub title: &'static str,
    pub message: &'static str,
    pub help: &'static str,
}

pub const fn startup_failure_presentation(
    category: StartupFailureCategory,
) -> StartupFailurePresentation {
    match category {
        StartupFailureCategory::PersistedState => StartupFailurePresentation {
            title: "Kabu could not load your settings",
            message: "Your saved settings could not be read safely.",
            help: "Close Kabu, check that its data folder is readable, then reopen the app.",
        },
        StartupFailureCategory::MarketCache => StartupFailurePresentation {
            title: "Kabu could not load its market cache",
            message: "Saved market data could not be read safely.",
            help: "Close Kabu, check that its data folder is readable, then reopen the app.",
        },
        StartupFailureCategory::ProviderSetup => StartupFailurePresentation {
            title: "Kabu could not prepare market data",
            message: "Market-data connections could not be initialized.",
            help: "Close Kabu and reopen it. If this continues, check your network settings.",
        },
        StartupFailureCategory::BackgroundRuntime => StartupFailurePresentation {
            title: "Kabu could not start",
            message: "The background runtime could not be initialized.",
            help: "Close Kabu and reopen it. If this continues, restart your Mac.",
        },
    }
}

pub struct StartupFailureView {
    category: StartupFailureCategory,
}

impl StartupFailureView {
    pub const fn new(category: StartupFailureCategory) -> Self {
        Self { category }
    }
}

impl Render for StartupFailureView {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let presentation = startup_failure_presentation(self.category);
        div()
            .debug_selector(|| "startup-failure-view".into())
            .size_full()
            .flex()
            .items_center()
            .justify_center()
            .bg(cx.theme().background)
            .p_6()
            .child(
                div()
                    .w_full()
                    .max_w(px(480.0))
                    .flex()
                    .flex_col()
                    .gap_3()
                    .rounded_lg()
                    .border_1()
                    .border_color(cx.theme().border)
                    .bg(cx.theme().popover)
                    .p_5()
                    .child(div().text_lg().font_semibold().child(presentation.title))
                    .child(
                        div()
                            .text_sm()
                            .text_color(cx.theme().foreground)
                            .child(presentation.message),
                    )
                    .child(
                        div()
                            .text_sm()
                            .text_color(cx.theme().muted_foreground)
                            .child(presentation.help),
                    )
                    .child(
                        div().flex().justify_end().child(
                            div()
                                .debug_selector(|| "startup-failure-close".into())
                                .child(
                                    Button::new("close-kabu-after-launch-failure")
                                        .label("Close Kabu")
                                        .on_click(|_, window, _| window.remove_window()),
                                ),
                        ),
                    ),
            )
    }
}
