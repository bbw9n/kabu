use gpui::App;

pub fn quit_after_last_window_closes(cx: &mut App) {
    on_last_window_closed(cx, |cx| cx.quit());
}

fn on_last_window_closed(cx: &mut App, mut action: impl FnMut(&mut App) + 'static) {
    cx.on_window_closed(move |cx| {
        if cx.windows().is_empty() {
            action(cx);
        }
    })
    .detach();
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    use gpui::{App, Context, IntoElement, Render, Window, div};

    use super::on_last_window_closed;

    struct EmptyView;

    impl Render for EmptyView {
        fn render(&mut self, _: &mut Window, _: &mut Context<Self>) -> impl IntoElement {
            div()
        }
    }

    #[gpui::test]
    fn last_window_removal_invokes_the_registered_application_action(
        cx: &mut gpui::TestAppContext,
    ) {
        let invoked = Arc::new(AtomicBool::new(false));
        let invoked_by_action = Arc::clone(&invoked);
        cx.update(|cx: &mut App| {
            on_last_window_closed(cx, move |_| {
                invoked_by_action.store(true, Ordering::Release);
            });
        });
        let window = cx.add_window(|_, _| EmptyView);

        window
            .update(cx, |_, window, _| window.remove_window())
            .expect("remove test window");
        cx.run_until_parked();

        assert!(invoked.load(Ordering::Acquire));
        assert!(cx.windows().is_empty());
    }
}
