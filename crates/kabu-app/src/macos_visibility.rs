#![allow(unsafe_code)]

pub trait VisibilityProbe {
    fn is_visible(&self) -> bool;
}

pub struct AlwaysVisible;

impl VisibilityProbe for AlwaysVisible {
    fn is_visible(&self) -> bool {
        true
    }
}

impl<T: VisibilityProbe + ?Sized> VisibilityProbe for Box<T> {
    fn is_visible(&self) -> bool {
        (**self).is_visible()
    }
}

pub struct VisibilityTracker<P> {
    probe: P,
    previous: Option<bool>,
}

impl<P: VisibilityProbe> VisibilityTracker<P> {
    pub fn new(probe: P) -> Self {
        Self {
            probe,
            previous: None,
        }
    }

    pub fn poll(&mut self) -> Option<bool> {
        let visible = self.probe.is_visible();
        if self.previous == Some(visible) {
            None
        } else {
            self.previous = Some(visible);
            Some(visible)
        }
    }
}

#[cfg(target_os = "macos")]
mod appkit {
    use super::VisibilityProbe;
    use gpui::Window;
    use objc2::{MainThreadMarker, rc::Retained};
    use objc2_app_kit::{NSApplication, NSView, NSWindow};
    use raw_window_handle::{HasWindowHandle, RawWindowHandle};

    #[derive(Debug, thiserror::Error)]
    pub enum VisibilityProbeError {
        #[error("the GPUI window handle is unavailable")]
        HandleUnavailable,
        #[error("the GPUI window did not expose an AppKit view")]
        NotAppKit,
        #[error("the AppKit view is not attached to a window")]
        DetachedView,
        #[error("the AppKit visibility probe must be created on the main thread")]
        NotMainThread,
    }

    pub struct MacOsVisibilityProbe {
        window: Retained<NSWindow>,
        application: Retained<NSApplication>,
    }

    impl MacOsVisibilityProbe {
        pub fn new(window: &Window) -> Result<Self, VisibilityProbeError> {
            let handle = HasWindowHandle::window_handle(window)
                .map_err(|_| VisibilityProbeError::HandleUnavailable)?;
            let RawWindowHandle::AppKit(handle) = handle.as_raw() else {
                return Err(VisibilityProbeError::NotAppKit);
            };
            // SAFETY: GPUI's AppKit raw handle is a non-null NSView owned by this live
            // GPUI Window. Construction is performed on the GPUI main thread, and the
            // returned NSWindow is retained before the borrowed raw-handle lifetime ends.
            let view = unsafe { handle.ns_view.cast::<NSView>().as_ref() };
            let appkit_window = view.window().ok_or(VisibilityProbeError::DetachedView)?;
            let main_thread = MainThreadMarker::new().ok_or(VisibilityProbeError::NotMainThread)?;
            let application = NSApplication::sharedApplication(main_thread);
            Ok(Self {
                window: appkit_window,
                application,
            })
        }
    }

    impl VisibilityProbe for MacOsVisibilityProbe {
        fn is_visible(&self) -> bool {
            !self.window.isMiniaturized() && !self.application.isHidden()
        }
    }
}

#[cfg(target_os = "macos")]
pub use appkit::{MacOsVisibilityProbe, VisibilityProbeError};

#[cfg(test)]
mod tests {
    use super::{VisibilityProbe, VisibilityTracker};
    use std::{cell::Cell, rc::Rc};

    struct FakeVisibilityProbe {
        visible: Rc<Cell<bool>>,
    }

    impl VisibilityProbe for FakeVisibilityProbe {
        fn is_visible(&self) -> bool {
            self.visible.get()
        }
    }

    #[test]
    fn visibility_edges_are_reported_once_and_activation_is_irrelevant() {
        let visible = Rc::new(Cell::new(true));
        let active = Rc::new(Cell::new(false));
        let probe = FakeVisibilityProbe {
            visible: visible.clone(),
        };
        let mut tracker = VisibilityTracker::new(probe);

        assert_eq!(tracker.poll(), Some(true));
        active.set(true);
        assert!(active.get());
        assert_eq!(tracker.poll(), None);
        visible.set(false);
        assert_eq!(tracker.poll(), Some(false));
        assert_eq!(tracker.poll(), None);
        visible.set(true);
        assert_eq!(tracker.poll(), Some(true));
    }
}
