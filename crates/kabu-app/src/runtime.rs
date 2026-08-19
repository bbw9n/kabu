/// Main-thread handle for Kabu's background runtime.
///
/// Task 10 extends this seam with command and event channels. Keeping the
/// placeholder zero-sized makes cached launch and model construction testable
/// without starting provider work in Task 9.
#[derive(Clone, Debug, Default)]
pub struct RuntimeHandle {
    _placeholder: (),
}
