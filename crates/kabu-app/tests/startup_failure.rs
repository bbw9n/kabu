use gpui::{AppContext as _, Modifiers};
use gpui_component::Root;
use kabu_app::{
    runtime::{ProductionComponents, ProductionStartError, StartupFailureCategory},
    startup::{StartupFailureView, startup_failure_presentation},
};
use kabu_core::{PersistedState, StateRepository};

#[test]
fn state_load_and_schema_failures_are_classified_before_launch() {
    let unreadable = tempfile::tempdir().expect("temporary data directory");
    std::fs::create_dir(unreadable.path().join("state.json")).expect("directory at state path");
    let error = match ProductionComponents::at(unreadable.path()) {
        Ok(_) => panic!("state load must fail"),
        Err(error) => error,
    };
    assert_eq!(error.category(), StartupFailureCategory::PersistedState);

    let unsupported = tempfile::tempdir().expect("temporary data directory");
    let repository = kabu_data::JsonRepository::at(unsupported.path());
    repository
        .save_state(&PersistedState::starter())
        .expect("write valid state");
    let state_path = unsupported.path().join("state.json");
    let state = std::fs::read_to_string(&state_path).expect("read state fixture");
    std::fs::write(
        &state_path,
        state.replacen("\"schema_version\": 1", "\"schema_version\": 99", 1),
    )
    .expect("write unsupported schema");
    let error = match ProductionComponents::at(unsupported.path()) {
        Ok(_) => panic!("schema must fail"),
        Err(error) => error,
    };
    assert_eq!(error.category(), StartupFailureCategory::PersistedState);
}

#[test]
fn cache_client_and_runtime_failures_are_classified_without_rendering_their_sources() {
    let cache_failure = tempfile::tempdir().expect("temporary data directory");
    std::fs::create_dir(cache_failure.path().join("cache.json")).expect("directory at cache path");
    let error = match ProductionComponents::at(cache_failure.path()) {
        Ok(_) => panic!("cache load must fail"),
        Err(error) => error,
    };
    assert_eq!(error.category(), StartupFailureCategory::MarketCache);

    let client_source = reqwest::Client::builder()
        .user_agent("private\nprovider-body")
        .build()
        .expect_err("invalid user-agent creates a client error");
    let client = ProductionStartError::ProviderSetup(client_source);
    let runtime =
        ProductionStartError::BackgroundRuntime(std::io::Error::other("/private/runtime/socket"));
    assert_eq!(client.category(), StartupFailureCategory::ProviderSetup);
    assert_eq!(
        runtime.category(),
        StartupFailureCategory::BackgroundRuntime
    );

    for category in [client.category(), runtime.category()] {
        let presentation = startup_failure_presentation(category);
        let visible = format!(
            "{} {} {}",
            presentation.title, presentation.message, presentation.help
        );
        assert!(!visible.contains("provider-body"));
        assert!(!visible.contains("/private/"));
    }
}

#[test]
fn every_launch_failure_category_has_fixed_sanitized_actionable_copy() {
    let private_material = [
        "/Users/private/Kabu/state.json",
        "persisted secret query",
        "provider response body",
    ];

    for category in [
        StartupFailureCategory::PersistedState,
        StartupFailureCategory::MarketCache,
        StartupFailureCategory::ProviderSetup,
        StartupFailureCategory::BackgroundRuntime,
    ] {
        let presentation = startup_failure_presentation(category);
        assert!(!presentation.title.is_empty());
        assert!(!presentation.message.is_empty());
        assert!(presentation.help.contains("Close Kabu"));
        let rendered = format!(
            "{} {} {}",
            presentation.title, presentation.message, presentation.help
        );
        assert!(
            private_material
                .iter()
                .all(|private| !rendered.contains(private))
        );
    }
}

#[gpui::test]
fn launch_failure_produces_a_visible_view_with_an_accessible_close_action(
    cx: &mut gpui::TestAppContext,
) {
    cx.update(gpui_component::init);
    let (_, cx) = cx.add_window_view(|window, cx| {
        let view = cx.new(|_| StartupFailureView::new(StartupFailureCategory::ProviderSetup));
        Root::new(view, window, cx)
    });
    cx.refresh().expect("render fatal launch view");

    assert!(cx.debug_bounds("startup-failure-view").is_some());
    let close = cx
        .debug_bounds("startup-failure-close")
        .expect("accessible close button");
    cx.simulate_click(close.center(), Modifiers::none());
    cx.run_until_parked();

    assert!(cx.windows().is_empty());
}
