use kabu_core::{
    CachedMarketState, PersistedState, PersistenceError, STATE_SCHEMA_VERSION, StateRepository,
};
use kabu_data::JsonRepository;

#[test]
fn state_and_cache_are_independent_atomic_files() {
    let temp = tempfile::tempdir().expect("tempdir");
    let repo = JsonRepository::at(temp.path());

    repo.save_state(&PersistedState::starter())
        .expect("save state");
    repo.save_cache(&CachedMarketState::default())
        .expect("save cache");

    assert!(temp.path().join("state.json").is_file());
    assert!(temp.path().join("cache.json").is_file());
    assert_eq!(repo.load_state().expect("load").watchlist.len(), 4);
}

#[test]
fn corrupt_state_is_backed_up_without_destroying_cache() {
    let temp = tempfile::tempdir().expect("tempdir");
    std::fs::write(temp.path().join("state.json"), b"not json").expect("write corrupt state");
    std::fs::write(temp.path().join("cache.json"), valid_empty_cache()).expect("write cache");
    let repo = JsonRepository::at(temp.path());

    let recovery = repo.load_state_with_recovery().expect("recover");

    assert!(recovery.recovered_from_corruption);
    assert_eq!(recovery.value.watchlist.len(), 4);
    assert_eq!(repo.load_cache().expect("cache").quotes.len(), 0);
    assert_eq!(corrupt_backups(temp.path(), "state.json").len(), 1);
}

#[test]
fn corrupt_cache_is_backed_up_without_destroying_state() {
    let temp = tempfile::tempdir().expect("tempdir");
    std::fs::write(temp.path().join("state.json"), valid_starter_state()).expect("write state");
    std::fs::write(temp.path().join("cache.json"), b"not json").expect("write corrupt cache");
    let repo = JsonRepository::at(temp.path());

    let recovery = repo.load_cache_with_recovery().expect("recover");

    assert!(recovery.recovered_from_corruption);
    assert!(recovery.value.quotes.is_empty());
    assert_eq!(repo.load_state().expect("state").watchlist.len(), 4);
    assert_eq!(corrupt_backups(temp.path(), "cache.json").len(), 1);
}

#[test]
fn unsupported_schema_is_not_recovered_or_renamed() {
    let temp = tempfile::tempdir().expect("tempdir");
    let mut state = PersistedState::starter();
    state.schema_version = STATE_SCHEMA_VERSION + 1;
    let state_path = temp.path().join("state.json");
    std::fs::write(
        &state_path,
        serde_json::to_vec(&state).expect("serialize state"),
    )
    .expect("write state");
    let repo = JsonRepository::at(temp.path());

    let error = repo
        .load_state_with_recovery()
        .expect_err("unsupported schema");

    assert!(matches!(
        error,
        PersistenceError::UnsupportedSchema(version) if version == STATE_SCHEMA_VERSION + 1
    ));
    assert!(state_path.is_file());
    assert!(corrupt_backups(temp.path(), "state.json").is_empty());
}

fn valid_empty_cache() -> Vec<u8> {
    serde_json::to_vec(&CachedMarketState::default()).expect("serialize cache")
}

fn valid_starter_state() -> Vec<u8> {
    serde_json::to_vec(&PersistedState::starter()).expect("serialize state")
}

fn corrupt_backups(directory: &std::path::Path, file_name: &str) -> Vec<std::path::PathBuf> {
    let prefix = format!("{file_name}.corrupt-");
    std::fs::read_dir(directory)
        .expect("read directory")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(&prefix))
        })
        .collect()
}
