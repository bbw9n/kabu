use chrono::Utc;
use directories::BaseDirs;
use kabu_core::{
    CachedMarketState, PersistedState, PersistenceError, STATE_SCHEMA_VERSION, StateRepository,
};
use serde::{Serialize, de::DeserializeOwned};
use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
};
use tempfile::NamedTempFile;

const STATE_FILE: &str = "state.json";
const CACHE_FILE: &str = "cache.json";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Recovery<T> {
    pub value: T,
    pub recovered_from_corruption: bool,
}

#[derive(Debug, Clone)]
pub struct JsonRepository {
    directory: PathBuf,
}

impl JsonRepository {
    pub fn default_macos() -> Self {
        let base_dirs = BaseDirs::new().expect("supported platform has a data directory");
        Self::at(base_dirs.data_dir().join("Kabu"))
    }

    pub fn at(directory: impl Into<PathBuf>) -> Self {
        Self {
            directory: directory.into(),
        }
    }

    pub fn load_state_with_recovery(&self) -> Result<Recovery<PersistedState>, PersistenceError> {
        self.load_with_recovery(STATE_FILE, PersistedState::starter)
    }

    pub fn load_cache_with_recovery(
        &self,
    ) -> Result<Recovery<CachedMarketState>, PersistenceError> {
        self.load_with_recovery(CACHE_FILE, CachedMarketState::default)
    }

    fn load_with_recovery<T>(
        &self,
        file_name: &str,
        fallback: impl FnOnce() -> T,
    ) -> Result<Recovery<T>, PersistenceError>
    where
        T: DeserializeOwned + HasSchemaVersion,
    {
        let path = self.directory.join(file_name);
        let contents = match fs::read(&path) {
            Ok(contents) => contents,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Recovery {
                    value: fallback(),
                    recovered_from_corruption: false,
                });
            }
            Err(error) => return Err(error.into()),
        };

        match serde_json::from_slice::<T>(&contents) {
            Ok(value) if value.schema_version() == STATE_SCHEMA_VERSION => Ok(Recovery {
                value,
                recovered_from_corruption: false,
            }),
            Ok(value) => Err(PersistenceError::UnsupportedSchema(value.schema_version())),
            Err(_) => {
                self.back_up_corrupt_file(&path, file_name)?;
                Ok(Recovery {
                    value: fallback(),
                    recovered_from_corruption: true,
                })
            }
        }
    }

    fn save<T: Serialize>(&self, file_name: &str, value: &T) -> Result<(), PersistenceError> {
        fs::create_dir_all(&self.directory)?;
        let bytes = serde_json::to_vec_pretty(value)?;
        let mut temporary = NamedTempFile::new_in(&self.directory)?;
        temporary.write_all(&bytes)?;
        temporary.as_file().sync_all()?;
        temporary
            .persist(self.directory.join(file_name))
            .map_err(|error| error.error)?;
        Ok(())
    }

    fn back_up_corrupt_file(&self, path: &Path, file_name: &str) -> Result<(), PersistenceError> {
        let timestamp = Utc::now().format("%Y%m%dT%H%M%SZ");
        fs::rename(
            path,
            self.directory
                .join(format!("{file_name}.corrupt-{timestamp}")),
        )?;
        Ok(())
    }
}

impl StateRepository for JsonRepository {
    fn load_state(&self) -> Result<PersistedState, PersistenceError> {
        Ok(self.load_state_with_recovery()?.value)
    }

    fn save_state(&self, state: &PersistedState) -> Result<(), PersistenceError> {
        self.save(STATE_FILE, state)
    }

    fn load_cache(&self) -> Result<CachedMarketState, PersistenceError> {
        Ok(self.load_cache_with_recovery()?.value)
    }

    fn save_cache(&self, cache: &CachedMarketState) -> Result<(), PersistenceError> {
        self.save(CACHE_FILE, cache)
    }
}

trait HasSchemaVersion {
    fn schema_version(&self) -> u32;
}

impl HasSchemaVersion for PersistedState {
    fn schema_version(&self) -> u32 {
        self.schema_version
    }
}

impl HasSchemaVersion for CachedMarketState {
    fn schema_version(&self) -> u32 {
        self.schema_version
    }
}
