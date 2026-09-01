use thiserror::Error;

/// A synchronous boundary for platform-backed secret storage.
pub trait SecretStore: Send + Sync {
    fn get(&self, service: &str, account: &str) -> Result<Option<Vec<u8>>, SecretStoreError>;
    fn set(&self, service: &str, account: &str, value: &[u8]) -> Result<(), SecretStoreError>;
    fn delete(&self, service: &str, account: &str) -> Result<(), SecretStoreError>;
}

/// Sanitized failures from the platform secret store.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum SecretStoreError {
    #[error("secure storage is unavailable")]
    Unavailable,
    #[error("secure storage contains invalid data")]
    InvalidData,
    #[error("secure storage operation could not be completed")]
    TaskFailed,
    #[error("secure storage is unsupported on this platform")]
    UnsupportedPlatform,
}

/// The macOS Keychain implementation of [`SecretStore`].
pub struct MacKeychainSecretStore;

impl MacKeychainSecretStore {
    pub fn new() -> Self {
        Self
    }
}

impl Default for MacKeychainSecretStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(target_os = "macos")]
impl SecretStore for MacKeychainSecretStore {
    fn get(&self, service: &str, account: &str) -> Result<Option<Vec<u8>>, SecretStoreError> {
        use security_framework::passwords::{PasswordOptions, generic_password};

        const ITEM_NOT_FOUND: i32 = -25_300;
        match generic_password(PasswordOptions::new_generic_password(service, account)) {
            Ok(value) => Ok(Some(value)),
            Err(error) if error.code() == ITEM_NOT_FOUND => Ok(None),
            Err(_) => Err(SecretStoreError::Unavailable),
        }
    }

    fn set(&self, service: &str, account: &str, value: &[u8]) -> Result<(), SecretStoreError> {
        use security_framework::passwords::set_generic_password;

        set_generic_password(service, account, value).map_err(|_| SecretStoreError::Unavailable)
    }

    fn delete(&self, service: &str, account: &str) -> Result<(), SecretStoreError> {
        use security_framework::passwords::delete_generic_password;

        const ITEM_NOT_FOUND: i32 = -25_300;
        match delete_generic_password(service, account) {
            Ok(()) => Ok(()),
            Err(error) if error.code() == ITEM_NOT_FOUND => Ok(()),
            Err(_) => Err(SecretStoreError::Unavailable),
        }
    }
}

#[cfg(not(target_os = "macos"))]
impl SecretStore for MacKeychainSecretStore {
    fn get(&self, _service: &str, _account: &str) -> Result<Option<Vec<u8>>, SecretStoreError> {
        Err(SecretStoreError::UnsupportedPlatform)
    }

    fn set(&self, _service: &str, _account: &str, _value: &[u8]) -> Result<(), SecretStoreError> {
        Err(SecretStoreError::UnsupportedPlatform)
    }

    fn delete(&self, _service: &str, _account: &str) -> Result<(), SecretStoreError> {
        Err(SecretStoreError::UnsupportedPlatform)
    }
}
