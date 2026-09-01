//! Provider integrations and persistence for Kabu.

mod binance;
mod http;
pub mod mcp;
pub mod persistence;
mod secret_store;
mod yahoo;

pub use binance::BinanceClient;
pub use mcp::{BrokerCredentialStore, BrokerOAuth, PendingAuthorization};
pub use persistence::{JsonRepository, Recovery};
pub use secret_store::{MacKeychainSecretStore, SecretStore, SecretStoreError};
pub use yahoo::YahooClient;
