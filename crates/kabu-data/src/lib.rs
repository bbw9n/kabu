//! Provider integrations and persistence for Kabu.

mod http;
pub mod persistence;
mod yahoo;

pub use persistence::{JsonRepository, Recovery};
pub use yahoo::YahooClient;
