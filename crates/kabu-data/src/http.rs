use std::time::Duration;

use kabu_core::{MarketDataError, MarketDataErrorKind};
use reqwest::{Response, StatusCode};
use serde::de::DeserializeOwned;

pub(crate) fn client() -> Result<reqwest::Client, reqwest::Error> {
    ensure_default_crypto_provider();
    reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .user_agent(format!("Kabu/{}", env!("CARGO_PKG_VERSION")))
        .build()
}

pub(crate) fn ensure_default_crypto_provider() {
    if rustls::crypto::CryptoProvider::get_default().is_some() {
        return;
    }

    // Kabu's direct HTTP provider stack uses Reqwest's AWS-LC Rustls backend.
    // GPUI also enables Ring for its internal HTTP client, so the process-wide
    // Rustls feature set is ambiguous and applications must choose explicitly.
    // A concurrent installer can win this one-time race; its installed process
    // default is then the accepted provider for every Rustls client.
    _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
}

pub(crate) async fn decode_json<T: DeserializeOwned>(
    response: Response,
    endpoint: &str,
) -> Result<T, MarketDataError> {
    let response = classify_response(response, endpoint)?;
    let bytes = response
        .bytes()
        .await
        .map_err(|error| classify_request_error(error, endpoint))?;
    serde_json::from_slice(&bytes).map_err(|_| malformed_response(endpoint))
}

pub(crate) fn classify_response(
    response: Response,
    endpoint: &str,
) -> Result<Response, MarketDataError> {
    let status = response.status();
    if status.is_success() {
        return Ok(response);
    }

    let retry_after = if status == StatusCode::TOO_MANY_REQUESTS {
        response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .map(Duration::from_secs)
    } else {
        None
    };
    let kind = match status {
        StatusCode::TOO_MANY_REQUESTS => MarketDataErrorKind::RateLimited,
        StatusCode::NOT_FOUND => MarketDataErrorKind::UnsupportedInstrument,
        status if status.is_client_error() || status.is_server_error() => {
            MarketDataErrorKind::Unavailable
        }
        _ => MarketDataErrorKind::Unavailable,
    };

    Err(MarketDataError {
        kind,
        message: format!("{endpoint} request returned HTTP {}", status.as_u16()),
        retry_after,
    })
}

pub(crate) fn classify_request_error(error: reqwest::Error, endpoint: &str) -> MarketDataError {
    let kind = if error.is_timeout() {
        MarketDataErrorKind::Timeout
    } else {
        MarketDataErrorKind::Unavailable
    };
    MarketDataError {
        kind,
        message: format!("{endpoint} request failed"),
        retry_after: None,
    }
}

pub(crate) fn malformed_response(endpoint: &str) -> MarketDataError {
    MarketDataError {
        kind: MarketDataErrorKind::MalformedResponse,
        message: format!("{endpoint} response was malformed"),
        retry_after: None,
    }
}

#[cfg(test)]
mod tests {
    use super::{client, ensure_default_crypto_provider};
    use rustls::crypto::CryptoProvider;
    use std::{
        process::Command,
        sync::{Arc, Barrier},
    };

    const PRISTINE_PROVIDER_CHILD: &str = "KABU_PRISTINE_PROVIDER_CHILD";

    #[test]
    fn production_clients_reuse_the_installed_crypto_provider() {
        let _first_client = client().expect("first production HTTP client");
        let installed = CryptoProvider::get_default().expect("installed crypto provider");

        let _second_client = client().expect("second production HTTP client");
        let reused = CryptoProvider::get_default().expect("reused crypto provider");
        assert!(Arc::ptr_eq(installed, reused));
    }

    #[test]
    fn pristine_process_concurrently_installs_aws_lc() {
        let output = Command::new(std::env::current_exe().expect("current test executable"))
            .args([
                "--exact",
                "http::tests::pristine_provider_child",
                "--nocapture",
            ])
            .env(PRISTINE_PROVIDER_CHILD, "1")
            .output()
            .expect("run pristine provider child");

        assert!(
            output.status.success(),
            "pristine provider child failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn pristine_provider_child() {
        if std::env::var(PRISTINE_PROVIDER_CHILD).as_deref() != Ok("1") {
            return;
        }

        assert!(
            CryptoProvider::get_default().is_none(),
            "child test process must begin without a provider"
        );
        let worker_count = 16;
        let barrier = Arc::new(Barrier::new(worker_count));
        let workers = (0..worker_count)
            .map(|_| {
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    ensure_default_crypto_provider();
                })
            })
            .collect::<Vec<_>>();
        for worker in workers {
            worker.join().expect("provider worker");
        }

        let installed = CryptoProvider::get_default().expect("installed crypto provider");
        let aws_lc = rustls::crypto::aws_lc_rs::default_provider();
        assert!(std::ptr::eq(installed.secure_random, aws_lc.secure_random));
    }
}
