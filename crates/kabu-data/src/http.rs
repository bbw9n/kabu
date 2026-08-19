use std::time::Duration;

use kabu_core::{MarketDataError, MarketDataErrorKind};
use reqwest::{Response, StatusCode};
use serde::de::DeserializeOwned;

pub(crate) fn client() -> Result<reqwest::Client, reqwest::Error> {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .user_agent(format!("Kabu/{}", env!("CARGO_PKG_VERSION")))
        .build()
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
