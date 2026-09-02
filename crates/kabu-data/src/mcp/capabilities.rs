use std::{
    borrow::Cow,
    collections::{BTreeMap, HashSet},
    io::{self, Write},
    sync::Arc,
    time::Duration,
};

use futures_util::{StreamExt, future::ready, stream::BoxStream};
use kabu_core::{Brokerage, BrokerageError, BrokerageErrorKind};
use reqwest::header::{ACCEPT, CONTENT_TYPE, HeaderName, HeaderValue, WWW_AUTHENTICATE};
use rmcp::{
    RoleClient, ServiceError, ServiceExt,
    model::{
        CallToolRequestParams, CallToolResult, ClientJsonRpcMessage, ClientRequest, ErrorData,
        JsonRpcMessage, ServerJsonRpcMessage, Tool,
    },
    service::RunningService,
    transport::{
        auth::AuthClient,
        streamable_http_client::{
            AuthRequiredError, SseError, StreamableHttpClient, StreamableHttpClientTransport,
            StreamableHttpClientTransportConfig, StreamableHttpError, StreamableHttpPostResponse,
        },
    },
};
use serde_json::{Map, Value, json};
use sse_stream::{Sse, SseStream};
use url::Url;

const MAX_RAW_RESULT_BYTES: usize = 1_048_576;
const MAX_ARRAY_ITEMS: usize = 2_000;
const MAX_STRING_BYTES: usize = 16 * 1_024;
const MAX_NESTING_DEPTH: usize = 32;
const MAX_DECIMAL_SIGNIFICANT_DIGITS: usize = 38;
const SESSION_TIMEOUT: Duration = Duration::from_secs(5);
const WRITE_ORIENTED_FRAGMENTS: &[&str] = &[
    "order", "buy", "sell", "cancel", "replace", "transfer", "alert", "dca", "grid", "create",
    "update", "delete",
];
const DESCRIPTIVE_SCHEMA_KEYS: &[&str] = &["title", "description", "examples", "$comment"];
const HEADER_SESSION_ID: &str = "Mcp-Session-Id";
const HEADER_LAST_EVENT_ID: &str = "Last-Event-Id";
const HEADER_PROTOCOL_VERSION: &str = "MCP-Protocol-Version";
const EVENT_STREAM_MIME_TYPE: &str = "text/event-stream";
const JSON_MIME_TYPE: &str = "application/json";

/// A compile-time role for one approved, read-only broker operation.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ReadCapability {
    Accounts,
    PortfolioSummary,
    EquityPositions,
    CryptoPositions,
}

/// Closed, capability-specific inputs accepted by the read-only session.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReadCapabilityInput {
    Accounts { currency: Option<String> },
    PortfolioSummary,
    EquityPositions,
    CryptoPositions,
}

impl ReadCapabilityInput {
    fn capability(&self) -> ReadCapability {
        match self {
            Self::Accounts { .. } => ReadCapability::Accounts,
            Self::PortfolioSummary => ReadCapability::PortfolioSummary,
            Self::EquityPositions => ReadCapability::EquityPositions,
            Self::CryptoPositions => ReadCapability::CryptoPositions,
        }
    }

    fn into_arguments(
        self,
        schema: InputSchema,
    ) -> Result<Option<Map<String, Value>>, BrokerageError> {
        match (self, schema) {
            (Self::Accounts { currency: None }, InputSchema::EmptyObject)
            | (Self::Accounts { currency: None }, InputSchema::OptionalString("currency"))
            | (Self::PortfolioSummary, InputSchema::EmptyObject)
            | (Self::EquityPositions, InputSchema::EmptyObject)
            | (Self::CryptoPositions, InputSchema::EmptyObject) => Ok(None),
            (
                Self::Accounts {
                    currency: Some(currency),
                },
                InputSchema::OptionalString("currency"),
            ) => Ok(Some(Map::from_iter([(
                "currency".to_string(),
                Value::String(currency),
            )]))),
            _ => Err(incompatible_schema()),
        }
    }
}

/// The exact provider tool names accepted for one connected session.
#[derive(Debug)]
pub struct ResolvedCapabilities(BTreeMap<ReadCapability, Arc<str>>);

impl ResolvedCapabilities {
    pub fn name(&self, capability: ReadCapability) -> &str {
        self.0
            .get(&capability)
            .map(AsRef::as_ref)
            .expect("requested capability is not available for this brokerage")
    }

    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.0.values().map(AsRef::as_ref)
    }

    fn get(&self, capability: ReadCapability) -> Option<&Arc<str>> {
        self.0.get(&capability)
    }
}

#[derive(Clone, Copy)]
enum InputSchema {
    EmptyObject,
    OptionalString(&'static str),
}

struct CapabilityDescriptor {
    brokerage: Brokerage,
    capability: ReadCapability,
    candidates: &'static [&'static str],
    schema: InputSchema,
}

const CAPABILITY_DESCRIPTORS: &[CapabilityDescriptor] = &[
    CapabilityDescriptor {
        brokerage: Brokerage::Longbridge,
        capability: ReadCapability::Accounts,
        candidates: &["account_balance"],
        schema: InputSchema::OptionalString("currency"),
    },
    CapabilityDescriptor {
        brokerage: Brokerage::Longbridge,
        capability: ReadCapability::EquityPositions,
        candidates: &["stock_positions"],
        schema: InputSchema::EmptyObject,
    },
    CapabilityDescriptor {
        brokerage: Brokerage::Robinhood,
        capability: ReadCapability::Accounts,
        candidates: &["get_accounts", "list_accounts"],
        schema: InputSchema::EmptyObject,
    },
    CapabilityDescriptor {
        brokerage: Brokerage::Robinhood,
        capability: ReadCapability::PortfolioSummary,
        candidates: &["get_portfolio", "get_portfolio_summary"],
        schema: InputSchema::EmptyObject,
    },
    CapabilityDescriptor {
        brokerage: Brokerage::Robinhood,
        capability: ReadCapability::EquityPositions,
        candidates: &["get_equity_positions", "list_equity_positions"],
        schema: InputSchema::EmptyObject,
    },
    CapabilityDescriptor {
        brokerage: Brokerage::Robinhood,
        capability: ReadCapability::CryptoPositions,
        candidates: &["get_crypto_positions", "list_crypto_positions"],
        schema: InputSchema::EmptyObject,
    },
];

/// Resolve only known read roles whose complete input schemas still match.
pub fn resolve_capabilities(
    brokerage: Brokerage,
    tools: &[Tool],
) -> Result<ResolvedCapabilities, BrokerageError> {
    let mut resolved = BTreeMap::new();
    let mut seen_names = HashSet::new();

    for descriptor in CAPABILITY_DESCRIPTORS
        .iter()
        .filter(|descriptor| descriptor.brokerage == brokerage)
    {
        if descriptor
            .candidates
            .iter()
            .any(|name| is_write_oriented(name))
        {
            return Err(incompatible_schema());
        }

        let mut selected = None;
        for candidate in descriptor.candidates {
            for tool in tools.iter().filter(|tool| tool.name == *candidate) {
                if !seen_names.insert(tool.name.to_string())
                    || is_write_oriented(&tool.name)
                    || !schema_matches(&tool.input_schema, descriptor.schema)
                {
                    return Err(incompatible_schema());
                }
                if selected.is_none() {
                    selected = Some(Arc::<str>::from(tool.name.as_ref()));
                }
            }
        }

        let Some(name) = selected else {
            return Err(incompatible_schema());
        };
        resolved.insert(descriptor.capability, name);
    }

    Ok(ResolvedCapabilities(resolved))
}

fn is_write_oriented(name: &str) -> bool {
    let lowercase = name.to_ascii_lowercase();
    WRITE_ORIENTED_FRAGMENTS
        .iter()
        .any(|fragment| lowercase.contains(fragment))
}

fn schema_matches(schema: &Map<String, Value>, expected: InputSchema) -> bool {
    let expected = match expected {
        InputSchema::EmptyObject => json!({
            "type": "object",
            "properties": {}
        }),
        InputSchema::OptionalString(property) => json!({
            "type": "object",
            "properties": {
                property: { "type": "string" }
            }
        }),
    };
    strip_descriptive_metadata(Value::Object(schema.clone())) == expected
}

fn strip_descriptive_metadata(value: Value) -> Value {
    match value {
        Value::Object(object) => Value::Object(
            object
                .into_iter()
                .filter(|(key, _)| !DESCRIPTIVE_SCHEMA_KEYS.contains(&key.as_str()))
                .map(|(key, value)| {
                    let value = if key == "properties" {
                        strip_property_schemas(value)
                    } else {
                        strip_descriptive_metadata(value)
                    };
                    (key, value)
                })
                .collect(),
        ),
        Value::Array(values) => {
            Value::Array(values.into_iter().map(strip_descriptive_metadata).collect())
        }
        primitive => primitive,
    }
}

fn strip_property_schemas(value: Value) -> Value {
    let Value::Object(properties) = value else {
        return value;
    };
    Value::Object(
        properties
            .into_iter()
            .map(|(name, schema)| (name, strip_descriptive_metadata(schema)))
            .collect(),
    )
}

fn incompatible_schema() -> BrokerageError {
    BrokerageError::new(BrokerageErrorKind::IncompatibleToolSchema)
}

fn capability_descriptor(
    brokerage: Brokerage,
    capability: ReadCapability,
) -> Option<&'static CapabilityDescriptor> {
    CAPABILITY_DESCRIPTORS
        .iter()
        .find(|descriptor| descriptor.brokerage == brokerage && descriptor.capability == capability)
}

/// JSON returned by a broker tool after all Kabu wire bounds have passed.
#[derive(Debug)]
pub struct BoundedToolResult(Value);

impl BoundedToolResult {
    /// Parse and validate an untrusted tool payload, checking its byte bound first.
    pub fn parse(payload: &[u8]) -> Result<Self, BrokerageError> {
        if payload.len() > MAX_RAW_RESULT_BYTES {
            return Err(malformed_response());
        }
        let value: Value = serde_json::from_slice(payload).map_err(|_| malformed_response())?;
        validate_value(&value, 0)?;
        Ok(Self(value))
    }

    pub fn value(&self) -> &Value {
        &self.0
    }

    pub fn into_value(self) -> Value {
        self.0
    }

    fn from_call_result(result: CallToolResult) -> Result<Self, BrokerageError> {
        let raw = serialize_bounded(&result)?;
        Self::parse(&raw)?;
        if result.is_error == Some(true) {
            return Err(BrokerageError::new(BrokerageErrorKind::Unavailable));
        }

        if let Some(value) = result.structured_content {
            return Self::from_value(value);
        }
        let text = result
            .content
            .first()
            .and_then(|content| content.as_text())
            .ok_or_else(malformed_response)?;
        Self::parse(text.text.as_bytes())
    }

    fn from_value(value: Value) -> Result<Self, BrokerageError> {
        let raw = serialize_bounded(&value)?;
        Self::parse(&raw)
    }
}

fn serialize_bounded(value: &impl serde::Serialize) -> Result<Vec<u8>, BrokerageError> {
    let mut writer = BoundedWriter::default();
    serde_json::to_writer(&mut writer, value).map_err(|_| malformed_response())?;
    Ok(writer.bytes)
}

#[derive(Default)]
struct BoundedWriter {
    bytes: Vec<u8>,
}

impl Write for BoundedWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let remaining = MAX_RAW_RESULT_BYTES.saturating_sub(self.bytes.len());
        if buffer.len() > remaining {
            return Err(io::Error::other("broker tool result exceeds byte limit"));
        }
        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn validate_value(value: &Value, depth: usize) -> Result<(), BrokerageError> {
    if depth > MAX_NESTING_DEPTH {
        return Err(malformed_response());
    }
    match value {
        Value::Array(values) => {
            if values.len() > MAX_ARRAY_ITEMS {
                return Err(malformed_response());
            }
            for value in values {
                validate_value(value, depth + 1)?;
            }
        }
        Value::Object(object) => {
            for (key, value) in object {
                validate_string(key)?;
                validate_value(value, depth + 1)?;
            }
        }
        Value::String(value) => validate_string(value)?,
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
    Ok(())
}

fn validate_string(value: &str) -> Result<(), BrokerageError> {
    if value.len() > MAX_STRING_BYTES
        || decimal_significant_digits(value)
            .is_some_and(|digits| digits > MAX_DECIMAL_SIGNIFICANT_DIGITS)
    {
        return Err(malformed_response());
    }
    Ok(())
}

fn decimal_significant_digits(value: &str) -> Option<usize> {
    let bytes = value.as_bytes();
    let mut index = usize::from(matches!(bytes.first(), Some(b'+') | Some(b'-')));
    let mantissa_start = index;
    let mut saw_digit = false;
    let mut saw_decimal_point = false;
    let mut significant_digits = 0;
    let mut saw_nonzero = false;

    while let Some(byte) = bytes.get(index) {
        match byte {
            b'0'..=b'9' => {
                saw_digit = true;
                if *byte != b'0' || saw_nonzero {
                    saw_nonzero = true;
                    significant_digits += 1;
                }
                index += 1;
            }
            b'.' if !saw_decimal_point => {
                saw_decimal_point = true;
                index += 1;
            }
            _ => break,
        }
    }

    if !saw_digit
        || index == mantissa_start
        || matches!(bytes.get(index.wrapping_sub(1)), Some(b'.'))
    {
        return None;
    }

    if matches!(bytes.get(index), Some(b'e') | Some(b'E')) {
        index += 1;
        if matches!(bytes.get(index), Some(b'+') | Some(b'-')) {
            index += 1;
        }
        let exponent_start = index;
        while matches!(bytes.get(index), Some(b'0'..=b'9')) {
            index += 1;
        }
        if index == exponent_start {
            return None;
        }
    }

    (index == bytes.len()).then_some(significant_digits.max(1))
}

fn malformed_response() -> BrokerageError {
    BrokerageError::new(BrokerageErrorKind::MalformedResponse)
}

/// Reqwest MCP backend that rejects oversized bodies before protocol decoding.
#[derive(Clone)]
pub struct BrokerMcpHttpClient {
    inner: reqwest::Client,
}

impl BrokerMcpHttpClient {
    pub fn new(inner: reqwest::Client) -> Self {
        Self { inner }
    }

    async fn send(
        &self,
        request: reqwest::RequestBuilder,
    ) -> Result<reqwest::Response, StreamableHttpError<io::Error>> {
        request
            .send()
            .await
            .map_err(|error| StreamableHttpError::Client(http_io_error(&error)))
    }

    fn apply_headers(
        &self,
        mut request: reqwest::RequestBuilder,
        custom_headers: MapHeaders,
    ) -> Result<reqwest::RequestBuilder, StreamableHttpError<io::Error>> {
        for (name, value) in custom_headers {
            if is_reserved_header(&name)
                && !name.as_str().eq_ignore_ascii_case(HEADER_PROTOCOL_VERSION)
            {
                return Err(StreamableHttpError::ReservedHeaderConflict(
                    name.to_string(),
                ));
            }
            request = request.header(name, value);
        }
        Ok(request)
    }

    fn authenticate(
        mut request: reqwest::RequestBuilder,
        auth_token: Option<String>,
    ) -> reqwest::RequestBuilder {
        if let Some(token) = auth_token {
            request = request.bearer_auth(token);
        }
        request
    }
}

impl Default for BrokerMcpHttpClient {
    fn default() -> Self {
        Self::new(reqwest::Client::new())
    }
}

type MapHeaders = std::collections::HashMap<HeaderName, HeaderValue>;

impl StreamableHttpClient for BrokerMcpHttpClient {
    type Error = io::Error;

    async fn post_message(
        &self,
        uri: Arc<str>,
        message: ClientJsonRpcMessage,
        session_id: Option<Arc<str>>,
        auth_token: Option<String>,
        custom_headers: MapHeaders,
    ) -> Result<StreamableHttpPostResponse, StreamableHttpError<Self::Error>> {
        self.post_message_with_max_sse_event_size(
            uri,
            message,
            session_id,
            auth_token,
            custom_headers,
            MAX_RAW_RESULT_BYTES,
        )
        .await
    }

    async fn post_message_with_max_sse_event_size(
        &self,
        uri: Arc<str>,
        message: ClientJsonRpcMessage,
        session_id: Option<Arc<str>>,
        auth_token: Option<String>,
        custom_headers: MapHeaders,
        max_response_bytes: usize,
    ) -> Result<StreamableHttpPostResponse, StreamableHttpError<Self::Error>> {
        let session_was_attached = session_id.is_some();
        let is_control = matches!(
            message,
            ClientJsonRpcMessage::Notification(_)
                | ClientJsonRpcMessage::Response(_)
                | ClientJsonRpcMessage::Error(_)
        );
        let mut request = self
            .inner
            .post(uri.as_ref())
            .header(ACCEPT, [EVENT_STREAM_MIME_TYPE, JSON_MIME_TYPE].join(", "));
        request = Self::authenticate(request, auth_token);
        request = self.apply_headers(request, custom_headers)?;
        if let Some(session_id) = session_id {
            request = request.header(HEADER_SESSION_ID, session_id.as_ref());
        }
        let response = self.send(request.json(&message)).await?;
        let status = response.status();
        if let Some(error) = authentication_response_error(&response)? {
            return Err(error);
        }
        if matches!(
            status,
            reqwest::StatusCode::ACCEPTED | reqwest::StatusCode::NO_CONTENT
        ) {
            return Ok(StreamableHttpPostResponse::Accepted);
        }
        if status == reqwest::StatusCode::NOT_FOUND && session_was_attached {
            return Err(StreamableHttpError::SessionExpired);
        }

        let content_type = response_content_type(&response);
        let content_length = response.content_length();
        let response_session_id = response_session_id(&response);
        if status.is_success() && content_length == Some(0) && is_control {
            return Ok(StreamableHttpPostResponse::Accepted);
        }
        if !status.is_success() {
            let body = read_bounded_body(response, max_response_bytes).await?;
            if content_type
                .as_deref()
                .is_some_and(|value| value.as_bytes().starts_with(JSON_MIME_TYPE.as_bytes()))
                && let Ok(message) = serde_json::from_slice::<ServerJsonRpcMessage>(&body)
                && matches!(message, JsonRpcMessage::Error(_))
            {
                return Ok(StreamableHttpPostResponse::Json(
                    message,
                    response_session_id,
                ));
            }
            if let Some(message) = legacy_discover_error(&message, session_was_attached, status) {
                return Ok(StreamableHttpPostResponse::Json(message, None));
            }
            return Err(StreamableHttpError::UnexpectedServerResponse(
                format!("HTTP {status}").into(),
            ));
        }
        if content_type.as_deref().is_some_and(|value| {
            value
                .as_bytes()
                .starts_with(EVENT_STREAM_MIME_TYPE.as_bytes())
        }) {
            return Ok(StreamableHttpPostResponse::Sse(
                bounded_sse_response(response, max_response_bytes),
                response_session_id,
            ));
        }

        let body = read_bounded_body(response, max_response_bytes).await?;
        if !content_type
            .as_deref()
            .is_some_and(|value| value.as_bytes().starts_with(JSON_MIME_TYPE.as_bytes()))
        {
            return Err(StreamableHttpError::UnexpectedContentType(content_type));
        }
        let message = serde_json::from_slice(&body)?;
        Ok(StreamableHttpPostResponse::Json(
            message,
            response_session_id,
        ))
    }

    async fn delete_session(
        &self,
        uri: Arc<str>,
        session_id: Arc<str>,
        auth_token: Option<String>,
        custom_headers: MapHeaders,
    ) -> Result<(), StreamableHttpError<Self::Error>> {
        let request = self.inner.delete(uri.as_ref());
        let mut request = Self::authenticate(request, auth_token);
        request = self.apply_headers(request, custom_headers)?;
        request = request.header(HEADER_SESSION_ID, session_id.as_ref());
        let response = self.send(request).await?;
        if response.status() == reqwest::StatusCode::METHOD_NOT_ALLOWED {
            return Ok(());
        }
        if let Some(error) = authentication_response_error(&response)? {
            return Err(error);
        }
        if !response.status().is_success() {
            return Err(StreamableHttpError::UnexpectedServerResponse(
                format!("HTTP {}", response.status()).into(),
            ));
        }
        Ok(())
    }

    async fn get_stream(
        &self,
        uri: Arc<str>,
        session_id: Option<Arc<str>>,
        last_event_id: Option<String>,
        auth_token: Option<String>,
        custom_headers: MapHeaders,
    ) -> Result<BoxStream<'static, Result<Sse, SseError>>, StreamableHttpError<Self::Error>> {
        self.get_stream_with_max_sse_event_size(
            uri,
            session_id,
            last_event_id,
            auth_token,
            custom_headers,
            MAX_RAW_RESULT_BYTES,
        )
        .await
    }

    async fn get_stream_with_max_sse_event_size(
        &self,
        uri: Arc<str>,
        session_id: Option<Arc<str>>,
        last_event_id: Option<String>,
        auth_token: Option<String>,
        custom_headers: MapHeaders,
        max_event_bytes: usize,
    ) -> Result<BoxStream<'static, Result<Sse, SseError>>, StreamableHttpError<Self::Error>> {
        let request = self
            .inner
            .get(uri.as_ref())
            .header(ACCEPT, [EVENT_STREAM_MIME_TYPE, JSON_MIME_TYPE].join(", "));
        let mut request = Self::authenticate(request, auth_token);
        request = self.apply_headers(request, custom_headers)?;
        if let Some(session_id) = session_id {
            request = request.header(HEADER_SESSION_ID, session_id.as_ref());
        }
        if let Some(last_event_id) = last_event_id {
            request = request.header(HEADER_LAST_EVENT_ID, last_event_id);
        }
        let response = self.send(request).await?;
        if response.status() == reqwest::StatusCode::METHOD_NOT_ALLOWED {
            return Err(StreamableHttpError::ServerDoesNotSupportSse);
        }
        if let Some(error) = authentication_response_error(&response)? {
            return Err(error);
        }
        if !response.status().is_success() {
            return Err(StreamableHttpError::UnexpectedServerResponse(
                format!("HTTP {}", response.status()).into(),
            ));
        }
        let content_type = response_content_type(&response);
        if !content_type.as_deref().is_some_and(|value| {
            value
                .as_bytes()
                .starts_with(EVENT_STREAM_MIME_TYPE.as_bytes())
                || value.as_bytes().starts_with(JSON_MIME_TYPE.as_bytes())
        }) {
            return Err(StreamableHttpError::UnexpectedContentType(content_type));
        }
        Ok(bounded_sse_response(response, max_event_bytes))
    }
}

fn is_reserved_header(name: &HeaderName) -> bool {
    [
        "accept",
        HEADER_SESSION_ID,
        HEADER_PROTOCOL_VERSION,
        HEADER_LAST_EVENT_ID,
    ]
    .iter()
    .any(|reserved| name.as_str().eq_ignore_ascii_case(reserved))
}

fn http_io_error(error: &reqwest::Error) -> io::Error {
    let kind = if error.is_timeout() {
        io::ErrorKind::TimedOut
    } else {
        io::ErrorKind::Other
    };
    io::Error::new(kind, "broker HTTP request failed")
}

fn authentication_response_error(
    response: &reqwest::Response,
) -> Result<Option<StreamableHttpError<io::Error>>, StreamableHttpError<io::Error>> {
    if !matches!(
        response.status(),
        reqwest::StatusCode::UNAUTHORIZED | reqwest::StatusCode::FORBIDDEN
    ) {
        return Ok(None);
    }
    let Some(header) = response.headers().get(WWW_AUTHENTICATE) else {
        return Ok(None);
    };
    let header = header
        .to_str()
        .map_err(|_| StreamableHttpError::UnexpectedServerResponse("invalid auth header".into()))?
        .to_string();
    if response.status() == reqwest::StatusCode::UNAUTHORIZED {
        Ok(Some(StreamableHttpError::AuthRequired(
            AuthRequiredError::new(header),
        )))
    } else {
        Ok(Some(StreamableHttpError::Client(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "broker rejected the requested scope",
        ))))
    }
}

fn response_content_type(response: &reqwest::Response) -> Option<String> {
    response
        .headers()
        .get(CONTENT_TYPE)
        .map(|value| String::from_utf8_lossy(value.as_bytes()).to_string())
}

fn response_session_id(response: &reqwest::Response) -> Option<String> {
    response
        .headers()
        .get(HEADER_SESSION_ID)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
}

async fn read_bounded_body(
    response: reqwest::Response,
    max_bytes: usize,
) -> Result<Vec<u8>, StreamableHttpError<io::Error>> {
    if response
        .content_length()
        .is_some_and(|length| length > max_bytes as u64)
    {
        return Err(response_too_large());
    }
    let mut bytes = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| StreamableHttpError::Client(http_io_error(&error)))?;
        if chunk.len() > max_bytes.saturating_sub(bytes.len()) {
            return Err(response_too_large());
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

fn response_too_large() -> StreamableHttpError<io::Error> {
    StreamableHttpError::Client(io::Error::new(
        io::ErrorKind::InvalidData,
        "broker response exceeds byte limit",
    ))
}

fn legacy_discover_error(
    message: &ClientJsonRpcMessage,
    session_was_attached: bool,
    status: reqwest::StatusCode,
) -> Option<ServerJsonRpcMessage> {
    if session_was_attached
        || !status.is_client_error()
        || matches!(
            status,
            reqwest::StatusCode::UNAUTHORIZED | reqwest::StatusCode::FORBIDDEN
        )
    {
        return None;
    }
    let ClientJsonRpcMessage::Request(request) = message else {
        return None;
    };
    if !matches!(request.request, ClientRequest::DiscoverRequest(_)) {
        return None;
    }
    Some(ServerJsonRpcMessage::error(
        ErrorData::invalid_request("server/discover rejected", None),
        Some(request.id.clone()),
    ))
}

fn bounded_sse_response(
    response: reqwest::Response,
    max_event_bytes: usize,
) -> BoxStream<'static, Result<Sse, SseError>> {
    let bytes = response
        .bytes_stream()
        .scan(SseByteLimit::new(max_event_bytes), |limit, item| {
            let next = if limit.failed {
                None
            } else {
                Some(match item {
                    Ok(chunk) => match limit.observe(&chunk) {
                        Ok(()) => Ok(chunk),
                        Err(()) => {
                            limit.failed = true;
                            Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "broker SSE event exceeds byte limit",
                            ))
                        }
                    },
                    Err(error) => {
                        limit.failed = true;
                        Err(http_io_error(&error))
                    }
                })
            };
            ready(next)
        });
    SseStream::from_bytes_stream(bytes).boxed()
}

struct SseByteLimit {
    max: usize,
    retained: usize,
    line: usize,
    line_is_comment: bool,
    previous_was_cr: bool,
    failed: bool,
}

impl SseByteLimit {
    fn new(max: usize) -> Self {
        Self {
            max,
            retained: 0,
            line: 0,
            line_is_comment: false,
            previous_was_cr: false,
            failed: false,
        }
    }

    fn observe(&mut self, chunk: &[u8]) -> Result<(), ()> {
        for byte in chunk {
            if self.previous_was_cr {
                self.previous_was_cr = false;
                if *byte == b'\n' {
                    continue;
                }
            }
            match byte {
                b'\r' => {
                    self.finish_line()?;
                    self.previous_was_cr = true;
                }
                b'\n' => self.finish_line()?,
                _ => {
                    if self.line == 0 {
                        self.line_is_comment = *byte == b':';
                    }
                    self.line = self.line.saturating_add(1);
                    self.check()?;
                }
            }
        }
        Ok(())
    }

    fn finish_line(&mut self) -> Result<(), ()> {
        if self.line == 0 {
            self.retained = 0;
        } else if !self.line_is_comment {
            self.retained = self.retained.saturating_add(self.line).saturating_add(1);
        }
        self.line = 0;
        self.line_is_comment = false;
        self.check()
    }

    fn check(&self) -> Result<(), ()> {
        if self.retained.saturating_add(self.line) > self.max {
            Err(())
        } else {
            Ok(())
        }
    }
}

/// Inputs required to establish one authenticated broker MCP session.
pub struct BrokerMcpConfig {
    brokerage: Brokerage,
    endpoint: Url,
    client: AuthClient<BrokerMcpHttpClient>,
}

impl BrokerMcpConfig {
    pub fn new(
        brokerage: Brokerage,
        endpoint: Url,
        client: AuthClient<BrokerMcpHttpClient>,
    ) -> Self {
        Self {
            brokerage,
            endpoint,
            client,
        }
    }
}

/// A live read-only MCP service whose tool names were resolved at connection time.
pub struct BrokerMcpSession {
    brokerage: Brokerage,
    service: RunningService<RoleClient, ()>,
    capabilities: ResolvedCapabilities,
}

impl BrokerMcpSession {
    pub async fn connect(config: BrokerMcpConfig) -> Result<Self, BrokerageError> {
        let transport_config =
            StreamableHttpClientTransportConfig::with_uri(config.endpoint.as_str())
                .max_concurrent_requests(4)
                .control_request_timeout(SESSION_TIMEOUT)
                .session_recovery_timeout(SESSION_TIMEOUT)
                .max_sse_event_size(MAX_RAW_RESULT_BYTES)
                .reinit_on_expired_session(true);
        let transport = StreamableHttpClientTransport::with_client(config.client, transport_config);
        let mut service = ().serve(transport).await.map_err(|_| unavailable())?;

        let tools = match service.list_tools(None).await {
            Ok(result) => result.tools,
            Err(error) => {
                let mapped = map_service_error(error);
                let _ = service.close().await;
                return Err(mapped);
            }
        };
        let capabilities = match resolve_capabilities(config.brokerage, &tools) {
            Ok(capabilities) => capabilities,
            Err(error) => {
                let _ = service.close().await;
                return Err(error);
            }
        };

        Ok(Self {
            brokerage: config.brokerage,
            service,
            capabilities,
        })
    }

    pub async fn call_read(
        &self,
        input: ReadCapabilityInput,
    ) -> Result<BoundedToolResult, BrokerageError> {
        let capability = input.capability();
        let name = self
            .capabilities
            .get(capability)
            .ok_or_else(incompatible_schema)?;
        let schema = capability_descriptor(self.brokerage, capability)
            .map(|descriptor| descriptor.schema)
            .ok_or_else(incompatible_schema)?;
        let arguments = input.into_arguments(schema)?;
        let mut request = CallToolRequestParams::new(Cow::Owned(name.to_string()));
        if let Some(arguments) = arguments {
            request = request.with_arguments(arguments);
        }
        let result = self
            .service
            .call_tool(request)
            .await
            .map_err(map_service_error)?;
        BoundedToolResult::from_call_result(result)
    }

    /// Cancel the rmcp service and await transport cleanup before returning.
    pub async fn close(self) -> Result<(), BrokerageError> {
        self.service.cancel().await.map_err(|_| unavailable())?;
        Ok(())
    }
}

fn map_service_error(error: ServiceError) -> BrokerageError {
    match error {
        ServiceError::Timeout { .. } => BrokerageError::new(BrokerageErrorKind::Timeout),
        ServiceError::TransportClosed | ServiceError::Cancelled { .. } => {
            BrokerageError::new(BrokerageErrorKind::Disconnected)
        }
        ServiceError::TransportSend(error) => {
            BrokerageError::new(classify_transport_error(error.error.as_ref()))
        }
        _ => unavailable(),
    }
}

fn classify_transport_error(error: &(dyn std::error::Error + 'static)) -> BrokerageErrorKind {
    let mut current = Some(error);
    while let Some(error) = current {
        if let Some(stream_error) = error.downcast_ref::<StreamableHttpError<io::Error>>() {
            match stream_error {
                StreamableHttpError::Client(error) => match error.kind() {
                    io::ErrorKind::InvalidData => return BrokerageErrorKind::MalformedResponse,
                    io::ErrorKind::TimedOut => return BrokerageErrorKind::Timeout,
                    io::ErrorKind::PermissionDenied => return BrokerageErrorKind::Permission,
                    _ => {}
                },
                StreamableHttpError::AuthRequired(_) => {
                    return BrokerageErrorKind::AuthorizationRequired;
                }
                _ => {}
            }
        }
        current = error.source();
    }
    BrokerageErrorKind::Unavailable
}

fn unavailable() -> BrokerageError {
    BrokerageError::new(BrokerageErrorKind::Unavailable)
}

#[cfg(test)]
mod tests {
    use rmcp::model::{CallToolResult, ContentBlock};
    use serde_json::json;

    use super::{BoundedToolResult, BrokerageErrorKind};

    #[test]
    fn tool_error_content_becomes_a_sanitized_provider_failure() {
        let private_provider_message = "private-provider-error-4281";
        let result = CallToolResult::error(vec![ContentBlock::text(private_provider_message)]);

        let error = BoundedToolResult::from_call_result(result).expect_err("provider failure");

        assert_eq!(error.kind, BrokerageErrorKind::Unavailable);
        assert!(!error.to_string().contains(private_provider_message));
    }

    #[test]
    fn structured_tool_content_is_retained_after_bounds_validation() {
        let result = CallToolResult::structured(json!({
            "positions": [{ "quantity": "12.345678" }]
        }));

        let bounded = BoundedToolResult::from_call_result(result).expect("bounded result");

        assert_eq!(
            bounded.value(),
            &json!({ "positions": [{ "quantity": "12.345678" }] })
        );
    }
}
