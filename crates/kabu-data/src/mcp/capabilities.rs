use std::{
    borrow::Cow,
    collections::{BTreeMap, HashSet},
    io::{self, Write},
    sync::Arc,
    time::Duration,
};

use kabu_core::{Brokerage, BrokerageError, BrokerageErrorKind};
use rmcp::{
    RoleClient, ServiceError, ServiceExt,
    model::{CallToolRequestParams, CallToolResult, Tool},
    service::RunningService,
    transport::{
        auth::AuthClient,
        streamable_http_client::{
            StreamableHttpClientTransport, StreamableHttpClientTransportConfig,
        },
    },
};
use serde_json::{Map, Value, json};
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

/// A compile-time role for one approved, read-only broker operation.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ReadCapability {
    Accounts,
    PortfolioSummary,
    EquityPositions,
    CryptoPositions,
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

/// Inputs required to establish one authenticated broker MCP session.
pub struct BrokerMcpConfig {
    brokerage: Brokerage,
    endpoint: Url,
    client: AuthClient<reqwest::Client>,
}

impl BrokerMcpConfig {
    pub fn new(brokerage: Brokerage, endpoint: Url, client: AuthClient<reqwest::Client>) -> Self {
        Self {
            brokerage,
            endpoint,
            client,
        }
    }
}

/// A live read-only MCP service whose tool names were resolved at connection time.
pub struct BrokerMcpSession {
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
            service,
            capabilities,
        })
    }

    pub async fn call_read(
        &self,
        capability: ReadCapability,
        arguments: Option<Map<String, Value>>,
    ) -> Result<BoundedToolResult, BrokerageError> {
        let name = self
            .capabilities
            .get(capability)
            .ok_or_else(incompatible_schema)?;
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
        _ => unavailable(),
    }
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
