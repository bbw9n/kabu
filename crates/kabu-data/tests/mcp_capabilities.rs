use std::{
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use kabu_core::{Brokerage, BrokerageErrorKind};
use kabu_data::MacKeychainSecretStore;
use kabu_data::mcp::{
    BoundedToolResult, BrokerCredentialStore, BrokerMcpConfig, BrokerMcpHttpClient,
    BrokerMcpSession, BrokerOAuth, ReadCapability, ReadCapabilityInput, resolve_capabilities,
};
use rmcp::{
    ServiceExt,
    model::{ListToolsResult, Tool},
    transport::auth::{AuthClient, AuthorizationManager},
    transport::streamable_http_client::{
        StreamableHttpClientTransport, StreamableHttpClientTransportConfig,
    },
};
use serde_json::{Value, json};
use wiremock::{
    Mock, MockServer, Request, Respond, ResponseTemplate, matchers::path as request_path,
};

fn tool_fixture(path: &str) -> Vec<Tool> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(path);
    let fixture: Value = serde_json::from_slice(&std::fs::read(path).expect("read tool fixture"))
        .expect("valid tool fixture JSON");
    serde_json::from_value::<ListToolsResult>(fixture["result"].clone())
        .expect("complete tools/list result")
        .tools
}

fn replace_required_property(tools: &mut [Tool], tool_name: &str, property_name: &str) {
    let tool = tools
        .iter_mut()
        .find(|tool| tool.name == tool_name)
        .expect("fixture tool");
    let mut schema = (*tool.input_schema).clone();
    schema.insert(
        "properties".into(),
        json!({ property_name: { "type": "string" } }),
    );
    schema.insert("required".into(), json!([property_name]));
    tool.input_schema = schema.into();
}

#[derive(Clone, Copy)]
enum FakeCatalog {
    Compatible(Brokerage),
    IncompatibleLongbridge,
    OversizedToolsList,
    OversizedToolResult,
}

#[derive(Clone, Copy)]
enum FakePayload {
    Normal,
    OversizedToolsList,
    OversizedToolResult,
}

struct FakeMcpState {
    tools_list_calls: AtomicUsize,
    tool_calls: AtomicUsize,
    delete_calls: AtomicUsize,
    last_tool_params: Mutex<Option<Value>>,
}

impl FakeMcpState {
    fn new() -> Self {
        Self {
            tools_list_calls: AtomicUsize::new(0),
            tool_calls: AtomicUsize::new(0),
            delete_calls: AtomicUsize::new(0),
            last_tool_params: Mutex::new(None),
        }
    }
}

#[derive(Clone)]
struct FakeMcpResponder {
    state: Arc<FakeMcpState>,
    tools_result: Value,
    delete_delay: Duration,
    payload: FakePayload,
}

impl Respond for FakeMcpResponder {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        if request.method.as_str() == "DELETE" {
            self.state.delete_calls.fetch_add(1, Ordering::SeqCst);
            return ResponseTemplate::new(200).set_delay(self.delete_delay);
        }
        if request.method.as_str() == "GET" {
            return ResponseTemplate::new(405);
        }

        let message: Value = serde_json::from_slice(&request.body).expect("fake MCP request JSON");
        let method = message["method"].as_str().unwrap_or_default();
        let id = message.get("id").cloned().unwrap_or(Value::Null);
        match method {
            "server/discover" => json_rpc_error(id, -32601, "method not found"),
            "initialize" => ResponseTemplate::new(200)
                .insert_header("content-type", "application/json")
                .insert_header("mcp-session-id", "kabu-test-session")
                .set_body_json(json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": {
                        "protocolVersion": "2025-06-18",
                        "capabilities": { "tools": { "listChanged": false } },
                        "serverInfo": { "name": "kabu-test", "version": "1.0.0" }
                    }
                })),
            "notifications/initialized" => ResponseTemplate::new(202),
            "tools/list" => {
                self.state.tools_list_calls.fetch_add(1, Ordering::SeqCst);
                let mut result = self.tools_result.clone();
                if matches!(self.payload, FakePayload::OversizedToolsList) {
                    result["ignoredPadding"] = Value::String("x".repeat(1_048_577));
                }
                json_rpc_result(id, result)
            }
            "tools/call" => {
                self.state.tool_calls.fetch_add(1, Ordering::SeqCst);
                *self
                    .state
                    .last_tool_params
                    .lock()
                    .expect("tool params lock") = message.get("params").cloned();
                let mut result = json!({
                    "content": [{ "type": "text", "text": "{\"ok\":true}" }],
                    "structuredContent": { "ok": true },
                    "isError": false
                });
                if matches!(self.payload, FakePayload::OversizedToolResult) {
                    result["ignoredPadding"] = Value::String("x".repeat(1_048_577));
                }
                json_rpc_result(id, result)
            }
            _ => ResponseTemplate::new(202),
        }
    }
}

fn json_rpc_result(id: Value, result: Value) -> ResponseTemplate {
    ResponseTemplate::new(200)
        .insert_header("content-type", "application/json")
        .set_body_json(json!({ "jsonrpc": "2.0", "id": id, "result": result }))
}

fn json_rpc_error(id: Value, code: i64, message: &str) -> ResponseTemplate {
    ResponseTemplate::new(200)
        .insert_header("content-type", "application/json")
        .set_body_json(json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": { "code": code, "message": message }
        }))
}

async fn fake_mcp(catalog: FakeCatalog, delete_delay: Duration) -> (MockServer, Arc<FakeMcpState>) {
    let server = MockServer::start().await;
    let state = Arc::new(FakeMcpState::new());
    let brokerage = match catalog {
        FakeCatalog::Compatible(brokerage) => brokerage,
        FakeCatalog::IncompatibleLongbridge
        | FakeCatalog::OversizedToolsList
        | FakeCatalog::OversizedToolResult => Brokerage::Longbridge,
    };
    let payload = match catalog {
        FakeCatalog::OversizedToolsList => FakePayload::OversizedToolsList,
        FakeCatalog::OversizedToolResult => FakePayload::OversizedToolResult,
        FakeCatalog::Compatible(_) | FakeCatalog::IncompatibleLongbridge => FakePayload::Normal,
    };
    let fixture_path = match brokerage {
        Brokerage::Robinhood => "robinhood/tools.json",
        Brokerage::Longbridge => "longbridge/tools.json",
    };
    let mut tools_result = tool_fixture_value(fixture_path)["result"].clone();
    if matches!(catalog, FakeCatalog::IncompatibleLongbridge) {
        let tools = tools_result["tools"]
            .as_array_mut()
            .expect("fixture tools array");
        let positions = tools
            .iter_mut()
            .find(|tool| tool["name"] == "stock_positions")
            .expect("stock_positions fixture");
        positions["inputSchema"] = json!({
            "type": "object",
            "properties": { "order_id": { "type": "string" } },
            "required": ["order_id"]
        });
    }
    Mock::given(request_path("/mcp"))
        .respond_with(FakeMcpResponder {
            state: state.clone(),
            tools_result,
            delete_delay,
            payload,
        })
        .mount(&server)
        .await;
    (server, state)
}

fn tool_fixture_value(path: &str) -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(path);
    serde_json::from_slice(&std::fs::read(path).expect("read tool fixture"))
        .expect("valid tool fixture JSON")
}

async fn fake_config(brokerage: Brokerage, server: &MockServer) -> BrokerMcpConfig {
    let endpoint = url::Url::parse(&format!("{}/mcp", server.uri())).expect("fake endpoint");
    let manager = AuthorizationManager::new(endpoint.clone())
        .await
        .expect("authorization manager");
    let client = AuthClient::new(BrokerMcpHttpClient::default(), manager);
    BrokerMcpConfig::new(brokerage, endpoint, client)
}

#[tokio::test]
async fn typed_capability_input_serializes_only_descriptor_arguments() {
    let (server, state) = fake_mcp(
        FakeCatalog::Compatible(Brokerage::Longbridge),
        Duration::ZERO,
    )
    .await;
    let session = BrokerMcpSession::connect(fake_config(Brokerage::Longbridge, &server).await)
        .await
        .expect("connect");

    session
        .call_read(ReadCapabilityInput::Accounts {
            currency: Some("USD".to_string()),
        })
        .await
        .expect("account read");

    let mut params = state
        .last_tool_params
        .lock()
        .expect("tool params lock")
        .clone()
        .expect("captured tool params");
    params
        .as_object_mut()
        .expect("tool params object")
        .remove("_meta");
    assert_eq!(
        params,
        json!({
            "name": "account_balance",
            "arguments": { "currency": "USD" }
        })
    );
    session.close().await.expect("close");
}

#[tokio::test]
async fn descriptor_rejects_typed_arguments_before_tool_invocation() {
    let (server, state) = fake_mcp(
        FakeCatalog::Compatible(Brokerage::Robinhood),
        Duration::ZERO,
    )
    .await;
    let session = BrokerMcpSession::connect(fake_config(Brokerage::Robinhood, &server).await)
        .await
        .expect("connect");

    let error = session
        .call_read(ReadCapabilityInput::Accounts {
            currency: Some("USD".to_string()),
        })
        .await
        .expect_err("Robinhood account input is empty");

    assert_eq!(error.kind, BrokerageErrorKind::IncompatibleToolSchema);
    assert_eq!(state.tool_calls.load(Ordering::SeqCst), 0);
    session.close().await.expect("close");
}

#[tokio::test]
async fn connected_session_lists_tools_only_once_across_reads() {
    let (server, state) = fake_mcp(
        FakeCatalog::Compatible(Brokerage::Longbridge),
        Duration::ZERO,
    )
    .await;
    let session = BrokerMcpSession::connect(fake_config(Brokerage::Longbridge, &server).await)
        .await
        .expect("connect");

    session
        .call_read(ReadCapabilityInput::Accounts { currency: None })
        .await
        .expect("accounts");
    session
        .call_read(ReadCapabilityInput::EquityPositions)
        .await
        .expect("positions");

    assert_eq!(state.tools_list_calls.load(Ordering::SeqCst), 1);
    session.close().await.expect("close");
}

#[tokio::test]
async fn schema_failure_awaits_remote_session_cleanup() {
    let close_delay = Duration::from_millis(100);
    let (server, state) = fake_mcp(FakeCatalog::IncompatibleLongbridge, close_delay).await;
    let started = Instant::now();

    let error =
        match BrokerMcpSession::connect(fake_config(Brokerage::Longbridge, &server).await).await {
            Ok(session) => {
                session.close().await.expect("close unexpected session");
                panic!("schema failure unexpectedly connected");
            }
            Err(error) => error,
        };

    assert_eq!(error.kind, BrokerageErrorKind::IncompatibleToolSchema);
    assert_eq!(state.delete_calls.load(Ordering::SeqCst), 1);
    assert!(started.elapsed() >= close_delay);
}

#[tokio::test]
async fn explicit_close_awaits_remote_session_cleanup() {
    let close_delay = Duration::from_millis(100);
    let (server, state) =
        fake_mcp(FakeCatalog::Compatible(Brokerage::Longbridge), close_delay).await;
    let session = BrokerMcpSession::connect(fake_config(Brokerage::Longbridge, &server).await)
        .await
        .expect("connect");
    let started = Instant::now();

    session.close().await.expect("close");

    assert_eq!(state.delete_calls.load(Ordering::SeqCst), 1);
    assert!(started.elapsed() >= close_delay);
}

#[tokio::test]
async fn oversized_direct_json_tools_list_is_rejected_before_catalog_deserialization() {
    let (server, state) = fake_mcp(FakeCatalog::OversizedToolsList, Duration::ZERO).await;

    let error =
        match BrokerMcpSession::connect(fake_config(Brokerage::Longbridge, &server).await).await {
            Ok(session) => {
                session.close().await.expect("close unexpected session");
                panic!("oversized tools/list unexpectedly connected");
            }
            Err(error) => error,
        };

    assert_eq!(error.kind, BrokerageErrorKind::MalformedResponse);
    assert_eq!(state.tools_list_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn oversized_direct_json_tool_result_is_rejected_before_result_deserialization() {
    let (server, state) = fake_mcp(FakeCatalog::OversizedToolResult, Duration::ZERO).await;
    let session = BrokerMcpSession::connect(fake_config(Brokerage::Longbridge, &server).await)
        .await
        .expect("connect");

    let error = session
        .call_read(ReadCapabilityInput::Accounts { currency: None })
        .await
        .expect_err("oversized direct JSON");

    assert_eq!(error.kind, BrokerageErrorKind::MalformedResponse);
    assert_eq!(state.tool_calls.load(Ordering::SeqCst), 1);
    session.close().await.expect("close");
}

#[test]
fn longbridge_resolves_only_exact_read_tools() {
    let tools = tool_fixture("longbridge/tools.json");
    let resolved = resolve_capabilities(Brokerage::Longbridge, &tools).expect("compatible");

    assert_eq!(resolved.name(ReadCapability::Accounts), "account_balance");
    assert_eq!(
        resolved.name(ReadCapability::EquityPositions),
        "stock_positions"
    );
    assert!(!resolved.names().any(|name| name.contains("submit_order")));
    assert!(!resolved.names().any(|name| name.contains("cancel_order")));
}

#[test]
fn robinhood_resolves_the_conservative_read_set() {
    let tools = tool_fixture("robinhood/tools.json");
    let resolved = resolve_capabilities(Brokerage::Robinhood, &tools).expect("compatible");

    assert_eq!(resolved.name(ReadCapability::Accounts), "get_accounts");
    assert_eq!(
        resolved.name(ReadCapability::PortfolioSummary),
        "get_portfolio"
    );
    assert_eq!(
        resolved.name(ReadCapability::EquityPositions),
        "get_equity_positions"
    );
    assert_eq!(
        resolved.name(ReadCapability::CryptoPositions),
        "get_crypto_positions"
    );
    assert_eq!(resolved.names().count(), 4);
}

#[test]
fn robinhood_resolves_documented_alternate_candidate_names() {
    let mut tools = tool_fixture("robinhood/tools.json");
    let replacements = [
        ("get_accounts", "list_accounts"),
        ("get_portfolio", "get_portfolio_summary"),
        ("get_equity_positions", "list_equity_positions"),
        ("get_crypto_positions", "list_crypto_positions"),
    ];
    for (primary, alternate) in replacements {
        tools
            .iter_mut()
            .find(|tool| tool.name == primary)
            .expect("primary candidate")
            .name = alternate.to_owned().into();
    }

    let resolved = resolve_capabilities(Brokerage::Robinhood, &tools).expect("alternate set");
    assert_eq!(resolved.name(ReadCapability::Accounts), "list_accounts");
    assert_eq!(
        resolved.name(ReadCapability::PortfolioSummary),
        "get_portfolio_summary"
    );
    assert_eq!(
        resolved.name(ReadCapability::EquityPositions),
        "list_equity_positions"
    );
    assert_eq!(
        resolved.name(ReadCapability::CryptoPositions),
        "list_crypto_positions"
    );
}

#[test]
fn changed_required_input_schema_fails_closed() {
    let mut tools = tool_fixture("robinhood/tools.json");
    replace_required_property(&mut tools, "get_accounts", "order_id");

    let error = resolve_capabilities(Brokerage::Robinhood, &tools).expect_err("schema drift");
    assert_eq!(error.kind, BrokerageErrorKind::IncompatibleToolSchema);
}

#[test]
fn recognized_name_with_write_suffix_never_resolves() {
    let mut tools = tool_fixture("robinhood/tools.json");
    let account = tools
        .iter_mut()
        .find(|tool| tool.name == "get_accounts")
        .expect("accounts tool");
    account.name = "get_accounts_order".into();

    let error = resolve_capabilities(Brokerage::Robinhood, &tools).expect_err("write-like name");
    assert_eq!(error.kind, BrokerageErrorKind::IncompatibleToolSchema);
}

#[test]
fn optional_schema_descriptions_do_not_cause_drift() {
    let mut tools = tool_fixture("robinhood/tools.json");
    let account = tools
        .iter_mut()
        .find(|tool| tool.name == "get_accounts")
        .expect("accounts tool");
    let mut schema = (*account.input_schema).clone();
    schema.insert("description".into(), json!("Current account records."));
    account.input_schema = schema.into();

    let resolved = resolve_capabilities(Brokerage::Robinhood, &tools).expect("descriptive drift");
    assert_eq!(resolved.name(ReadCapability::Accounts), "get_accounts");
}

#[test]
fn additional_functional_schema_constraint_fails_closed() {
    let mut tools = tool_fixture("longbridge/tools.json");
    let account = tools
        .iter_mut()
        .find(|tool| tool.name == "account_balance")
        .expect("accounts tool");
    let mut schema = (*account.input_schema).clone();
    schema.insert("additionalProperties".into(), json!(false));
    account.input_schema = schema.into();

    let error = resolve_capabilities(Brokerage::Longbridge, &tools).expect_err("schema drift");
    assert_eq!(error.kind, BrokerageErrorKind::IncompatibleToolSchema);
}

#[test]
fn input_property_named_description_is_not_mistaken_for_schema_metadata() {
    let mut tools = tool_fixture("robinhood/tools.json");
    let account = tools
        .iter_mut()
        .find(|tool| tool.name == "get_accounts")
        .expect("accounts tool");
    let mut schema = (*account.input_schema).clone();
    schema.insert(
        "properties".into(),
        json!({ "description": { "type": "string" } }),
    );
    account.input_schema = schema.into();

    let error = resolve_capabilities(Brokerage::Robinhood, &tools).expect_err("input drift");
    assert_eq!(error.kind, BrokerageErrorKind::IncompatibleToolSchema);
}

#[test]
fn missing_required_read_tool_fails_closed() {
    let mut tools = tool_fixture("longbridge/tools.json");
    tools.retain(|tool| tool.name != "stock_positions");

    let error = resolve_capabilities(Brokerage::Longbridge, &tools).expect_err("missing read");
    assert_eq!(error.kind, BrokerageErrorKind::IncompatibleToolSchema);
}

#[test]
fn result_over_one_mebibyte_is_rejected_before_deserialization() {
    let payload = vec![b'x'; 1_048_577];

    assert_eq!(
        BoundedToolResult::parse(&payload)
            .expect_err("oversized")
            .kind,
        BrokerageErrorKind::MalformedResponse
    );
}

#[test]
fn result_array_over_two_thousand_items_is_rejected() {
    let payload = serde_json::to_vec(&vec![0_u8; 2_001]).expect("serialize payload");

    assert_eq!(
        BoundedToolResult::parse(&payload)
            .expect_err("oversized array")
            .kind,
        BrokerageErrorKind::MalformedResponse
    );
}

#[test]
fn result_string_over_sixteen_kibibytes_is_rejected() {
    let payload = serde_json::to_vec(&"x".repeat(16 * 1_024 + 1)).expect("serialize payload");

    assert_eq!(
        BoundedToolResult::parse(&payload)
            .expect_err("oversized string")
            .kind,
        BrokerageErrorKind::MalformedResponse
    );
}

#[test]
fn result_nesting_over_thirty_two_levels_is_rejected() {
    let mut value = json!(null);
    for _ in 0..33 {
        value = json!([value]);
    }
    let payload = serde_json::to_vec(&value).expect("serialize payload");

    assert_eq!(
        BoundedToolResult::parse(&payload)
            .expect_err("excessive nesting")
            .kind,
        BrokerageErrorKind::MalformedResponse
    );
}

#[test]
fn decimal_text_over_thirty_eight_significant_digits_is_rejected() {
    let payload =
        serde_json::to_vec("123456789012345678901234567890123456789").expect("serialize payload");

    assert_eq!(
        BoundedToolResult::parse(&payload)
            .expect_err("excessive precision")
            .kind,
        BrokerageErrorKind::MalformedResponse
    );
}

#[test]
fn bounded_result_retains_valid_json_for_normalization() {
    let payload = br#"{"positions":[{"quantity":"12.345678"}]}"#;
    let result = BoundedToolResult::parse(payload).expect("bounded result");

    assert_eq!(
        result.value(),
        &json!({ "positions": [{ "quantity": "12.345678" }] })
    );
}

#[tokio::test]
#[ignore = "requires an authorized Robinhood account; prints only names and input schemas"]
async fn capture_live_robinhood_tool_names_and_schemas() {
    let endpoint = url::Url::parse("https://agent.robinhood.com/mcp/trading")
        .expect("official Robinhood endpoint");
    let credentials = Arc::new(BrokerCredentialStore::new(
        Brokerage::Robinhood,
        Arc::new(MacKeychainSecretStore),
    ));
    let oauth = BrokerOAuth::new(Brokerage::Robinhood, endpoint.clone(), credentials);
    let client = oauth
        .authorized_transport_client()
        .await
        .expect("stored authorization");
    let transport_config = StreamableHttpClientTransportConfig::with_uri(endpoint.as_str())
        .max_concurrent_requests(4)
        .control_request_timeout(Duration::from_secs(5))
        .session_recovery_timeout(Duration::from_secs(5))
        .max_sse_event_size(1_048_576)
        .reinit_on_expired_session(true);
    let transport = StreamableHttpClientTransport::with_client(client, transport_config);
    let mut service = ().serve(transport).await.expect("MCP initialize");
    let tools = service.list_tools(None).await.expect("tools/list");
    let capture: Vec<_> = tools
        .tools
        .iter()
        .map(|tool| {
            json!({
                "name": tool.name,
                "inputSchema": tool.input_schema,
            })
        })
        .collect();

    println!(
        "{}",
        serde_json::to_string_pretty(&capture).expect("serialize sanitized tool capture")
    );
    service.close().await.expect("await MCP close");
}
