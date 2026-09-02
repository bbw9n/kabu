use std::{path::PathBuf, sync::Arc, time::Duration};

use kabu_core::{Brokerage, BrokerageErrorKind};
use kabu_data::MacKeychainSecretStore;
use kabu_data::mcp::{
    BoundedToolResult, BrokerCredentialStore, BrokerOAuth, ReadCapability, resolve_capabilities,
};
use rmcp::{
    ServiceExt,
    model::{ListToolsResult, Tool},
    transport::streamable_http_client::{
        StreamableHttpClientTransport, StreamableHttpClientTransportConfig,
    },
};
use serde_json::{Value, json};

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
