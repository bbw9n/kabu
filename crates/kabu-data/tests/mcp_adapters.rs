use std::str::FromStr;

use chrono::{DateTime, Utc};
use kabu_core::{BrokerageAssetKind, BrokerageErrorKind};
use kabu_data::{normalize_longbridge, normalize_robinhood};
use rust_decimal::Decimal;
use serde_json::{Value, json};

fn fixture(path: &str) -> Value {
    serde_json::from_str(
        &std::fs::read_to_string(format!(
            "{}/tests/fixtures/{path}",
            env!("CARGO_MANIFEST_DIR")
        ))
        .expect("fixture is readable"),
    )
    .expect("fixture is valid JSON")
}

fn fixed_now() -> DateTime<Utc> {
    "2026-09-02T01:02:03Z".parse().expect("fixed timestamp")
}

fn normalize_robinhood_fixture(equity_fixture: &str) -> kabu_core::PortfolioSnapshot {
    normalize_robinhood(
        fixture("robinhood/accounts.json"),
        fixture("robinhood/portfolio.json"),
        fixture(&format!("robinhood/{equity_fixture}")),
        fixture("robinhood/crypto_positions.json"),
        fixed_now(),
    )
    .expect("valid Robinhood snapshot")
}

#[test]
fn longbridge_position_preserves_decimal_and_normalizes_us_equity() {
    let snapshot = normalize_longbridge(
        fixture("longbridge/account_balance.json"),
        fixture("longbridge/stock_positions.json"),
        fixed_now(),
    )
    .expect("valid snapshot");

    let aapl = snapshot
        .positions
        .iter()
        .find(|position| position.native_symbol == "AAPL.US")
        .expect("AAPL");
    assert_eq!(
        aapl.quantity,
        Decimal::from_str("12.345678").expect("decimal")
    );
    assert_eq!(
        aapl.instrument
            .as_ref()
            .map(|instrument| instrument.id.canonical_symbol()),
        Some("AAPL".into())
    );
    assert_eq!(
        aapl.average_cost,
        Some(Decimal::from_str("192.123456789").expect("decimal"))
    );
}

#[test]
fn longbridge_uses_current_array_and_channel_envelopes() {
    let snapshot = normalize_longbridge(
        fixture("longbridge/account_balance.json"),
        fixture("longbridge/stock_positions.json"),
        fixed_now(),
    )
    .expect("official envelopes");

    assert_eq!(snapshot.accounts.len(), 1);
    assert_eq!(snapshot.positions.len(), 2);

    let wrapped_balance = json!({ "balances": fixture("longbridge/account_balance.json") });
    let error = normalize_longbridge(
        wrapped_balance,
        fixture("longbridge/stock_positions.json"),
        fixed_now(),
    )
    .expect_err("stale balance envelope");
    assert_eq!(error.kind, BrokerageErrorKind::MalformedResponse);
}

#[test]
fn null_missing_and_zero_money_values_stay_distinct() {
    let snapshot = normalize_robinhood_fixture("equity_positions.json");
    let account = snapshot.accounts.first().expect("account");

    assert_eq!(account.equity, None);
    assert_eq!(account.cash, Some(Decimal::ZERO));
    assert_eq!(account.buying_power, None);

    let aapl = snapshot
        .positions
        .iter()
        .find(|position| position.native_symbol == "AAPL")
        .expect("AAPL");
    assert_eq!(aapl.quantity, Decimal::ZERO);
    assert_eq!(aapl.average_cost, None);
    assert_eq!(aapl.market_value, None);
    assert_eq!(aapl.unrealized_pnl, Some(Decimal::ZERO));
}

#[test]
fn unknown_broker_instruments_are_retained_without_coercion() {
    let snapshot = normalize_robinhood_fixture("equity_positions_with_option.json");

    for kind in [
        BrokerageAssetKind::Option,
        BrokerageAssetKind::Fund,
        BrokerageAssetKind::Cash,
        BrokerageAssetKind::Other,
    ] {
        let position = snapshot
            .positions
            .iter()
            .find(|position| position.asset_kind == kind)
            .expect("unsupported position retained");
        assert_eq!(position.instrument, None);
        assert!(!position.native_symbol.is_empty());
    }
}

#[test]
fn crypto_is_normalized_only_with_explicit_base_and_quote() {
    let snapshot = normalize_robinhood_fixture("equity_positions.json");
    let btc = snapshot
        .positions
        .iter()
        .find(|position| position.native_symbol == "BTC-USD")
        .expect("BTC");
    assert_eq!(
        btc.instrument
            .as_ref()
            .map(|instrument| instrument.id.canonical_symbol()),
        Some("BTC/USD".into())
    );

    let unpaired = snapshot
        .positions
        .iter()
        .find(|position| position.native_symbol == "DOGE")
        .expect("unpaired crypto retained");
    assert_eq!(unpaired.asset_kind, BrokerageAssetKind::Crypto);
    assert_eq!(unpaired.instrument, None);
}

#[test]
fn empty_robinhood_accounts_produce_an_empty_snapshot() {
    let snapshot = normalize_robinhood(
        json!({ "results": [], "next": null, "previous": null }),
        json!({ "not": "a portfolio" }),
        json!({ "not": "positions" }),
        json!({ "not": "crypto positions" }),
        fixed_now(),
    )
    .expect("empty accounts are valid");

    assert!(snapshot.accounts.is_empty());
    assert!(snapshot.positions.is_empty());
    assert_eq!(snapshot.received_at, fixed_now());
}

#[test]
fn account_display_labels_never_expose_the_raw_identifier() {
    let snapshot = normalize_robinhood_fixture("equity_positions.json");
    let account = snapshot.accounts.first().expect("account");

    assert_eq!(account.display_label, "Robinhood •••7890");
    assert!(!account.display_label.contains("RH-ABC-1234567890"));
    assert!(!format!("{snapshot:?}").contains("RH-ABC-1234567890"));
}

#[test]
fn explicit_position_account_not_in_the_account_page_is_rejected() {
    let mut equities = fixture("robinhood/equity_positions.json");
    equities["results"][0]["account_number"] = json!("RH-STALE-0000");

    let error = normalize_robinhood(
        fixture("robinhood/accounts.json"),
        fixture("robinhood/portfolio.json"),
        equities,
        fixture("robinhood/crypto_positions.json"),
        fixed_now(),
    )
    .expect_err("unknown account must not be assigned to another account");

    assert_eq!(error.kind, BrokerageErrorKind::MalformedResponse);
}

#[test]
fn missing_position_account_is_rejected_when_the_account_page_is_ambiguous() {
    let mut accounts = fixture("robinhood/accounts.json");
    let mut second = accounts["results"][0].clone();
    second["account_number"] = json!("RH-SECOND-1234");
    accounts["results"]
        .as_array_mut()
        .expect("results array")
        .push(second);

    let mut equities = fixture("robinhood/equity_positions.json");
    equities["results"][0]["account_number"] = Value::Null;
    let error = normalize_robinhood(
        accounts,
        fixture("robinhood/portfolio.json"),
        equities,
        fixture("robinhood/crypto_positions.json"),
        fixed_now(),
    )
    .expect_err("ambiguous account assignment must fail");

    assert_eq!(error.kind, BrokerageErrorKind::MalformedResponse);
}

#[test]
fn missing_position_account_uses_the_only_available_account() {
    let mut equities = fixture("robinhood/equity_positions.json");
    equities["results"][0]["account_number"] = Value::Null;
    let snapshot = normalize_robinhood(
        fixture("robinhood/accounts.json"),
        fixture("robinhood/portfolio.json"),
        equities,
        fixture("robinhood/crypto_positions.json"),
        fixed_now(),
    )
    .expect("one account makes a missing position account unambiguous");

    assert_eq!(snapshot.positions[0].account_id, snapshot.accounts[0].id);
}

#[test]
fn malformed_decimal_is_a_typed_sanitized_failure() {
    let mut balances = fixture("longbridge/account_balance.json");
    balances[0]["total_cash"] = json!("NaN-secret");

    let error = normalize_longbridge(
        balances,
        fixture("longbridge/stock_positions.json"),
        fixed_now(),
    )
    .expect_err("invalid decimal");
    assert_eq!(error.kind, BrokerageErrorKind::MalformedResponse);
    assert!(!error.to_string().contains("NaN-secret"));
    assert!(!format!("{error:?}").contains("NaN-secret"));
}

#[test]
fn excessive_precision_and_exponent_overflow_are_rejected() {
    for malformed in [
        "123456789012345678901234567890123456789",
        "1e99999999999999999999999999999999999999",
    ] {
        let mut positions = fixture("longbridge/stock_positions.json");
        positions["list"][0]["stock_info"][0]["quantity"] = json!(malformed);
        let error = normalize_longbridge(
            fixture("longbridge/account_balance.json"),
            positions,
            fixed_now(),
        )
        .expect_err("unrepresentable decimal");
        assert_eq!(error.kind, BrokerageErrorKind::MalformedResponse);
        assert!(!error.to_string().contains(malformed));
    }
}

#[test]
fn unknown_fields_are_ignored_inside_records_but_not_in_required_envelopes() {
    let mut balances = fixture("longbridge/account_balance.json");
    balances[0]["future_optional_field"] = json!({ "ignored": true });
    normalize_longbridge(
        balances,
        fixture("longbridge/stock_positions.json"),
        fixed_now(),
    )
    .expect("optional record field ignored");

    let mut positions = fixture("longbridge/stock_positions.json");
    positions["unknown_structural_sibling"] = json!([]);
    let error = normalize_longbridge(
        fixture("longbridge/account_balance.json"),
        positions,
        fixed_now(),
    )
    .expect_err("unknown structural envelope field");
    assert_eq!(error.kind, BrokerageErrorKind::MalformedResponse);
}
