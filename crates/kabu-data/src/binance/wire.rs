use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Deserialize)]
pub(super) struct ExchangeInfoResponse {
    pub symbols: Vec<ExchangeInfoSymbol>,
}

#[derive(Debug, Deserialize)]
pub(super) struct ExchangeInfoSymbol {
    pub symbol: String,
    pub status: String,
    #[serde(rename = "baseAsset")]
    pub base_asset: String,
    #[serde(rename = "baseAssetName")]
    pub base_asset_name: Option<String>,
    #[serde(rename = "quoteAsset")]
    pub quote_asset: String,
    #[serde(rename = "isSpotTradingAllowed")]
    pub is_spot_trading_allowed: bool,
}

#[derive(Debug, Deserialize)]
pub(super) struct Ticker24Hour {
    pub symbol: String,
    #[serde(rename = "lastPrice")]
    pub last_price: String,
    #[serde(rename = "prevClosePrice")]
    pub previous_close_price: String,
    #[serde(rename = "openPrice")]
    pub open_price: String,
    #[serde(rename = "highPrice")]
    pub high_price: String,
    #[serde(rename = "lowPrice")]
    pub low_price: String,
    pub volume: String,
    #[serde(rename = "closeTime")]
    pub close_time: i64,
}

#[derive(Debug, Deserialize)]
pub(super) struct Klines(pub Vec<Vec<Value>>);
