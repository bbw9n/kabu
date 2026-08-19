use crate::{DataSource, Instrument, InstrumentId};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SearchResult {
    pub instrument: Instrument,
    pub source: DataSource,
    pub venue_label: String,
}

pub fn rank_search_results(query: &str, results: Vec<SearchResult>) -> Vec<SearchResult> {
    let mut unique = Vec::new();
    for result in results {
        if let Some(index) = unique
            .iter()
            .position(|existing: &SearchResult| existing.instrument.id == result.instrument.id)
        {
            if prefers_source(&result, &unique[index]) {
                unique[index] = result;
            }
        } else {
            unique.push(result);
        }
    }

    let query = query.trim().to_ascii_uppercase();
    unique.sort_by(|left, right| {
        search_rank(&query, &left.instrument)
            .cmp(&search_rank(&query, &right.instrument))
            .then_with(|| {
                left.instrument
                    .id
                    .canonical_symbol()
                    .cmp(&right.instrument.id.canonical_symbol())
            })
    });
    unique
}

fn prefers_source(candidate: &SearchResult, current: &SearchResult) -> bool {
    match candidate.instrument.id {
        InstrumentId::Crypto { .. } => {
            candidate.source == DataSource::Binance && current.source != DataSource::Binance
        }
        InstrumentId::Equity { .. } => {
            candidate.source == DataSource::Yahoo && current.source != DataSource::Yahoo
        }
    }
}

fn search_rank(query: &str, instrument: &Instrument) -> u8 {
    let symbol = instrument.id.canonical_symbol();
    let name = instrument.display_name.to_ascii_uppercase();

    if symbol == query {
        0
    } else if matches!(&instrument.id, InstrumentId::Equity { symbol, .. } if symbol == query) {
        1
    } else if matches!(&instrument.id, InstrumentId::Crypto { base, quote } if base == query || quote == query)
    {
        2
    } else if symbol.starts_with(query) {
        3
    } else if name.starts_with(query) {
        4
    } else if name.contains(query) {
        5
    } else {
        6
    }
}

#[cfg(test)]
mod tests {
    use super::{SearchResult, rank_search_results};
    use crate::{DataSource, Instrument};

    #[test]
    fn exact_crypto_pair_beats_company_name_matches() {
        let ranked =
            rank_search_results("btc/usdt", vec![apple_btc_name(), btc_usdt(), btc_usdc()]);
        assert_eq!(ranked[0].instrument.id.canonical_symbol(), "BTC/USDT");
    }

    #[test]
    fn results_are_deduplicated_by_canonical_identity() {
        let ranked = rank_search_results("nvda", vec![yahoo_nvda(), duplicate_nvda()]);
        assert_eq!(ranked.len(), 1);
        assert_eq!(ranked[0].source, DataSource::Yahoo);
    }

    #[test]
    fn exact_equity_ticker_beats_prefix_symbol() {
        let ranked = rank_search_results(
            "nvda",
            vec![
                result(
                    Instrument::equity("NVDAX", None, "NVDAX Fund", "USD").expect("instrument"),
                    DataSource::Yahoo,
                ),
                yahoo_nvda(),
            ],
        );
        assert_eq!(ranked[0].instrument.id.canonical_symbol(), "NVDA");
    }

    #[test]
    fn crypto_duplicates_prefer_binance() {
        let instrument = Instrument::crypto("BTC", "USDT", "Bitcoin Tether").expect("instrument");
        let ranked = rank_search_results(
            "btc",
            vec![
                result(instrument.clone(), DataSource::Yahoo),
                result(instrument, DataSource::Binance),
            ],
        );
        assert_eq!(ranked.len(), 1);
        assert_eq!(ranked[0].source, DataSource::Binance);
    }

    #[test]
    fn equal_rank_results_use_canonical_symbol_order() {
        let ranked = rank_search_results(
            "ac",
            vec![
                result(
                    Instrument::equity("ACME", None, "Acme", "USD").expect("instrument"),
                    DataSource::Yahoo,
                ),
                result(
                    Instrument::equity("ACAD", None, "Acadia", "USD").expect("instrument"),
                    DataSource::Yahoo,
                ),
            ],
        );
        assert_eq!(
            ranked
                .iter()
                .map(|entry| entry.instrument.id.canonical_symbol())
                .collect::<Vec<_>>(),
            vec!["ACAD", "ACME"]
        );
    }

    fn apple_btc_name() -> SearchResult {
        result(
            Instrument::equity("AAPL", Some("NASDAQ"), "BTC Holdings Apple", "USD")
                .expect("instrument"),
            DataSource::Yahoo,
        )
    }

    fn btc_usdt() -> SearchResult {
        result(
            Instrument::crypto("BTC", "USDT", "Bitcoin Tether").expect("instrument"),
            DataSource::Binance,
        )
    }

    fn btc_usdc() -> SearchResult {
        result(
            Instrument::crypto("BTC", "USDC", "Bitcoin USD Coin").expect("instrument"),
            DataSource::Binance,
        )
    }

    fn yahoo_nvda() -> SearchResult {
        result(
            Instrument::equity("NVDA", Some("NASDAQ"), "NVIDIA Corporation", "USD")
                .expect("instrument"),
            DataSource::Yahoo,
        )
    }

    fn duplicate_nvda() -> SearchResult {
        result(
            Instrument::equity("NVDA", Some("NMS"), "NVIDIA Corporation", "USD")
                .expect("instrument"),
            DataSource::Binance,
        )
    }

    fn result(instrument: Instrument, source: DataSource) -> SearchResult {
        SearchResult {
            instrument,
            source,
            venue_label: "fixture".into(),
        }
    }
}
