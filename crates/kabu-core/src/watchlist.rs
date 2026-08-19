use crate::{Instrument, InstrumentId};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct Watchlist {
    instruments: Vec<Instrument>,
    selected_index: Option<usize>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AddOutcome {
    Added { index: usize },
    AlreadyPresent { index: usize },
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum WatchlistError {
    #[error("selection index {index} is outside a watchlist of length {len}")]
    SelectionOutOfBounds { index: usize, len: usize },
}

impl Watchlist {
    pub fn starter() -> Self {
        Self {
            instruments: vec![
                Instrument::equity("AAPL", Some("NASDAQ"), "Apple", "USD").expect("valid starter"),
                Instrument::equity("NVDA", Some("NASDAQ"), "NVIDIA", "USD").expect("valid starter"),
                Instrument::crypto("BTC", "USDT", "Bitcoin").expect("valid starter"),
                Instrument::crypto("ETH", "USDT", "Ethereum").expect("valid starter"),
            ],
            selected_index: Some(0),
        }
    }

    pub fn instruments(&self) -> &[Instrument] {
        &self.instruments
    }

    pub fn len(&self) -> usize {
        self.instruments.len()
    }

    pub fn is_empty(&self) -> bool {
        self.instruments.is_empty()
    }

    pub fn selected_index(&self) -> Option<usize> {
        self.selected_index
    }

    pub fn selected(&self) -> Option<&Instrument> {
        self.selected_index
            .and_then(|index| self.instruments.get(index))
    }

    pub fn add(&mut self, instrument: Instrument) -> AddOutcome {
        if let Some(index) = self
            .instruments
            .iter()
            .position(|existing| existing.id == instrument.id)
        {
            return AddOutcome::AlreadyPresent { index };
        }

        let index = self.instruments.len();
        self.instruments.push(instrument);
        if self.selected_index.is_none() {
            self.selected_index = Some(index);
        }
        AddOutcome::Added { index }
    }

    pub fn remove(&mut self, id: &InstrumentId) -> Option<Instrument> {
        let index = self
            .instruments
            .iter()
            .position(|instrument| &instrument.id == id)?;
        let removed = self.instruments.remove(index);
        self.selected_index =
            (!self.instruments.is_empty()).then(|| index.min(self.instruments.len() - 1));
        Some(removed)
    }

    pub fn select(&mut self, index: usize) -> Result<(), WatchlistError> {
        if index >= self.instruments.len() {
            return Err(WatchlistError::SelectionOutOfBounds {
                index,
                len: self.instruments.len(),
            });
        }
        self.selected_index = Some(index);
        Ok(())
    }

    pub fn select_next(&mut self) -> Option<usize> {
        let index = self.selected_index.map_or(0, |index| {
            (index + 1).min(self.instruments.len().saturating_sub(1))
        });
        if self.instruments.is_empty() {
            self.selected_index = None;
            None
        } else {
            self.selected_index = Some(index);
            Some(index)
        }
    }

    pub fn select_previous(&mut self) -> Option<usize> {
        let index = self.selected_index.unwrap_or(0).saturating_sub(1);
        if self.instruments.is_empty() {
            self.selected_index = None;
            None
        } else {
            self.selected_index = Some(index);
            Some(index)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{AddOutcome, Watchlist};
    use crate::{Instrument, InstrumentId};

    #[test]
    fn add_is_ordered_and_duplicate_safe() {
        let mut list = Watchlist::default();
        let apple = Instrument::equity("AAPL", Some("NASDAQ"), "Apple", "USD").expect("instrument");
        assert_eq!(list.add(apple.clone()), AddOutcome::Added { index: 0 });
        assert_eq!(list.add(apple), AddOutcome::AlreadyPresent { index: 0 });
        assert_eq!(list.len(), 1);
    }

    #[test]
    fn removal_selects_the_nearest_remaining_row() {
        let mut list = Watchlist::starter();
        let removed = list.remove(&InstrumentId::equity("NVDA", Some("NASDAQ")).expect("id"));
        assert!(removed.is_some());
        assert_eq!(list.selected_index(), Some(1));
    }

    #[test]
    fn selection_navigation_stops_at_the_list_boundaries() {
        let mut list = Watchlist::starter();
        list.select(0).expect("first row");
        assert_eq!(list.select_previous(), Some(0));
        assert_eq!(list.select_next(), Some(1));
        assert_eq!(list.select_next(), Some(2));
        assert_eq!(list.select_next(), Some(3));
        assert_eq!(list.select_next(), Some(3));
    }

    #[test]
    fn serde_round_trip_preserves_order_and_selection() {
        let mut list = Watchlist::starter();
        list.select(2).expect("crypto row");
        let restored: Watchlist =
            serde_json::from_str(&serde_json::to_string(&list).expect("json"))
                .expect("restored watchlist");
        assert_eq!(restored.selected_index(), Some(2));
        assert_eq!(
            restored
                .instruments()
                .iter()
                .map(|instrument| instrument.id.canonical_symbol())
                .collect::<Vec<_>>(),
            vec!["AAPL", "NVDA", "BTC/USDT", "ETH/USDT"]
        );
    }
}
