use crate::model::orderbook::L2Orderbook;

/// public orderbook change received from exchange
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum OrderbookEvent {
    OrderbookReceived((String, L2Orderbook)),
    OrderbookChangeReceived((String, L2Orderbook)),
}
