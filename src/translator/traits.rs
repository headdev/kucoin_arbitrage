use crate::model::orderbook::L2Orderbook;
use crate::model::symbol::SymbolInfo;
use crate::model::trade::TradeInfo;
use eyre::Result;

pub trait ToOrderBook {
    fn to_internal(&self) -> L2Orderbook;
}

pub trait ToOrderBookChange {
    fn to_internal(&self, serial: u64) -> (String, L2Orderbook);
}

pub trait ToSymbolInfo {
    fn to_internal(&self) -> SymbolInfo;
}

pub trait ToTradeInfo {
    fn to_internal(&self) -> Result<TradeInfo>;
}
