use tracing;
use eyre::Result;
use tokio::sync::broadcast::{Receiver, Sender};
use crate::event::orderbook::OrderbookEvent;
use crate::model::orderbook::FullOrderbook;
use std::sync::Arc;
use tokio::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct SyncState {
    pub symbols_synced: AtomicUsize,
    pub total_symbols: usize,
}

impl SyncState {
    pub fn new(total_symbols: usize) -> Self {
        Self {
            symbols_synced: AtomicUsize::new(0),
            total_symbols,
        }
    }
}

pub async fn task_sync_orderbook(
    mut receiver: Receiver<OrderbookEvent>,
    sender: Sender<OrderbookEvent>,
    local_full_orderbook: Arc<Mutex<FullOrderbook>>,
    sync_state: Arc<SyncState>,
) -> Result<()> {
    let mut counter = 0;
    tracing::info!("Starting task_sync_orderbook");

    loop {
        match receiver.recv().await {
            Ok(event) => {
                let mut full_orderbook = local_full_orderbook.lock().await;
                match event {
                    OrderbookEvent::OrderbookReceived((symbol, orderbook)) => {
                        full_orderbook.insert(symbol.clone(), orderbook);
                        sync_state.symbols_synced.fetch_add(1, Ordering::Relaxed);
                        tracing::info!("Initialized Orderbook for {symbol}. Total synced: {}", 
                                       sync_state.symbols_synced.load(Ordering::Relaxed));
                    }
                    OrderbookEvent::OrderbookChangeReceived((symbol, orderbook_change)) => {
                        if let Some(orderbook) = full_orderbook.get_mut(&symbol) {
                            match orderbook.merge(orderbook_change) {
                                Ok(Some(ob)) => {
                                    if let Err(e) = sender.send(OrderbookEvent::OrderbookChangeReceived((symbol.clone(), ob))) {
                                        tracing::error!("Failed to send OrderbookChangeReceived event: {}", e);
                                    }
                                }
                                Err(e) => {
                                    tracing::error!("Merge conflict for {symbol}: {e}");
                                }
                                _ => {} // no update in best price
                            }
                        } else {
                            tracing::warn!("Received change for unknown symbol: {symbol}");
                        }
                    }
                }

                counter += 1;
                if counter % 1000 == 0 {
                    let synced = sync_state.symbols_synced.load(Ordering::Relaxed);
                    let total = sync_state.total_symbols;
                    tracing::info!("Orderbook sync state: {}/{} symbols", synced, total);
                }
            }
            Err(e) => {
                tracing::error!("Error receiving OrderbookEvent: {}", e);
                // Puedes decidir si quieres romper el loop aquí o continuar
                // break;
            }
        }
    }
}