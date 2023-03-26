use kucoin_arbitrage::broker::orderbook::kucoin::{task_pub_orderevent, task_sync_orderbook};
use kucoin_arbitrage::model::orderbook::FullOrderbook as InhouseFullOrderBook;
// use kucoin_arbitrage::strategy::all_taker::task_triangular_arbitrage;
use kucoin_rs::kucoin::{
    client::{Kucoin, KucoinEnv},
    model::websocket::{WSTopic, WSType},
};
use kucoin_rs::tokio;
use std::sync::Arc;
use tokio::sync::broadcast::channel;

#[tokio::main]
async fn main() -> Result<(), kucoin_rs::failure::Error> {
    // provide logging format
    kucoin_arbitrage::logger::log_init();
    log::info!("Log setup");

    // credentials
    let credentials = kucoin_arbitrage::globals::config::credentials();
    let api = Kucoin::new(KucoinEnv::Live, Some(credentials))?;
    let url = api.get_socket_endpoint(WSType::Public).await?;
    log::info!("Credentials setup");

    // Initialize the websocket
    let mut ws = api.websocket();
    let subs = vec![
        WSTopic::OrderBook(vec![
            "ETH-BTC".to_string(),
            "BTC-USDT".to_string(),
            "ETH-USDT".to_string(),
        ]),
        // WSTopic::OrderBookChange(vec!["ETH-BTC".to_string(), "BTC-USDT".to_string()]),
    ];
    ws.subscribe(url, subs).await?;
    log::info!("Websocket subscription setup");

    // Create a broadcast channel.
    let (sender, mut receiver) = channel(256);
    let sender = Arc::new(sender);
    log::info!("Channel setup");

    tokio::spawn(async move { task_pub_orderevent(ws, sender).await });
    log::info!("task_pub_orderevent setup");

    // Spawn multiple tasks to receive messages.
    let mut full_orderbook = InhouseFullOrderBook::new();

    tokio::spawn(async move { task_sync_orderbook(&mut receiver, &mut full_orderbook).await });
    log::info!("task_pub_orderevent setup");

    kucoin_arbitrage::tasks::background_routine().await
}
