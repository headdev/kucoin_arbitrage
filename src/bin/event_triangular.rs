use kucoin_arbitrage::broker::gatekeeper::kucoin::task_gatekeep_chances;
use kucoin_arbitrage::broker::order::kucoin::task_place_order;
use kucoin_arbitrage::broker::orderbook::kucoin::{task_pub_orderevent, task_sync_orderbook};
use kucoin_arbitrage::broker::strategy::all_taker_btc_usdt::task_pub_chance_all_taker_btc_usdt;
use kucoin_arbitrage::event::chance::ChanceEvent;
use kucoin_arbitrage::event::order::OrderEvent;
use kucoin_arbitrage::event::orderbook::OrderbookEvent;
use kucoin_arbitrage::model::orderbook::FullOrderbook;
use kucoin_arbitrage::translator::translator::OrderBookTranslator;
use kucoin_rs::kucoin::{
    client::{Kucoin, KucoinEnv},
    model::market::OrderBookType,
    model::websocket::{WSTopic, WSType},
};

use std::sync::{Arc, Mutex};
use tokio;
use tokio::sync::broadcast::channel;

#[tokio::main]
async fn main() -> Result<(), kucoin_rs::failure::Error> {
    // provide logging format
    kucoin_arbitrage::logger::log_init();
    log::info!("Log setup");

    // credentials
    let credentials = kucoin_arbitrage::globals::config::credentials();
    let api_1 = Kucoin::new(KucoinEnv::Live, Some(credentials))?;
    let api_2 = api_1.clone();
    let api_3 = api_1.clone();
    let url = api_1.get_socket_endpoint(WSType::Public).await?;
    log::info!("Credentials setup");

    // Configure the pairs to subscribe and analyze
    let symbols = [
        "ETH-BTC".to_string(),
        "BTC-USDT".to_string(),
        "ETH-USDT".to_string(),
    ];

    // TODO get the coin configs
    //  get all the data from symbol first, then obtain the symbil info
    // let x = api.get_symbol_list(market);

    // Initialize the websocket
    let mut ws = api_1.websocket();
    let subs = vec![WSTopic::OrderBook(symbols.to_vec())];
    ws.subscribe(url, subs).await?;
    log::info!("Websocket subscription setup");

    // Create broadcast channels
    // for syncing
    let (tx_orderbook, rx_orderbook) = channel::<OrderbookEvent>(256);
    // for getting notable orderbook after syncing
    let (tx_orderbook_best, rx_orderbook_best) = channel::<OrderbookEvent>(64);
    // for getting chance
    let (tx_chance, rx_chance) = channel::<ChanceEvent>(64);
    // for placing order
    let (tx_order, rx_order) = channel::<OrderEvent>(16);
    log::info!("broadcast channels setup");

    let full_orderbook = Arc::new(Mutex::new(FullOrderbook::new()));
    log::info!("Local Orderbook setup");

    // Infrastructure tasks
    tokio::spawn(task_sync_orderbook(rx_orderbook, tx_orderbook_best, full_orderbook.clone()));
    tokio::spawn(task_pub_chance_all_taker_btc_usdt(
        rx_orderbook_best,
        tx_chance,
        full_orderbook.clone(),
    ));
    tokio::spawn(task_gatekeep_chances(rx_chance, tx_order));
    tokio::spawn(task_place_order(rx_order, api_2));

    // use REST to obtain the initial orderbook before subscribing to websocket
    let full_orderbook_2 = full_orderbook.clone();
    for symbol in symbols.iter() {
        log::info!("obtaining initial orderbook[{symbol}] from REST");
        // OrderBookType::Full fails
        let res = api_3
            .get_orderbook(symbol.as_str(), OrderBookType::L100)
            .await
            .expect("invalid data");
        if let Some(data) = res.data {
            // log::info!("orderbook[{symbol}] {:#?}", data);
            let mut x = full_orderbook_2.lock().unwrap();
            (*x).insert(symbol.to_string(), data.to_internal());
        } else {
            log::warn!("orderbook[{symbol}] received none")
        }
    }

    // task_pub_orderevent is the source of data (websocket)
    tokio::spawn(task_pub_orderevent(ws, tx_orderbook));
    log::info!("task_pub_orderevent setup");

    log::info!("all application tasks setup");
    let _res = tokio::join!(kucoin_arbitrage::tasks::background_routine());
    panic!("Program should not arrive here")
}
