use crate::event::orderbook::OrderbookEvent;
use crate::model::orderbook::FullL2Orderbook;
use crate::model::symbol::SymbolInfo;
use crate::translator::traits::{ToOrderBook, ToOrderBookChange};
use chrono::{TimeZone, Utc};
use eyre::Result;
use kucoin_api::client::Kucoin;
use kucoin_api::futures::TryStreamExt;
use kucoin_api::model::market::{OrderBook, OrderBookType};
use kucoin_api::model::websocket::{KucoinWebsocketMsg, WSTopic, WSType};
use std::sync::Arc;
use tokio::sync::broadcast::Sender;
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tokio::time::Duration;

/// Subscribe Websocket API, then publish internal OrderbookEvent
pub async fn task_pub_orderbook_event(
    api: Kucoin,
    topics: Vec<WSTopic>,
    sender: Sender<OrderbookEvent>,
) -> Result<()> {
    let serial = 0;
    let url_public = api
        .get_socket_endpoint(WSType::Public)
        .await
        .map_err(|e| eyre::eyre!(e))?;
    let mut ws = api.websocket();
    ws.subscribe(url_public.clone(), topics)
        .await
        .map_err(|e| eyre::eyre!(e))?;
    loop {
        let msg = ws.try_next().await.map_err(|e| eyre::eyre!(e))?;
        let msg = msg.unwrap();
        match msg {
            KucoinWebsocketMsg::OrderBookMsg(msg) => {
                let ts_message = msg.data.time;
                tracing::info!("message(raw): {ts_message:?}");
                let t_message = Utc.timestamp_millis_opt(ts_message as i64).unwrap();
                let t_system = Utc::now();
                let latency = t_system - t_message;
                tracing::info!("message: {t_message:?}");
                tracing::info!("system: {t_system:?}");
                tracing::info!("latency: {latency:?}");

                let (str, data) = msg.data.to_internal(serial);
                let event = OrderbookEvent::OrderbookChangeReceived((str, data));
                sender.send(event)?;
            }
            KucoinWebsocketMsg::TickerMsg(msg) => {
                tracing::info!("TickerMsg: {msg:#?}");
            }
            KucoinWebsocketMsg::OrderBookChangeMsg(msg) => {
                tracing::info!("OrderbookChange: {msg:#?}")
            }
            KucoinWebsocketMsg::WelcomeMsg(_) => {
                tracing::info!("Welcome to KuCoin public WS")
            }
            KucoinWebsocketMsg::PongMsg(_) => {}
            other => {
                tracing::error!("unregistered message {other:?}")
            }
        };
    }
}

/// Obtain current orderbook of a list of symbol from Kucoin REST API
pub async fn task_get_orderbook(api: Kucoin, symbol: &str) -> Result<OrderBook> {
    let mut try_counter = 0;
    loop {
        try_counter += 1;
        // OrderBookType::Full requires valid API Key
        let res = api.get_orderbook(symbol, OrderBookType::L20).await;

        if let Err(e) = res {
            tracing::warn!("orderbook[{symbol}] did not respond ({try_counter:?} tries) [{e:?}]");
            let null_err_msg = "invalid type: null, expected a string";
            if e.to_string().contains(null_err_msg) {
                eyre::bail!("null received ffrom {symbol}");
            }
            // TODO there are cases when no orderbook is obtained. Check if this is due to the network condition or the orderbook itself
            if try_counter > 100 {
                eyre::bail!("[{try_counter:?}] has failed more than 100 times");
            }
            continue;
        }
        let response = res.map_err(|e| eyre::eyre!(e))?;
        match response.code.as_str() {
            "200000" => {
                if response.data.is_none() {
                    tracing::warn!("orderbook[{symbol}] received none ({try_counter:?} tries)");
                    continue;
                }
                tracing::info!("obtained [{symbol}]");
                return Ok(response.data.unwrap());
            }
            "400003" => eyre::bail!("API key needed not but provided"),
            "429000" => {
                tracing::warn!("[{symbol:?}] request overloaded ({try_counter:?} tries)")
            }
            code => eyre::bail!("unrecognised code [{code:?}]"),
        }
    }
}

/// Obtain all the inital orderbook using Kucoin REST API
pub async fn task_get_initial_orderbooks(
    api: Kucoin,
    symbol_infos: Vec<SymbolInfo>,
    full_orderbook: Arc<Mutex<FullL2Orderbook>>,
) -> Result<()> {
    // replace spawn with or a taskpool
    let mut taskpool_aggregate = JoinSet::new();
    // collect all initial orderbook states with REST
    let symbols: Vec<String> = symbol_infos.into_iter().map(|info| info.symbol).collect();
    tracing::info!("Total symbols: {:?}", symbols.len());
    for symbol in symbols {
        let api = api.clone();
        let full_orderbook_arc = full_orderbook.clone();
        taskpool_aggregate.spawn(async move {
            let data = task_get_orderbook(api, &symbol).await.unwrap();
            let mut x = full_orderbook_arc.lock().await;
            x.insert(symbol.to_string(), data.to_internal());
            symbol
        });
        // prevent server overloading
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let task_name = taskpool_aggregate
        .join_next()
        .await
        .ok_or(eyre::eyre!("empty taskpool"))??;
    tracing::info!("Initialized orderbook for [{:?}]", task_name);
    Ok(())
}
