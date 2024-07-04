use eyre::Result;
use kucoin_api::client::{Kucoin, KucoinEnv};
use kucoin_arbitrage::broker::gatekeeper::kucoin::task_gatekeep_chances;
use kucoin_arbitrage::broker::order::kucoin::task_place_order;
use kucoin_arbitrage::broker::orderbook::internal::task_sync_orderbook;
use kucoin_arbitrage::broker::symbol::filter::{symbol_with_quotes, vector_to_hash};
use kucoin_arbitrage::broker::symbol::kucoin::{format_subscription_list, get_symbols};
use kucoin_arbitrage::event::{
    chance::ChanceEvent, order::OrderEvent, orderbook::OrderbookEvent, trade::TradeEvent,
};
use kucoin_arbitrage::model::orderbook::FullOrderbook;
use kucoin_arbitrage::monitor::counter::Counter;
use kucoin_arbitrage::monitor::task::{task_log_mps as external_task_log_mps, task_monitor_channel_mps as external_task_monitor_channel_mps};
use kucoin_arbitrage::strategy::all_taker_btc_usd::task_pub_chance_all_taker_btc_usd;
use kucoin_arbitrage::system_event::task_signal_handle;
use std::sync::Arc;
use tokio::sync::broadcast::{channel, Receiver};
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use serde_json::json;
use serde_derive::Serialize;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::Write;
use tokio::time::{sleep, Duration};
use chrono::Utc;

const MAKER_FEE: f64 = 0.001; // 0.1%
const TAKER_FEE: f64 = 0.001; // 0.1%
const MIN_PROFIT: f64 = 0.001; // 0.3%
const MIN_INVESTMENT: f64 = 100.0;
const MAX_INVESTMENT: f64 = 1000.0;
const MAX_ROUTE_LENGTH: usize = 5; // Máximo número de pasos en una ruta
const MAX_RETRIES: u32 = 5;
const RETRY_DELAY: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, Serialize)]
struct ArbitrageRoute {
    path: Vec<String>,
    profit: f64,
    optimal_investment: f64,
    steps: Vec<TradeStep>,
}

#[derive(Debug, Clone, Serialize)]
struct TradeStep {
    from: String,
    to: String,
    action: String,
    rate: f64,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing::info!("Log setup");
    let config = kucoin_arbitrage::config::from_file("config.toml")?;
    let _worker_guard = kucoin_arbitrage::logger::setup_logs(&config.log)?;
    tracing::info!("Log setup");
    tokio::select! {
        _ = task_signal_handle() => tracing::error!("received external signal, terminating program"),
        res = core(config) => tracing::error!("core ended first {res:?}"),
    };

    println!("Good bye!");
    Ok(())
}

async fn core(config: kucoin_arbitrage::config::Config) -> Result<()> {
    let budget = config.behaviour.usd_cyclic_arbitrage;
    let monitor_interval = config.behaviour.monitor_interval_sec;

    let api = Kucoin::new(KucoinEnv::Live, Some(config.kucoin_credentials()))
        .map_err(|e| eyre::eyre!(e))?;
    tracing::info!("Credentials setup");

    let symbol_list = get_symbols(api.clone()).await?;
    tracing::info!("Total exchange symbols: {:?}", symbol_list.len());

    let symbol_infos = symbol_with_quotes(&symbol_list, "BTC", "USDT");
    let hash_symbols = Arc::new(Mutex::new(vector_to_hash(&symbol_infos)));
    tracing::info!("Total symbols in scope: {:?}", symbol_infos.len());

    let subs = format_subscription_list(&symbol_infos);
    tracing::info!("Total orderbook WS sessions: {:?}", subs.len());

    let cx_orderbook = Arc::new(Mutex::new(Counter::new("orderbook")));
    let tx_orderbook = channel::<OrderbookEvent>(1024 * 2).0;
    let cx_orderbook_best = Arc::new(Mutex::new(Counter::new("best_price")));
    let tx_orderbook_best = channel::<OrderbookEvent>(512).0;
    let cx_chance = Arc::new(Mutex::new(Counter::new("chance")));
    let tx_chance = channel::<ChanceEvent>(64).0;
    let cx_order = Arc::new(Mutex::new(Counter::new("order")));
    let tx_order = channel::<OrderEvent>(16).0;
    let cx_trade = Arc::new(Mutex::new(Counter::new("trade")));
    let tx_trade = channel::<TradeEvent>(128).0;
    tracing::info!("Broadcast channels setup");

    let full_orderbook = Arc::new(Mutex::new(FullOrderbook::new()));
    tracing::info!("Local empty full orderbook setup");

    let mut taskpool_infrastructure: JoinSet<Result<()>> = JoinSet::new();
    taskpool_infrastructure.spawn(task_sync_orderbook(
        tx_orderbook.subscribe(),
        tx_orderbook_best.clone(),
        full_orderbook.clone(),
    ));
    taskpool_infrastructure.spawn(task_pub_chance_all_taker_btc_usd(
        tx_orderbook_best.subscribe(),
        tx_chance.clone(),
        full_orderbook.clone(),
        hash_symbols,
        budget as f64,
    ));
    taskpool_infrastructure.spawn(task_gatekeep_chances(
        tx_chance.subscribe(),
        tx_trade.subscribe(),
        tx_order.clone(),
    ));
    taskpool_infrastructure.spawn(task_place_order(tx_order.subscribe(), api.clone()));

    let mut taskpool_monitor: JoinSet<Result<()>> = JoinSet::new();
    taskpool_monitor.spawn(external_task_monitor_channel_mps(
        tx_orderbook.subscribe(),
        cx_orderbook.clone(),
    ));
    taskpool_monitor.spawn(external_task_monitor_channel_mps(
        tx_orderbook_best.subscribe(),
        cx_orderbook_best.clone(),
    ));
    taskpool_monitor.spawn(external_task_monitor_channel_mps(
        tx_chance.subscribe(),
        cx_chance.clone(),
    ));
    taskpool_monitor.spawn(external_task_monitor_channel_mps(
        tx_order.subscribe(),
        cx_order.clone(),
    ));
    taskpool_monitor.spawn(external_task_monitor_channel_mps(
        tx_trade.subscribe(),
        cx_trade.clone(),
    ));
    taskpool_monitor.spawn(external_task_log_mps(
        vec![
            cx_orderbook.clone(),
            cx_orderbook_best.clone(),
            cx_chance.clone(),
            cx_order.clone(),
            cx_trade.clone(),
        ],
        monitor_interval as u64,
    ));

    let mut taskpool_subscription: JoinSet<Result<()>> = JoinSet::new();
    for (i, sub) in subs.iter().enumerate() {
        taskpool_subscription.spawn(kucoin_arbitrage::broker::orderbook::kucoin::task_pub_orderbook_event(
            api.clone(),
            sub.to_vec(),
            tx_orderbook.clone(),
        ));
        tracing::info!("{i:?}-th session of WS subscription setup");
    }

    loop {
        tokio::select! {
            Some(res) = taskpool_infrastructure.join_next() => {
                tracing::warn!("Infrastructure task completed: {:?}", res);
                if let Err(e) = res {
                    tracing::error!("Infrastructure task pool error: {:?}", e);
                    break;
                }
            }
            Some(res) = taskpool_monitor.join_next() => {
                tracing::warn!("Monitor task completed: {:?}", res);
                if let Err(e) = res {
                    tracing::error!("Monitor task pool error: {:?}", e);
                    break;
                }
            }
            Some(res) = taskpool_subscription.join_next() => {
                tracing::warn!("Subscription task completed: {:?}", res);
                if let Err(e) = res {
                    tracing::error!("Subscription task pool error: {:?}", e);
                    break;
                }
            }
            _ = tokio::time::sleep(Duration::from_secs(10)) => {
                tracing::info!("Attempting to calculate arbitrage routes");
                let orderbook_size = {
                    let orderbook = full_orderbook.lock().await;
                    orderbook.len()
                };
                tracing::info!("Current orderbook size: {}", orderbook_size);
                
                match calculate_arbitrage_routes(full_orderbook.clone()).await {
                    Ok(arbitrage_routes) => {
                        tracing::info!("Found {} arbitrage routes", arbitrage_routes.len());
                        if !arbitrage_routes.is_empty() {
                            if let Err(e) = save_routes_to_json(&arbitrage_routes) {
                                tracing::error!("Failed to save routes to JSON: {:?}", e);
                            } else {
                                tracing::info!("Saved arbitrage routes to JSON");
                            }
                        }
                    }
                    Err(e) => tracing::error!("Failed to calculate arbitrage routes: {:?}", e),
                }
            }
        };
    }

    tracing::info!("Exiting core function");
    Ok(())
}

async fn calculate_arbitrage_routes(full_orderbook: Arc<Mutex<FullOrderbook>>) -> Result<Vec<ArbitrageRoute>> {
    tracing::info!("Starting arbitrage route calculation");
    let orderbook = full_orderbook.lock().await;
    tracing::info!("Orderbook locked, contains {} symbols", orderbook.len());
    
    if orderbook.is_empty() {
        tracing::warn!("Orderbook is empty, cannot calculate arbitrage routes");
        return Ok(vec![]);
    }

    let mut routes = Vec::new();
    let graph = build_graph(&orderbook);
    tracing::info!("Graph built with {} nodes", graph.len());

    if !graph.contains_key("USDT") {
        tracing::warn!("Graph does not contain USDT node");
        return Ok(vec![]);
    }

    let mut path = vec!["USDT".to_string()];
    let mut visited = HashSet::new();
    visited.insert("USDT".to_string());
    dfs_arbitrage(&graph, "USDT", &mut path, &mut visited, &mut routes, &orderbook);

    routes.sort_by(|a, b| b.profit.partial_cmp(&a.profit).unwrap());
    tracing::info!("Found {} potential arbitrage routes", routes.len());
    Ok(routes)
}

fn build_graph(orderbook: &FullOrderbook) -> HashMap<String, Vec<(String, f64)>> {
    let mut graph = HashMap::new();
    for (symbol, book) in orderbook.iter() {
        let (base, quote) = symbol.split_once('-').unwrap();
        graph.entry(base.to_string()).or_insert(Vec::new()).push((quote.to_string(), book.ask.first_key_value().unwrap().0.into_inner()));
        graph.entry(quote.to_string()).or_insert(Vec::new()).push((base.to_string(), 1.0 / book.bid.last_key_value().unwrap().0.into_inner()));
    }
    graph
}

fn dfs_arbitrage(
    graph: &HashMap<String, Vec<(String, f64)>>,
    current: &str,
    path: &mut Vec<String>,
    visited: &mut HashSet<String>,
    routes: &mut Vec<ArbitrageRoute>,
    orderbook: &FullOrderbook,
) {
    if path.len() > 1 && current == "USDT" {
        if let Some(route) = create_arbitrage_route(path, orderbook) {
            if route.profit > MIN_PROFIT {
                routes.push(route);
            }
        }
        return;
    }

    if path.len() >= MAX_ROUTE_LENGTH {
        return;
    }

    if let Some(neighbors) = graph.get(current) {
        for (next, _) in neighbors {
            if !visited.contains(next) {
                path.push(next.clone());
                visited.insert(next.clone());
                dfs_arbitrage(graph, next, path, visited, routes, orderbook);
                path.pop();
                visited.remove(next);
            }
        }
    }
}

fn create_arbitrage_route(path: &[String], orderbook: &FullOrderbook) -> Option<ArbitrageRoute> {
    let mut steps = Vec::new();
    let mut cumulative_rate = 1.0;

    for window in path.windows(2) {
        let from = &window[0];
        let to = &window[1];
        let symbol = format!("{}-{}", from, to);
        let reverse_symbol = format!("{}-{}", to, from);

        if let Some(book) = orderbook.get(&symbol) {
            let rate = book.ask.first_key_value()?.0.into_inner();
            cumulative_rate *= rate * (1.0 - TAKER_FEE);
            steps.push(TradeStep {
                from: from.clone(),
                to: to.clone(),
                action: "Buy".to_string(),
                rate,
            });
        } else if let Some(book) = orderbook.get(&reverse_symbol) {
            let rate = 1.0 / book.bid.last_key_value()?.0.into_inner();
            cumulative_rate *= rate * (1.0 - TAKER_FEE);
            steps.push(TradeStep {
                from: from.clone(),
                to: to.clone(),
                action: "Sell".to_string(),
                rate,
            });
        } else {
            return None;
        }
    }

    let profit = cumulative_rate - 1.0;
    let optimal_investment = optimize_investment(MIN_INVESTMENT, MAX_INVESTMENT, &steps, orderbook);

    Some(ArbitrageRoute {
        path: path.to_vec(),
        profit,
        optimal_investment,
        steps,
    })
}

fn optimize_investment(min: f64, max: f64, steps: &[TradeStep], orderbook: &FullOrderbook) -> f64 {
    let mut optimal = max;
    for step in steps {
        let symbol = format!("{}-{}", step.from, step.to);
        if let Some(book) = orderbook.get(&symbol) {
            let volume = if step.action == "Buy" {
                book.ask.first_key_value().unwrap().1.into_inner()
            } else {
                book.bid.last_key_value().unwrap().1.into_inner()
            };
            optimal = optimal.min(volume * step.rate);
        }
    }
    min.max(optimal.min(max))
}

fn save_routes_to_json(routes: &[ArbitrageRoute]) -> Result<()> {
    let json_data = json!({
        "arbitrage_routes": routes
    });

    let mut file = File::create("arbitrage_routes.json")?;
    file.write_all(serde_json::to_string_pretty(&json_data)?.as_bytes())?;
    Ok(())
}