use eyre::Result;
use interning::{InternedString, InternedStringHash};
use kucoin_api::client::{Kucoin, KucoinEnv};
use kucoin_api::model::market::SymbolList;
use kucoin_arbitrage::system_event::task_signal_handle;
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Display};
use std::hash::Hash;

/// cyclic_all is the advanced version of btcusdt which trades on all the cyclic arbitrage of any size, any path.
#[tokio::main]
async fn main() -> Result<()> {
    println!("program started, exit by sending SIGINT/SIGTERM");
    let config = kucoin_arbitrage::config::from_file("config.toml")?;
    tokio::select! {
        _ = task_signal_handle() => eyre::bail!("end"),
        _ = core(config) => Ok(()),
    }
}

////////////////////////////// struct

#[derive(Clone, Copy, Hash, PartialEq, PartialOrd, Eq, Ord)]
pub struct Pair {
    base: u64,
    quote: u64,
}
impl Pair {
    pub fn new(base: u64, quote: u64) -> Self {
        Pair { base, quote }
    }
}
impl Debug for Pair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let base = hash_to_string(self.base);
        let quote = hash_to_string(self.quote);
        write!(f, "{}-{}", base, quote)
    }
}
impl std::fmt::Display for Pair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let base = hash_to_string(self.base);
        let quote = hash_to_string(self.quote);
        write!(f, "{}-{}", base, quote)
    }
}
impl From<SymbolList> for Pair {
    fn from(value: SymbolList) -> Self {
        let base = InternedString::from(value.base_currency);
        let base = base.hash().hash();
        let quote = InternedString::from(value.quote_currency);
        let quote = quote.hash().hash();
        Pair { base, quote }
    }
}

#[derive(Clone, Copy, Hash, Debug, PartialEq, PartialOrd, Eq, Ord)]
pub enum Action {
    Buy,
    Sell,
}

#[derive(Clone, Copy, Hash, PartialEq, PartialOrd, Eq, Ord)]
pub struct TradeAction {
    pub pair: Pair,
    pub action: Action,
}
impl Debug for TradeAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}({:?})", self.action, self.pair)
    }
}
impl Display for TradeAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}({})", self.action, self.pair)
    }
}
impl TradeAction {
    pub fn buy(base: u64, quote: u64) -> Self {
        TradeAction {
            pair: Pair { base, quote },
            action: Action::Buy,
        }
    }
    pub fn sell(base: u64, quote: u64) -> Self {
        TradeAction {
            pair: Pair { base, quote },
            action: Action::Sell,
        }
    }
}
#[derive(Clone, Hash, PartialEq, PartialOrd, Eq, Ord, Default)]
pub struct TradeCycle {
    actions: Vec<TradeAction>,
}
impl From<Vec<TradeAction>> for TradeCycle {
    fn from(actions: Vec<TradeAction>) -> Self {
        TradeCycle { actions }
    }
}
impl TradeCycle {
    pub fn new() -> Self {
        TradeCycle::default()
    }
    pub fn push(&mut self, trade_action: TradeAction) {
        self.actions.push(trade_action)
    }
    pub fn pop(&mut self) -> Option<TradeAction> {
        self.actions.pop()
    }
    pub fn len(&self) -> usize {
        self.actions.len()
    }
    pub fn is_empty(&self) -> bool {
        self.actions.is_empty()
    }
    pub fn first(&self) -> Option<&TradeAction> {
        self.actions.first()
    }
    pub fn get_all_pairs(&self) -> HashSet<Pair> {
        let mut pairs = HashSet::<Pair>::new();
        for action in &self.actions {
            pairs.insert(action.pair);
        }
        pairs
    }
    pub fn contains(&self, pair: &Pair) -> bool {
        for action in &self.actions {
            if action.pair.eq(pair) {
                return true;
            }
        }
        return false;
    }
}
impl Debug for TradeCycle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Cycle{:?}", self.actions)
    }
}
impl Display for TradeCycle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Cycle{:?}", self.actions)
    }
}

// all cycles can be used to look up for trade cycles with the asset ID
pub type AllCycles = HashMap<u64, Vec<TradeCycle>>;

////////////////////////////// fn

pub fn hash_to_string(id: u64) -> String {
    let hash = InternedStringHash::from_bytes(id.to_be_bytes());
    unsafe { InternedString::from_hash(hash) }.to_string()
}
// pairs in, cycles out
#[derive(Clone, Default)]
struct CycleFinder {
    start: u64,
    visited: HashSet<u64>,
    path: Vec<TradeAction>,
    found_cycles: Vec<TradeCycle>,
    length_limit: Option<usize>,
}
impl CycleFinder {
    pub fn new(length_limit: Option<usize>) -> Self {
        CycleFinder {
            length_limit,
            ..Default::default()
        }
    }
    /// search function
    fn dfs(&mut self, current: u64, graph: &HashMap<u64, Vec<Pair>>) {
        self.visited.insert(current);
        let pairs = graph.get(&current).expect("no pair found");
        // tracing::info!("dfs({})", hash_to_string(current));
        for pair in pairs {
            let next_node = if current == pair.base {
                pair.quote
            } else {
                pair.base
            };

            let action = if current == pair.base {
                Action::Sell
            } else {
                Action::Buy
            };
            let action = TradeAction {
                pair: *pair,
                action,
            };
            if next_node == self.start {
                // found cycle
                let mut path = self.path.clone();
                path.push(action);
                self.found_cycles.push(TradeCycle::from(path));
            } else if !self.visited.contains(&next_node) {
                if self.path.len() <= self.length_limit.unwrap_or(self.path.len()) {
                    //skip when next node was alr visited
                    self.path.push(action);
                    self.dfs(next_node, graph); // After the first trade, no need to enforce Buy as start.
                    self.path.pop();
                }
            }
        }
        self.visited.remove(&current);
    }
    /// generate all the cyclic paths from the Graph
    pub fn find_cycles(
        &mut self,
        pairs: impl IntoIterator<Item = Pair>,
        start: u64,
    ) -> Vec<TradeCycle> {
        // populate graph from pairs
        let mut graph: HashMap<u64, Vec<Pair>> = HashMap::new();
        for pair in pairs {
            graph.entry(pair.base).or_default().push(pair);
            graph.entry(pair.quote).or_default().push(pair);
        }
        self.start = start;
        self.visited.clear();
        self.path.clear();
        self.found_cycles.clear();
        // Start DFS with the flag to ensure the first trade is a Buy
        self.dfs(start, &graph);
        std::mem::take(&mut self.found_cycles)
    }
}

/// generate a reverse pointing hashmap which uses pair to fid all the cyclic containing the pair.
pub fn pair_to_cycle(
    pairs: &Vec<Pair>,
    cycles: &Vec<TradeCycle>,
) -> HashMap<Pair, HashSet<TradeCycle>> {
    let mut map: HashMap<Pair, HashSet<TradeCycle>> = HashMap::new();
    for pair in pairs {
        for cycle in cycles {
            if cycle.contains(&pair) {
                // append to a hashmap
                map.entry(pair.clone())
                    .or_insert_with(HashSet::new)
                    .insert(cycle.clone());
            }
        }
    }
    map
}

async fn core(config: kucoin_arbitrage::config::Config) -> Result<()> {
    let _worker_guard = kucoin_arbitrage::logger::setup_logs(&config.log)?;
    // kucoin api endpoints
    let api = Kucoin::new(KucoinEnv::Live, Some(config.kucoin_credentials()))
        .map_err(|e| eyre::eyre!(e))?;

    tracing::info!("credentials setup");
    let symbol_list = api.get_symbol_list(None).await;
    let symbol_list = symbol_list.expect("failed receiving data from exchange");
    let symbol_list = symbol_list.data.expect("empty symbol list");
    let pairs: Vec<Pair> = symbol_list.into_iter().map(Pair::from).collect();
    tracing::info!("{} pairs found", pairs.len());
    let dt_found_pairs = chrono::Utc::now();

    // usd as starting node
    let start_node = InternedString::from_str("USDT");
    let start_node = start_node.hash().hash();
    // find all path
    // 1 seconds to find cycles len <= 3 (802 cycles)
    // 6 seconds to find cycles len <= 4 (62K cycles)
    // 30 seconds to find cycles len <= 5 (222K cycles)
    // 140 seconds to find cycles len <= 6 (3690K cycles)
    let length_min = 3;
    let length_max = 3;
    let mut finder = CycleFinder::new(Some(length_max));
    let found_cycles: Vec<TradeCycle> = finder.find_cycles(pairs.clone(), start_node);
    // filter
    let count = |x: &TradeCycle| x.len() >= length_min && x.len() <= length_max;
    let buy = |x: &TradeCycle| x.first().unwrap().action == Action::Buy;
    let found_cycles = found_cycles.into_iter();
    let found_cycles: Vec<TradeCycle> = found_cycles.filter(count).filter(buy).collect();
    tracing::info!("{} cycles found", found_cycles.len());
    let dt_found_cycles = chrono::Utc::now();

    // 1 pair id to N trade cycle id
    let mut pair_to_cycle = pair_to_cycle(&pairs, &found_cycles);
    let dt_mapped_cycles = chrono::Utc::now();

    // test with BTC_USDT pair
    let btc = InternedString::from_str("BTC").hash().hash();
    let usd = InternedString::from_str("USDT").hash().hash();
    let new_pair = Pair::new(btc, usd);
    // these should be the cycles containing BTC_USDT
    let res = pair_to_cycle.entry(new_pair).or_default();
    tracing::info!("res:{res:#?}");
    let dt_found_mapped_cycles = chrono::Utc::now();

    
    // print each time
    dbg!((dt_found_cycles - dt_found_pairs).num_milliseconds()); //750ms
    dbg!((dt_mapped_cycles - dt_found_cycles).num_milliseconds()); //2ms
    dbg!((dt_found_mapped_cycles - dt_mapped_cycles).num_milliseconds()); //4us without print
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dfs() {
        // Setup a simple graph that represents the trading pairs.
        let pairs = vec![
            Pair::new(1, 2),
            Pair::new(2, 3),
            Pair::new(3, 1),
            Pair::new(2, 4),
            Pair::new(4, 1),
        ];

        let start_node = 1u64;
        let mut finder = CycleFinder::new(None);
        let actual_cycles = finder.find_cycles(pairs, start_node);
        // Define the expected paths using the Trade struct.
        // Note: The expected paths should match the actual trading paths you expect based on your graph setup.
        let expected_cycles = vec![
            TradeCycle::from(vec![
                TradeAction::buy(3, 1),
                TradeAction::buy(2, 3),
                TradeAction::buy(1, 2),
            ]),
            TradeCycle::from(vec![
                TradeAction::buy(3, 1),
                TradeAction::buy(2, 3),
                TradeAction::sell(2, 4),
                TradeAction::sell(4, 1),
            ]),
            TradeCycle::from(vec![TradeAction::buy(3, 1), TradeAction::sell(3, 1)]),
            TradeCycle::from(vec![
                TradeAction::buy(4, 1),
                TradeAction::buy(2, 4),
                TradeAction::buy(1, 2),
            ]),
            TradeCycle::from(vec![TradeAction::buy(4, 1), TradeAction::sell(4, 1)]),
            TradeCycle::from(vec![
                TradeAction::buy(4, 1),
                TradeAction::buy(2, 4),
                TradeAction::sell(2, 3),
                TradeAction::sell(3, 1),
            ]),
        ];
        // TODO might better off writing a custom cmp function with Vec<TradeCycle>
        let actual: HashSet<TradeCycle> = actual_cycles.into_iter().collect();
        let expected: HashSet<TradeCycle> = expected_cycles.into_iter().collect();

        // Check if the trading paths found match the expected paths.
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_dfs_case_2() {
        // Setup a simple graph that represents the trading pairs.
        let pairs = vec![Pair::new(1, 2), Pair::new(2, 3), Pair::new(3, 1)];

        let start_node = 1u64;
        let mut finder = CycleFinder::new(None);
        let actual_cycles = finder.find_cycles(pairs, start_node);

        // Define the expected paths using the Trade struct.
        // Note: The expected paths should match the actual trading paths you expect based on your graph setup.
        let expected_cycles = vec![
            TradeCycle::from(vec![
                TradeAction::buy(3, 1),
                TradeAction::buy(2, 3),
                TradeAction::buy(1, 2),
            ]),
            TradeCycle::from(vec![TradeAction::buy(3, 1), TradeAction::sell(3, 1)]),
        ];

        // Check if the trading paths found match the expected paths.
        assert_eq!(actual_cycles, expected_cycles);
    }
}
