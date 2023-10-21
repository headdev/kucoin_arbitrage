use crate::monitor::counter;
use std::sync::Arc;
use tokio::sync::broadcast::Receiver;
use tokio::sync::Mutex;
use tokio::time;

/// log counters
async fn log_mps(
    counters: Vec<Arc<Mutex<counter::Counter>>>,
    interval: u64,
) -> Result<(), failure::Error> {
    log::info!("Broadcast channel MPS");
    for counter in counters.iter() {
        let (name, count) = {
            let p = counter.lock().await;
            (p.name, p.data_count)
        };
        log::info!("{name:12}: {count:5} messages ({:5}mps)", count / interval);
        // clear the data
        counter::reset(counter.clone()).await;
    }
    Ok(())
}
/// log counters as a task
pub async fn task_log_mps(
    counters: Vec<Arc<Mutex<counter::Counter>>>,
    interval: u64,
) -> Result<(), failure::Error> {
    let monitor_delay = time::Duration::from_secs(interval);
    loop {
        time::sleep(monitor_delay).await;
        log_mps(counters.clone(), interval)
            .await
            .expect("report status error");
    }
}

/// increment counter
pub async fn task_monitor_channel_mps<T: Clone>(
    mut receiver: Receiver<T>,
    counter: Arc<Mutex<counter::Counter>>,
) -> Result<(), failure::Error> {
    loop {
        receiver.recv().await?;
        counter::increment(counter.clone()).await;
    }
}