use timely::WorkerConfig;
use tracing::info;

use crate::catalog::Catalog;
use crate::gid::GIDGen;

pub mod row;
pub mod coord;
pub mod typedef;
pub mod worker;
pub mod handle;
pub mod name;
pub mod error;
pub mod gid;
pub mod catalog;

pub struct Config {
    pub workers: usize,
    pub worker_config: WorkerConfig,
}

pub async fn start(config: Config) -> Result<handle::Handle, error::Error> {
    info!("start ddquery");

    assert!(config.workers > 0);

    // 创建 coord 和 worker 之间的 channel
    let mut txs = vec![];
    let mut rxs = vec![];
    for _ in 0..config.workers {
        let (tx, rx) = crossbeam_channel::unbounded();
        txs.push(tx);
        rxs.push(rx);
    }

    let w_config = worker::Config {
        cmd_rxs: rxs,
        timely_worker: config.worker_config,
    };
    let guards = worker::serve(w_config)?;

    // 创建 handle 和 coord 之间的 channel
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    coord::Coord {
        epoch: 0,
        gid_gen: GIDGen::new(),
        catalog: Catalog::new(config.workers),
        cmd_rx: rx,
        worker_guards: guards,
        worker_txs: txs,
    }.run().await;

    info!("ddquery started");
    Ok(handle::Handle::new(tx))
}