use std::sync::Arc;

use crossbeam_channel::Sender;
use timely::communication::WorkerGuards;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;
use tracing::info;

use crate::catalog::Catalog;
use crate::error::Error;
use crate::gid::{GIDGen};
use crate::name::Name;
use crate::txn_manager::TxnManager;
use crate::typedef::{Data, Timestamp, Trace};
use crate::worker::{WorkerCommand, WorkerContext};

// TODO drop input/trace
pub enum CoordCommand<K, V>
where K: Data, V: Data
{
    CreateInputAndTrace {
        name: Name,
        tx: oneshot::Sender<Result<(), Error>>,
    },
    CreateDerive {
        name: Name,
        f: Arc<dyn for <'a> Fn(&mut WorkerContext<'a, K ,V>) -> Option<Trace<K, V>> + Send + Sync + 'static>,
        tx: oneshot::Sender<Result<(), Error>>,
    },
    Upsert {
        name: Name,
        key: K,
        value: Option<V>,
        tx: oneshot::Sender<Result<(), Error>>,
    },
    Query {
        name: Name,
        key: K,
        tx: UnboundedSender<Result<Vec<V>, Error>>,
    },
    // TODO: support gracefully shutdown
    Shutdown
}

pub struct Coord<K, V>
    where K: Data, V: Data
{
    pub (crate) epoch: Timestamp,
    pub (crate) gid_gen: GIDGen,
    pub (crate) catalog: Catalog,
    pub (crate) txn_mananger: TxnManager,
    pub (crate) cmd_rx: UnboundedReceiver<CoordCommand<K, V>>,
    // worker and channel
    pub (crate) worker_guards: WorkerGuards<()>,
    pub (crate) worker_txs: Vec<Sender<WorkerCommand<K, V>>>,
    pub (crate) closed: bool,
}

impl<K, V> Coord<K, V>
    where K: Data, V: Data
{
    pub async fn run(mut self) {
        info!("coord start to serve");
        loop {
            tokio::select! {
                cmd = self.cmd_rx.recv() => {
                    match cmd.unwrap() {
                        CoordCommand::CreateInputAndTrace { name, tx } => {
                            if let Err(e) = self.catalog.create_input_and_trace(name.clone()) {
                                tx.send(Err(e)).unwrap();
                            } else {
                                let cmd = WorkerCommand::CreateInputAndTrace {name};
                                self.broadcast(cmd);
                                tx.send(Ok(())).unwrap();
                            }
                        },
                        CoordCommand::CreateDerive { name, f, tx } => {
                            if let Err(e) = self.catalog.create_trace(name.clone()) {
                                tx.send(Err(e)).unwrap();
                            } else {
                                let cmd = WorkerCommand::CreateDerive {name, f};
                                self.broadcast(cmd);
                                tx.send(Ok(())).unwrap();
                            }
                        },
                        CoordCommand::Upsert { name, key, value, tx } => {
                            if !self.catalog.input_exists(&name) {
                                tx.send(Err(Error::InputNotExists(name))).unwrap();
                            } else {
                                let time = self.advance_epoch();
                                let c1 = WorkerCommand::Upsert {name, time, key, value};
                                let c2 = WorkerCommand::AdvanceInput {time: self.epoch};
                                self.broadcast_n([c1,c2]);
                                tx.send(Ok(())).unwrap();
                            }
                        },
                        CoordCommand::Query { name, key, tx } => {
                            match self.catalog.determine_trace_worker(&name, &key) {
                                Ok(idx) =>  {
                                    let time = self.query_time();
                                    let token = self.txn_mananger.allocate_read_token(time);
                                    let uuid = token.txn.uuid;
                                    let cmd = WorkerCommand::Query { uuid, name, time, key, tx, token};
                                    self.unicast(cmd, idx);
                                },
                                Err(e) => tx.send(Err(e)).unwrap(),
                            };
                        },
                        CoordCommand::Shutdown => {
                            let cmd = WorkerCommand::Shutdown;
                            self.broadcast(cmd);
                            self.closed = true;
                            break;
                        }
                    }
                }
                read_txn = self.txn_mananger.rx.recv() => {
                    let read_txn = read_txn.unwrap();
                    info!("read txn: {} completed", read_txn.uuid);
                    self.txn_mananger.read_complete(read_txn);
                }
            }
        }

        info!("coord shutdown")
    }

    fn advance_epoch(&mut self) -> Timestamp {
        let ret = self.epoch;
        self.epoch += 1;
        ret
    }

    fn query_time(&self) -> Timestamp {
        assert!(self.epoch > 0, "Coord not initialized, epoch must > 0");
        self.epoch - 1
    }

    fn unicast(&mut self, cmd: WorkerCommand<K, V>, idx: usize) {
        self.worker_txs[idx].send(cmd).unwrap();
        self.worker_guards.guards()[idx].thread().unpark();
    }

    fn broadcast(&mut self, cmd: WorkerCommand<K, V>) {
        for tx in &mut self.worker_txs {
            tx.send(cmd.clone()).unwrap();
        }
        for handle in self.worker_guards.guards() {
            handle.thread().unpark();
        }
    }

    fn broadcast_n<const N: usize>(&mut self, cmds: [WorkerCommand<K, V>; N]) {
        for tx in &mut self.worker_txs {
            for cmd in &cmds {
                tx.send(cmd.clone()).unwrap();
            }
        }
        for handle in self.worker_guards.guards() {
            handle.thread().unpark();
        }
    }
}

impl<K, V> Drop for Coord<K, V>
    where K: Data, V: Data
{
    fn drop(&mut self) {
        if !self.closed {
            let cmd = WorkerCommand::Shutdown;
            self.broadcast(cmd);
            self.closed = true;
        }
    }
}
