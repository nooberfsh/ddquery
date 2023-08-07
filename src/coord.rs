use std::sync::Arc;

use crossbeam_channel::Sender;
use timely::communication::WorkerGuards;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;

use crate::catalog::Catalog;
use crate::error::Error;
use crate::gid::{GIDGen};
use crate::name::Name;
use crate::row::Row;
use crate::typedef::{Timestamp, Trace};
use crate::worker::{WorkerCommand, WorkerContext};

// TODO drop input/trace
pub enum CoordCommand {
    CreateInput {
        name: Name,
        tx: oneshot::Sender<Result<(), Error>>,
    },
    CreateDerive {
        name: Name,
        f: Arc<dyn for <'a> Fn(&mut WorkerContext<'a>) -> Option<Trace> + Send + Sync + 'static>,
        tx: oneshot::Sender<Result<(), Error>>,
    },
    Upsert {
        name: Name,
        key: Row,
        value: Option<Row>,
        tx: oneshot::Sender<Result<(), Error>>,
    },
    Query {
        name: Name,
        key: Row,
        tx: UnboundedSender<Result<Vec<Row>, Error>>,
    },
    // TODO: support gracefully shutdown
    Shutdown
}

pub struct Coord {
    epoch: Timestamp,
    gid_gen: GIDGen,
    catalog: Catalog,
    cmd_rx: UnboundedReceiver<CoordCommand>,
    // worker and channel
    worker_guards: WorkerGuards<()>,
    worker_txs: Vec<Sender<WorkerCommand>>,
}

impl Coord {
    pub async fn run(mut self) {
        loop {
            match self.cmd_rx.recv().await.unwrap() {
                CoordCommand::CreateInput { name, tx } => {
                    if let Err(e) = self.catalog.create_input(name.clone()) {
                        tx.send(Err(e)).unwrap();
                    } else {
                        let cmd = WorkerCommand::CreateInput {name};
                        self.broadcast(cmd);
                    }
                },
                CoordCommand::CreateDerive { name, f, tx } => {
                    if let Err(e) = self.catalog.create_trace(name.clone()) {
                        tx.send(Err(e)).unwrap();
                    } else {
                        let cmd = WorkerCommand::CreateDerive {name, f};
                        self.broadcast(cmd);
                    }
                },
                CoordCommand::Upsert { name, key, value, tx } => {
                    if self.catalog.input_exists(&name) {
                        tx.send(Err(Error::InputNotExists)).unwrap();
                    } else {
                        tx.send(Ok(())).unwrap();
                        let time = self.advance_epoch();
                        let c1 = WorkerCommand::Upsert {name, time, key, value};
                        let c2 = WorkerCommand::AdvanceInput {time: self.epoch};
                        self.broadcast_n([c1,c2]);
                    }
                },
                CoordCommand::Query { name, key, tx } => {
                    match self.catalog.determine_trace_worker(&name, &key) {
                        Ok(idx) =>  {
                            let time = self.query_time();
                            let gid = self.gid_gen.next();
                            let cmd = WorkerCommand::Query {gid, name, time, key, tx};
                            self.unicast(cmd, idx);
                        },
                        Err(e) => tx.send(Err(e)).unwrap(),
                    };
                },
                CoordCommand::Shutdown => {
                    let cmd = WorkerCommand::Shutdown;
                    self.broadcast(cmd);
                    break;
                }
            }
        }
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

    fn unicast(&mut self, cmd: WorkerCommand, idx: usize) {
        self.worker_txs[idx].send(cmd).unwrap();
        self.worker_guards.guards()[idx].thread().unpark();
    }

    fn broadcast(&mut self, cmd: WorkerCommand) {
        for tx in &mut self.worker_txs {
            tx.send(cmd.clone()).unwrap();
        }
        for handle in self.worker_guards.guards() {
            handle.thread().unpark();
        }
    }

    fn broadcast_n<const N: usize>(&mut self, cmds: [WorkerCommand; N]) {
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
