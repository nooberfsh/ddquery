use std::sync::Arc;

use crossbeam_channel::{Receiver, Sender};
use timely::communication::WorkerGuards;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::oneshot;

use crate::catalog::Catalog;
use crate::error::Error;
use crate::name::Name;
use crate::row::Row;
use crate::timely::{Timestamp, Trace};
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
    },
    Query {
        name: Name,
        key: Row,
        tx: oneshot::Sender<Vec<Row>>
    },
    // TODO: support gracefully shutdown
    Shutdown
}

pub struct Coord {
    epoch: Timestamp,
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
                    if let Err(e) = self.catalog.create_input(name.clone()) {
                        tx.send(Err(e)).unwrap();
                    } else {
                        let cmd = WorkerCommand::CreateDerive {name, f};
                        self.broadcast(cmd);
                    }
                },
                CoordCommand::Upsert { .. } => {},
                CoordCommand::Query { .. } => {},
                CoordCommand::Shutdown => break,
            }
        }
    }

    fn broadcast(&mut self, cmd: WorkerCommand) {
        for tx in &mut self.worker_txs {
            tx.send(cmd.clone()).unwrap();
        }
        for handle in self.worker_guards.guards() {
            handle.thread().unpark();
        }
    }
}
