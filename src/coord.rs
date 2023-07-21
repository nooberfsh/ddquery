use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::oneshot;

use crate::catalog::Catalog;
use crate::error::Error;
use crate::name::Name;
use crate::row::Row;
use crate::timely::Timestamp;
use crate::worker::WorkerContext;

// TODO drop input/trace
pub enum CoordCommand {
    CreateInput {
        name: Name,
        tx: oneshot::Sender<Result<(), Error>>,
    },
    CreateDerive {
        name: Name,
        f: Box<dyn for <'a> Fn(&mut WorkerContext<'a>) -> anyhow::Result<Row> + Send + Sync + 'static>,
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
}

impl Coord {
    pub async fn run(mut self) {
        match self.cmd_rx.recv().await.unwrap() {
            CoordCommand::CreateInput { .. } => {}
            CoordCommand::CreateDerive { .. } => {}
            CoordCommand::Query { .. } => {}
            CoordCommand::Upsert { .. } => {}
            CoordCommand::Shutdown => {}
        }
    }
}
