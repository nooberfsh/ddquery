use differential_dataflow::Collection;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;

use crate::coord::{TraceManager};
use crate::error::Error;
use crate::name::Name;
use crate::row::Row;
use crate::timely::{Timestamp};
use crate::worker::{WorkerContext};

pub struct Client {
    cmd_tx: UnboundedSender<ClientCommand>,
}

impl Client {
    pub async fn create_input(&self, name: Name) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        let cmd = ClientCommand::CreateInput {name, tx};
        self.cmd_tx.send(cmd).unwrap();
        rx.await.unwrap()
    }

    pub async fn create_derive(&self, name: Name, f: impl for<'a> Fn(&mut WorkerContext<'a>) -> anyhow::Result<Row> + Send + Sync + 'static) -> Result<(), Error> {
        let f = Box::new(f);
        let (tx, rx) = oneshot::channel();
        let cmd = ClientCommand::CreateDerive {name, f, tx};
        self.cmd_tx.send(cmd).unwrap();
        rx.await.unwrap()
    }

    pub async fn query(&self, name: Name, key: Row) -> Result<Option<Row>, Error> {
        let (tx, rx) = oneshot::channel();
        let cmd = ClientCommand::Query {name, key, tx};
        self.cmd_tx.send(cmd).unwrap();
        rx.await.unwrap()
    }

    pub async fn insert(&self, name: Name, key: Row, value: Row) -> Result<(), Error> {
        let value = Some(value);
        let cmd = ClientCommand::Upsert {name, key, value};
        self.cmd_tx.send(cmd).unwrap();
        Ok(())
    }

    pub async fn delete(&self, name: Name, key: Row) -> Result<(), Error> {
        let value = None;
        let cmd = ClientCommand::Upsert {name, key, value};
        self.cmd_tx.send(cmd).unwrap();
        Ok(())
    }
}

// TODO drop input/trace
enum ClientCommand {
    CreateInput {
        name: Name,
        tx: oneshot::Sender<Result<(), Error>>,
    },
    CreateDerive {
        name: Name,
        f: Box<dyn for <'a> Fn(&mut WorkerContext<'a>) -> anyhow::Result<Row> + Send + Sync + 'static>,
        tx: oneshot::Sender<Result<(), Error>>,
    },
    Query {
        name: Name,
        key: Row,
        tx: oneshot::Sender<Result<Option<Row>, Error>>,
    },
    Upsert {
        name: Name,
        key: Row,
        value: Option<Row>,
    },
    // TODO: support gracefully shutdown
    Shutdown
}

impl Drop for Client {
    fn drop(&mut self) {
        self.cmd_tx.send(ClientCommand::Shutdown).unwrap();
    }
}
