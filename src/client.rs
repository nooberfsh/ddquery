use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;

use crate::coord::{CoordCommand};
use crate::error::Error;
use crate::name::Name;
use crate::row::Row;
use crate::worker::{WorkerContext};

pub struct Client {
    cmd_tx: UnboundedSender<CoordCommand>,
}

impl Client {
    pub async fn create_input(&self, name: Name) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        let cmd = CoordCommand::CreateInput {name, tx};
        self.cmd_tx.send(cmd).unwrap();
        rx.await.unwrap()
    }

    pub async fn create_derive(&self, name: Name, f: impl for<'a> Fn(&mut WorkerContext<'a>) -> anyhow::Result<Row> + Send + Sync + 'static) -> Result<(), Error> {
        let f = Box::new(f);
        let (tx, rx) = oneshot::channel();
        let cmd = CoordCommand::CreateDerive {name, f, tx};
        self.cmd_tx.send(cmd).unwrap();
        rx.await.unwrap()
    }

    pub async fn query(&self, name: Name, key: Row) -> Vec<Row> {
        let (tx, rx) = oneshot::channel();
        let cmd = CoordCommand::Query {name, key, tx};
        self.cmd_tx.send(cmd).unwrap();
        rx.await.unwrap()
    }

    pub async fn insert(&self, name: Name, key: Row, value: Row) -> Result<(), Error> {
        let value = Some(value);
        let cmd = CoordCommand::Upsert {name, key, value};
        self.cmd_tx.send(cmd).unwrap();
        Ok(())
    }

    pub async fn delete(&self, name: Name, key: Row) -> Result<(), Error> {
        let value = None;
        let cmd = CoordCommand::Upsert {name, key, value};
        self.cmd_tx.send(cmd).unwrap();
        Ok(())
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        self.cmd_tx.send(CoordCommand::Shutdown).unwrap();
    }
}
