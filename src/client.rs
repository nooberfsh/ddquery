use std::sync::Arc;
use futures::Stream;

use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio::sync::oneshot;
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::coord::{CoordCommand};
use crate::error::Error;
use crate::name::Name;
use crate::row::Row;
use crate::timely::Trace;
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

    pub async fn create_derive(&self, name: Name, f: impl for<'a> Fn(&mut WorkerContext<'a>) -> Option<Trace> + Send + Sync + 'static) -> Result<(), Error> {
        let f = Arc::new(f);
        let (tx, rx) = oneshot::channel();
        let cmd = CoordCommand::CreateDerive {name, f, tx};
        self.cmd_tx.send(cmd).unwrap();
        rx.await.unwrap()
    }

    pub async fn query(&self, name: Name, key: Row) -> impl Stream<Item=Result<Vec<Row>, Error>> {
        let (tx, rx) = unbounded_channel();
        let cmd = CoordCommand::Query {name, key, tx};
        self.cmd_tx.send(cmd).unwrap();
        UnboundedReceiverStream::new(rx)
    }

    pub async fn insert(&self, name: Name, key: Row, value: Row) -> Result<(), Error> {
        let value = Some(value);
        let (tx, rx) = oneshot::channel();
        let cmd = CoordCommand::Upsert {name, key, value, tx};
        self.cmd_tx.send(cmd).unwrap();
        rx.await.unwrap()
    }

    pub async fn delete(&self, name: Name, key: Row) -> Result<(), Error> {
        let value = None;
        let (tx, rx) = oneshot::channel();
        let cmd = CoordCommand::Upsert {name, key, value, tx};
        self.cmd_tx.send(cmd).unwrap();
        rx.await.unwrap()
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        self.cmd_tx.send(CoordCommand::Shutdown).unwrap();
    }
}
