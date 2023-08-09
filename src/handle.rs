use std::sync::Arc;

use futures::Stream;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio::sync::oneshot;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::info;

use crate::coord::{CoordCommand};
use crate::error::Error;
use crate::name::Name;
use crate::typedef::{Data, Trace};
use crate::worker::{WorkerContext};

#[derive(Clone)]
pub struct Handle<K, V>
    where K: Data, V: Data
{
    inner: Arc<Inner<K, V>>,
}

struct Inner<K, V>
    where K: Data, V: Data
{
    cmd_tx: UnboundedSender<CoordCommand<K, V>>,
}

impl<K, V> Handle<K, V>
    where K: Data, V: Data
{
    pub (crate) fn new(cmd_tx: UnboundedSender<CoordCommand<K, V>>) -> Self {
        info!("create handle");
        let inner = Arc::new(Inner {cmd_tx});
        Handle{inner}
    }

    pub async fn create_input<N: Into<Name>>(&self, name: N) -> Result<(), Error> {
        let name = name.into();
        let (tx, rx) = oneshot::channel();
        let cmd = CoordCommand::CreateInput {name, tx};
        self.inner.cmd_tx.send(cmd).unwrap();
        rx.await.unwrap()
    }

    pub async fn create_derive<N: Into<Name>>(&self, name: N, f: impl for<'a> Fn(&mut WorkerContext<'a, K, V>) -> Option<Trace<K, V>> + Send + Sync + 'static) -> Result<(), Error> {
        let name = name.into();
        let f = Arc::new(f);
        let (tx, rx) = oneshot::channel();
        let cmd = CoordCommand::CreateDerive {name, f, tx};
        self.inner.cmd_tx.send(cmd).unwrap();
        rx.await.unwrap()
    }

    pub async fn query<N: Into<Name>>(&self, name: N, key: K) -> impl Stream<Item=Result<Vec<V>, Error>> {
        let name = name.into();
        let (tx, rx) = unbounded_channel();
        let cmd = CoordCommand::Query {name, key, tx};
        self.inner.cmd_tx.send(cmd).unwrap();
        UnboundedReceiverStream::new(rx)
    }

    pub async fn query_one<N: Into<Name>>(&self, name: N, key: K) -> Result<Option<V>, Error> {
        let name = name.into();
        let (tx, mut rx) = unbounded_channel();
        let cmd = CoordCommand::Query {name, key, tx};
        self.inner.cmd_tx.send(cmd).unwrap();
        if let Some(d) = rx.recv().await {
            d.map(|mut v| v.pop())
        } else {
            Ok(None)
        }
    }

    pub async fn insert<N: Into<Name>>(&self, name: N, key: K, value: V) -> Result<(), Error> {
        let name = name.into();
        let value = Some(value);
        let (tx, rx) = oneshot::channel();
        let cmd = CoordCommand::Upsert {name, key, value, tx};
        self.inner.cmd_tx.send(cmd).unwrap();
        rx.await.unwrap()
    }

    pub async fn delete<N: Into<Name>>(&self, name: N, key: K) -> Result<(), Error> {
        let name = name.into();
        let value = None;
        let (tx, rx) = oneshot::channel();
        let cmd = CoordCommand::Upsert {name, key, value, tx};
        self.inner.cmd_tx.send(cmd).unwrap();
        rx.await.unwrap()
    }
}

impl<K, V> Drop for Inner<K, V>
    where K: Data, V: Data
{
    fn drop(&mut self) {
        info!("shutdown ddquery");
        self.cmd_tx.send(CoordCommand::Shutdown).unwrap();
    }
}
