use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::mpsc::{unbounded_channel, UnboundedSender, UnboundedReceiver};
use uuid::Uuid;

use crate::typedef::Timestamp;

pub struct TxnManager {
    pub (crate) read_txns: HashMap<Uuid, ReadTxn>,
    // report finished read
    pub (crate) tx: UnboundedSender<ReadTxn>,
    pub (crate) rx: UnboundedReceiver<ReadTxn>,
}

#[derive(Clone)]
pub struct ReadTxn {
    uuid: Uuid,
    time: Timestamp,
}

pub struct ReadToken {
    txn: ReadTxn,
    tx: UnboundedSender<ReadTxn>,
}

impl TxnManager {
    pub fn new() -> Self {
        let (tx, rx) = unbounded_channel();
        TxnManager {
            read_txns: HashMap::new(),
            tx,
            rx,
        }
    }

    pub fn allocate(&mut self, time: Timestamp) -> Arc<ReadToken> {
        let txn = ReadTxn {
            uuid: Uuid::new_v4(),
            time,
        };
        self.read_txns.insert(txn.uuid.clone(), txn.clone());
        Arc::new(ReadToken {
            txn,
            tx: self.tx.clone(),
        })
    }
}

impl Drop for ReadToken {
    fn drop(&mut self) {
        // TxnManager may ge dropped at this point.
        let _ = self.tx.send(self.txn.clone());
    }
}
