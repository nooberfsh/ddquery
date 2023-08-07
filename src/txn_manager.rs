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
    pub uuid: Uuid,
    pub time: Timestamp,
}

pub struct ReadToken {
    pub txn: ReadTxn,
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

    pub fn allocate_read_token(&mut self, time: Timestamp) -> Arc<ReadToken> {
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

    pub fn read_complete(&mut self, txn: ReadTxn) {
        let removed = self.read_txns.remove(&txn.uuid).unwrap();
        assert_eq!(removed.time, txn.time);
    }
}

impl Drop for ReadToken {
    fn drop(&mut self) {
        // TxnManager may ge dropped at this point.
        let _ = self.tx.send(self.txn.clone());
    }
}
