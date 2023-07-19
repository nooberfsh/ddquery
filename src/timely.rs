use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::trace::implementations::ord::{OrdKeySpine, OrdValSpine};

use crate::row::Row;

pub type Timestamp = u64;
pub type Spine<K, V, T, R> = OrdValSpine<K, V, T, R>;
pub type Trace = TraceAgent<Spine<Row, Row, Timestamp, i64>>;
