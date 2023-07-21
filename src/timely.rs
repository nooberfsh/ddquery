use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::trace::implementations::ord::OrdValSpine;
use timely::communication::allocator::Generic;
use timely::dataflow::scopes::Child;

use crate::row::Row;

pub type Timestamp = u64;
pub type GenericWorker = timely::worker::Worker<Generic>;
pub type Scope<'a> = Child<'a, GenericWorker, Timestamp>;
pub type Spine<K, V> = OrdValSpine<K, V, Timestamp, isize>;
pub type Trace = TraceAgent<Spine<Row, Row>>;
