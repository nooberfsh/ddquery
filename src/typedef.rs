use std::fmt::Debug;
use std::hash::Hash;

use abomonation::Abomonation;
use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::trace::implementations::ord::OrdValSpine;
use timely::communication::allocator::Generic;
use timely::dataflow::scopes::Child;
use trait_set::trait_set;

pub type Timestamp = u64;
pub type GenericWorker = timely::worker::Worker<Generic>;
pub type Scope<'a> = Child<'a, GenericWorker, Timestamp>;
pub type Spine<K, V> = OrdValSpine<K, V, Timestamp, isize>;
pub type Trace<K, V> = TraceAgent<Spine<K, V>>;

trait_set! {
    pub trait Data = Eq
    + PartialEq + Ord + PartialOrd + Send + Sync + Clone
    + Hash
    + Debug
    + Abomonation
    + 'static
    ;
}
