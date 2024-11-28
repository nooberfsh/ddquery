use differential_dataflow::trace::TraceReader;
use timely::progress::Antichain;
use timely::PartialOrder;

pub mod dd_input;
pub mod trace_group;
pub mod upsert_input;

pub fn trace_beyond<T, Tr>(tr: &mut Tr, t: &T) -> bool
where
    Tr: TraceReader<Time = T> + 'static,
    T: PartialOrder,
{
    let mut frontier = Antichain::new();
    tr.read_upper(&mut frontier);
    !frontier.less_equal(t)
}
