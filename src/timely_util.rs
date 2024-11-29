use std::fmt::Debug;

use differential_dataflow::trace::cursor::IntoOwned;
use differential_dataflow::trace::{Cursor, TraceReader};
use timely::progress::Antichain;
use timely::PartialOrder;

pub mod dd_input;
pub mod trace_group;
pub mod upsert_input;

pub fn trace_beyond<T, Tr>(tr: &mut Tr, time: &T) -> bool
where
    Tr: TraceReader<Time = T> + 'static,
    T: PartialOrder,
{
    let mut frontier = Antichain::new();
    tr.read_upper(&mut frontier);
    !frontier.less_equal(time)
}

pub fn collect_key_trace<Tr, K, T>(trace: &mut Tr, time: &T) -> Vec<K>
where
    for<'a> Tr: TraceReader<Time = T, TimeGat<'a> = &'a T> + 'static,
    for<'a> Tr::Key<'a>: IntoOwned<'a, Owned = K> + Debug,
    for<'a> Tr::Val<'a>: IntoOwned<'a, Owned = ()>,
    for<'a> Tr::DiffGat<'a>: IntoOwned<'a, Owned = i64>,
    T: PartialOrder,
{
    assert!(trace_beyond(trace, time));
    assert!(trace.get_logical_compaction().less_equal(time));

    let mut ret = vec![];
    let (mut cursor, storage) = trace.cursor();

    while cursor.key_valid(&storage) {
        let key = cursor.key(&storage);
        let mut count: i64 = 0;
        cursor.map_times(&storage, |dtime, diff| {
            if dtime.less_equal(time) {
                count += diff.into_owned();
            }
        });
        if count < 0 {
            panic!("key: `{:?}` count < 0", key);
        } else if count == 0 {
            // do nothing
        } else {
            for _ in 0..count {
                ret.push(key.into_owned())
            }
        }

        cursor.step_key(&storage);
    }
    ret
}
