use std::fmt::Debug;

use differential_dataflow::difference::Monoid;
use differential_dataflow::trace::cursor::IntoOwned;
use differential_dataflow::trace::{BatchReader, Cursor, TraceReader};
use timely::progress::Antichain;
use timely::PartialOrder;

type VecValTrace<K, V, T, R> = Vec<((K, V), Vec<(T, R)>)>;
type VecKeyTrace<K, T, R> = Vec<(K, Vec<(T, R)>)>;

pub fn trace_to_vec_val_trace<Tr, K, V>(trace: &mut Tr) -> VecValTrace<K, V, Tr::Time, Tr::Diff>
where
    Tr: TraceReader,
    for<'a> Tr::Key<'a>: IntoOwned<'a, Owned = K>,
    for<'a> Tr::Val<'a>: IntoOwned<'a, Owned = V>,
{
    let (mut cursor, storage) = trace.cursor();
    cursor.to_vec(&storage)
}

pub fn trace_to_vec_key_trace<Tr, K>(trace: &mut Tr) -> VecKeyTrace<K, Tr::Time, Tr::Diff>
where
    Tr: for<'a> TraceReader<Val<'a> = &'a ()>,
    for<'a> Tr::Key<'a>: IntoOwned<'a, Owned = K>,
{
    let data = trace_to_vec_val_trace(trace);
    data.into_iter().map(|((k, _), v)| (k, v)).collect()
}

pub fn is_empty_trace<Tr>(trace: &Tr) -> bool
where
    Tr: TraceReader,
{
    let mut count = 0;
    trace.map_batches(|b| count += b.len());
    count == 0
}

pub fn compact_vec_trace<D, T, R>(trace: Vec<(D, Vec<(T, R)>)>, upper: Antichain<T>) -> Vec<(D, R)>
where
    R: Monoid,
    T: PartialOrder,
{
    let mut ret = vec![];
    for (d, times) in trace {
        let mut count = R::zero();

        for (t, r) in times {
            if !upper.less_equal(&t) {
                count.plus_equals(&r);
            }
        }

        if !count.is_zero() {
            ret.push((d, count))
        }
    }
    ret
}

pub fn compact_unique_vec_trace<D, T, R>(
    trace: Vec<(D, Vec<(T, R)>)>,
    upper: Antichain<T>,
) -> Vec<D>
where
    R: Monoid + From<i8> + PartialEq + Debug,
    T: PartialOrder,
{
    let mut ret = vec![];
    for (d, times) in trace {
        let mut count = R::zero();

        for (t, r) in times {
            if !upper.less_equal(&t) {
                count.plus_equals(&r);
            }
        }

        let one: R = 1i8.into();
        if count == one {
            ret.push(d);
        } else {
            assert_eq!(count, R::zero())
        }
    }
    ret
}
