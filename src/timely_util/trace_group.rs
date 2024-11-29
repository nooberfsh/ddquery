use std::any::{type_name, Any, TypeId};
use std::collections::BTreeMap;

use differential_dataflow::trace::TraceReader;
use timely::progress::{Antichain, Timestamp};

struct Bundle<T> {
    trace: Box<dyn Any>,
    name: String,
    physical_compaction_fn: Box<dyn Fn(&mut Box<dyn Any>)>,
    logical_compaction_fn: Box<dyn Fn(&mut Box<dyn Any>, T)>,
    get_compaction_fn: Box<dyn Fn(&mut Box<dyn Any>) -> (Antichain<T>, Antichain<T>)>,
}

pub(crate) struct BundleInfo<T> {
    pub(crate) name: String,
    pub(crate) physical_compaction: Antichain<T>,
    pub(crate) logical_compaction: Antichain<T>,
}

impl<T: Clone> Bundle<T> {
    fn new<Tr>(trace: Tr, name: String) -> Self
    where
        Tr: TraceReader<Time = T> + 'static,
    {
        let trace = Box::new(trace);
        let physical_compaction_fn = Box::new(|any: &mut Box<dyn Any>| {
            let trace: &mut Tr = any.downcast_mut().unwrap();
            let mut upper = Antichain::new();
            trace.read_upper(&mut upper);
            trace.set_physical_compaction(upper.borrow())
        });
        let logical_compaction_fn = Box::new(|any: &mut Box<dyn Any>, frontier: T| {
            let trace: &mut Tr = any.downcast_mut().unwrap();
            let upper = Antichain::from_elem(frontier);
            trace.set_logical_compaction(upper.borrow())
        });
        let get_compaction_fn = Box::new(|any: &mut Box<dyn Any>| {
            let trace: &mut Tr = any.downcast_mut().unwrap();
            let logical = trace.get_logical_compaction().to_owned();
            let physical = trace.get_physical_compaction().to_owned();
            (logical, physical)
        });
        Bundle {
            trace,
            name,
            physical_compaction_fn,
            logical_compaction_fn,
            get_compaction_fn,
        }
    }
}

pub struct TraceGroup<T> {
    traces: BTreeMap<TypeId, Bundle<T>>,
}

impl<T> TraceGroup<T>
where
    T: Timestamp,
{
    pub fn new() -> Self {
        TraceGroup {
            traces: BTreeMap::new(),
        }
    }

    pub fn register_trace<Tr>(&mut self, trace: Tr)
    where
        Tr: TraceReader<Time = T> + 'static,
    {
        let tid = TypeId::of::<Tr>();
        let name = {
            let key_name = type_name::<Tr::Key<'_>>();
            let value_name = type_name::<Tr::Val<'_>>();
            let time_name = type_name::<Tr::Time>();
            let diff_name = type_name::<Tr::Diff>();
            format!("Trace[{key_name},{value_name},{time_name},{diff_name}]")
        };

        let bundle = Bundle::new::<Tr>(trace, name);
        let d = self.traces.insert(tid, bundle);
        assert!(d.is_none(), "register same trace")
    }

    pub fn get<Tr>(&self) -> Option<&Tr>
    where
        Tr: TraceReader<Time = T> + 'static,
    {
        let tid = TypeId::of::<Tr>();
        let bundle = self.traces.get(&tid)?;
        Some(bundle.trace.downcast_ref().unwrap())
    }

    pub fn get_mut<Tr>(&mut self) -> Option<&mut Tr>
    where
        Tr: TraceReader<Time = T> + 'static,
    {
        let tid = TypeId::of::<Tr>();
        let bundle = self.traces.get_mut(&tid)?;
        Some(bundle.trace.downcast_mut().unwrap())
    }

    pub fn physical_compaction(&mut self) {
        for bundle in self.traces.values_mut() {
            (bundle.physical_compaction_fn)(&mut bundle.trace)
        }
    }

    pub fn logical_compaction(&mut self, frontier: T) {
        for bundle in self.traces.values_mut() {
            (bundle.logical_compaction_fn)(&mut bundle.trace, frontier.clone())
        }
    }

    pub(crate) fn collect_info(&mut self) -> Vec<BundleInfo<T>> {
        let mut ret = vec![];
        for bundle in self.traces.values_mut() {
            let (logical, physical) = (bundle.get_compaction_fn)(&mut bundle.trace);
            ret.push(BundleInfo {
                name: bundle.name.clone(),
                logical_compaction: logical,
                physical_compaction: physical,
            });
        }
        ret
    }
}
