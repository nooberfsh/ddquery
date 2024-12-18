use std::any::{type_name, Any, TypeId};
use std::collections::BTreeMap;
use std::marker::PhantomData;

use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::{upsert, Arranged, TraceAgent};
use differential_dataflow::trace::cursor::IntoOwned;
use differential_dataflow::trace::implementations::ord_neu::OrdValSpine;
use differential_dataflow::trace::{Batch, Builder, Trace, TraceReader};
use differential_dataflow::AsCollection;
use differential_dataflow::{Collection, ExchangeData, Hashable};
use timely::dataflow::operators::{Input, Map};
use timely::dataflow::{InputHandle, Scope};
use timely::order::TotalOrder;
use timely::progress::Timestamp;

pub trait UpsertInput {
    type Key;

    fn get_key(&self) -> Self::Key;
}

struct Bundle<T> {
    name: &'static str,
    handle: Box<dyn Any>,
    advance_fn: Box<dyn Fn(&mut Box<dyn Any>, T)>,
    get_time_fn: Box<dyn Fn(&mut Box<dyn Any>) -> T>,
}

pub(crate) struct BundleInfo<T> {
    pub(crate) name: &'static str,
    pub(crate) time: T,
}

pub struct UpsertInputGroup<T, R> {
    inputs: BTreeMap<TypeId, Bundle<T>>,
    _marker: PhantomData<R>,
}

impl<T, R> UpsertInputGroup<T, R>
where
    T: Timestamp,
    R: Semigroup + 'static,
{
    pub fn new() -> Self {
        UpsertInputGroup {
            inputs: BTreeMap::new(),
            _marker: PhantomData,
        }
    }

    pub fn register<U>(&mut self, handle: InputHandle<T, (U::Key, Option<U>, T)>)
    where
        U::Key: Clone + 'static,
        U: UpsertInput + Clone + 'static,
    {
        let tid = TypeId::of::<U>();
        let name = type_name::<U>();
        let handle = Box::new(handle);
        let advance_fn = Box::new(|any: &mut Box<dyn Any>, t: T| {
            let handle: &mut InputHandle<T, (U::Key, Option<U>, T)> = any.downcast_mut().unwrap();
            handle.advance_to(t);
        });
        let get_time_fn = Box::new(|any: &mut Box<dyn Any>| {
            let handle: &mut InputHandle<T, (U::Key, Option<U>, T)> = any.downcast_mut().unwrap();
            handle.time().clone()
        });
        let bundle = Bundle {
            name,
            handle,
            advance_fn,
            get_time_fn,
        };
        let d = self.inputs.insert(tid, bundle);
        assert!(d.is_none(), "register same InputHandle");
    }

    pub fn get<U>(&self) -> Option<&InputHandle<T, (U::Key, Option<U>, T)>>
    where
        U::Key: Clone + 'static,
        U: UpsertInput + Clone + 'static,
    {
        let tid = TypeId::of::<U>();
        let bundle = self.inputs.get(&tid)?;
        Some(bundle.handle.downcast_ref().unwrap())
    }

    pub fn get_mut<U>(&mut self) -> Option<&mut InputHandle<T, (U::Key, Option<U>, T)>>
    where
        U::Key: Clone + 'static,
        U: UpsertInput + Clone + 'static,
    {
        let tid = TypeId::of::<U>();
        let bundle = self.inputs.get_mut(&tid)?;
        Some(bundle.handle.downcast_mut().unwrap())
    }

    pub fn upsert<U>(&mut self, value: U)
    where
        U::Key: Clone + 'static,
        U: UpsertInput + Clone + 'static,
    {
        let key = value.get_key();
        let handle = self.get_mut::<U>().expect("not registered");
        let time = handle.time();
        handle.send((key, Some(value), time.clone()));
    }

    fn get_arrange_named<U, Tr, G>(
        &mut self,
        scope: &mut G,
        name: &str,
    ) -> Option<Arranged<G, TraceAgent<Tr>>>
    where
        G: Scope<Timestamp = T>,
        Tr: Trace + TraceReader<Time = T, Diff = isize> + 'static,
        for<'a> Tr::Key<'a>: IntoOwned<'a, Owned = U::Key>,
        U::Key: ExchangeData + Hashable + std::hash::Hash,
        U: UpsertInput + ExchangeData,
        for<'a> Tr::Val<'a>: IntoOwned<'a, Owned = U>,
        T: TotalOrder + ExchangeData + Lattice,
        Tr::Batch: Batch,
        Tr::Builder: Builder<Input = Vec<((U::Key, U), Tr::Time, Tr::Diff)>>,
    {
        let input = self.get_mut::<U>()?;
        let stream = scope.input_from(input);
        Some(upsert::arrange_from_upsert::<G, U::Key, U, Tr>(
            &stream, name,
        ))
    }

    pub fn alloc_collection<U, G>(&mut self, scope: &mut G) -> Collection<G, U, R>
    where
        G: Scope<Timestamp = T>,
        U::Key: ExchangeData + Hashable + std::hash::Hash,
        U: UpsertInput + ExchangeData,
        T: TotalOrder + ExchangeData + Lattice,
        R: From<i64>,
    {
        let input: InputHandle<T, (U::Key, Option<U>, T)> = InputHandle::new();
        self.register(input);
        self.get_collection(scope).unwrap()
    }

    fn get_collection<U, G>(&mut self, scope: &mut G) -> Option<Collection<G, U, R>>
    where
        G: Scope<Timestamp = T>,
        U::Key: ExchangeData + Hashable + std::hash::Hash,
        U: UpsertInput + ExchangeData,
        T: TotalOrder + ExchangeData + Lattice,
        R: From<i64>,
    {
        let arranged = self
            .get_arrange_named::<U, OrdValSpine<_, _, _, _>, G>(scope, "UpsertInputToCollection")?;
        let collection = arranged.as_collection(|_k, v| v.clone());
        Some(
            collection
                .inner
                .map(|(u, t, r)| {
                    let d: i64 = r.try_into().expect("failed to convert isize diff to i64");
                    (u, t, d.into())
                })
                .as_collection(),
        )
    }

    pub fn delete<U>(&mut self, key: U::Key)
    where
        U::Key: Clone + 'static,
        U: UpsertInput + Clone + 'static,
    {
        let handle = self.get_mut::<U>().expect("not registered");
        let time = handle.time();
        handle.send((key, None, time.clone()));
    }

    pub fn advance_to(&mut self, frontier: T) {
        for bundle in self.inputs.values_mut() {
            (bundle.advance_fn)(&mut bundle.handle, frontier.clone());
        }
    }

    pub(crate) fn collect_info(&mut self) -> Vec<BundleInfo<T>> {
        let mut ret = vec![];
        for bundle in self.inputs.values_mut() {
            let time = (bundle.get_time_fn)(&mut bundle.handle);
            ret.push(BundleInfo {
                name: bundle.name,
                time,
            });
        }
        ret
    }
}
