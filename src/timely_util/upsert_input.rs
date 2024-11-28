use std::any::{Any, TypeId};
use std::collections::HashMap;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::{upsert, Arranged, TraceAgent};
use differential_dataflow::trace::cursor::IntoOwned;
use differential_dataflow::trace::implementations::ord_neu::OrdValSpine;
use differential_dataflow::trace::{Batch, Builder, Trace, TraceReader};
use differential_dataflow::{Collection, ExchangeData, Hashable};
use timely::dataflow::operators::Input;
use timely::dataflow::{InputHandle, Scope};
use timely::order::TotalOrder;
use timely::progress::Timestamp;

pub trait UpsertInput {
    type Key;

    fn get_key(&self) -> Self::Key;
}

struct Bundle<T> {
    handle: Box<dyn Any>,
    advance_fn: Box<dyn Fn(&mut Box<dyn Any>, T)>,
}

pub struct UpsertInputGroup<T> {
    inputs: HashMap<TypeId, Bundle<T>>,
}

impl<T> UpsertInputGroup<T>
where
    T: Timestamp,
{
    pub fn new() -> Self {
        UpsertInputGroup {
            inputs: HashMap::new(),
        }
    }

    pub fn register<U: UpsertInput>(&mut self, handle: InputHandle<T, (U::Key, Option<U>, T)>)
    where
        U::Key: Clone + 'static,
        U: Clone + 'static,
    {
        let tid = TypeId::of::<U>();
        let handle = Box::new(handle);
        let advance_fn = Box::new(|any: &mut Box<dyn Any>, t: T| {
            let handle: &mut InputHandle<T, (U::Key, Option<U>, T)> = any.downcast_mut().unwrap();
            handle.advance_to(t);
        });
        let bundle = Bundle { handle, advance_fn };
        let d = self.inputs.insert(tid, bundle);
        assert!(d.is_none(), "register same InputHandle");
    }

    pub fn get<U: UpsertInput>(&self) -> Option<&InputHandle<T, (U::Key, Option<U>, T)>>
    where
        U::Key: Clone + 'static,
        U: Clone + 'static,
    {
        let tid = TypeId::of::<U>();
        let bundle = self.inputs.get(&tid)?;
        Some(bundle.handle.downcast_ref().unwrap())
    }

    pub fn get_mut<U: UpsertInput>(&mut self) -> Option<&mut InputHandle<T, (U::Key, Option<U>, T)>>
    where
        U::Key: Clone + 'static,
        U: Clone + 'static,
    {
        let tid = TypeId::of::<U>();
        let bundle = self.inputs.get_mut(&tid)?;
        Some(bundle.handle.downcast_mut().unwrap())
    }

    pub fn upsert<U: UpsertInput>(&mut self, value: U)
    where
        U::Key: Clone + 'static,
        U: Clone + 'static,
    {
        let key = value.get_key();
        let handle = self.get_mut::<U>().expect("not registered");
        let time = handle.time();
        handle.send((key, Some(value), time.clone()));
    }

    fn get_arrange_named<U: UpsertInput, Tr, G>(
        &mut self,
        scope: &mut G,
        name: &str,
    ) -> Option<Arranged<G, TraceAgent<Tr>>>
    where
        G: Scope<Timestamp = T>,
        Tr: Trace + TraceReader<Time = T, Diff = isize> + 'static,
        for<'a> Tr::Key<'a>: IntoOwned<'a, Owned = U::Key>,
        U::Key: ExchangeData + Hashable + std::hash::Hash,
        U: ExchangeData,
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

    pub fn alloc_collection<U: UpsertInput, G>(&mut self, scope: &mut G) -> Collection<G, U>
    where
        G: Scope<Timestamp = T>,
        U::Key: ExchangeData + Hashable + std::hash::Hash,
        U: ExchangeData,
        T: TotalOrder + ExchangeData + Lattice,
    {
        let input: InputHandle<T, (U::Key, Option<U>, T)> = InputHandle::new();
        self.register(input);
        self.get_collection(scope).unwrap()
    }

    fn get_collection<U: UpsertInput, G>(&mut self, scope: &mut G) -> Option<Collection<G, U>>
    where
        G: Scope<Timestamp = T>,
        U::Key: ExchangeData + Hashable + std::hash::Hash,
        U: ExchangeData,
        T: TotalOrder + ExchangeData + Lattice,
    {
        let arranged = self
            .get_arrange_named::<U, OrdValSpine<_, _, _, _>, G>(scope, "UpsertInputToCollection")?;
        Some(arranged.as_collection(|_k, v| v.clone()))
    }

    pub fn delete<U: UpsertInput>(&mut self, key: U::Key)
    where
        U::Key: Clone + 'static,
        U: Clone + 'static,
    {
        let handle = self.get_mut::<U>().expect("not registered");
        let time = handle.time();
        handle.send((key, None, time.clone()));
    }

    pub fn advance_to(&mut self, frontier: T) {
        for (_, bundle) in &mut self.inputs {
            (bundle.advance_fn)(&mut bundle.handle, frontier.clone());
        }
    }
}
