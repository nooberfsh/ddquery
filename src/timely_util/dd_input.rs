use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::fmt::Debug;
use std::marker::PhantomData;

use differential_dataflow::difference::Semigroup;
use differential_dataflow::input::InputSession;
use differential_dataflow::Collection;
use timely::dataflow::operators::Input as TimelyInput;
use timely::progress::Timestamp;

struct Bundle<T> {
    handle: Box<dyn Any>,
    advance_fn: Box<dyn Fn(&mut Box<dyn Any>, T)>,
    flush_fn: Box<dyn Fn(&mut Box<dyn Any>)>,
}

pub struct DDInputGroup<T, R> {
    inputs: HashMap<TypeId, Bundle<T>>,
    _marker: PhantomData<R>,
}

impl<T, R> DDInputGroup<T, R>
where
    T: Timestamp,
    R: Semigroup + 'static,
{
    pub fn new() -> Self {
        DDInputGroup {
            inputs: HashMap::new(),
            _marker: PhantomData,
        }
    }

    pub fn alloc<D>(&mut self)
    where
        D: Clone + Ord + Debug + 'static,
    {
        let input: InputSession<T, D, R> = InputSession::new();
        self.register(input);
    }

    pub fn register<D>(&mut self, handle: InputSession<T, D, R>)
    where
        D: Clone + Ord + Debug + 'static,
    {
        let tid = TypeId::of::<D>();
        let handle = Box::new(handle);
        let advance_fn = Box::new(|any: &mut Box<dyn Any>, t: T| {
            let handle: &mut InputSession<T, D, R> = any.downcast_mut().unwrap();
            handle.advance_to(t);
        });
        let flush_fn = Box::new(move |any: &mut Box<dyn Any>| {
            let handle: &mut InputSession<T, D, R> = any.downcast_mut().unwrap();
            handle.flush();
        });
        let bundle = Bundle {
            handle,
            advance_fn,
            flush_fn,
        };
        let d = self.inputs.insert(tid, bundle);
        assert!(d.is_none(), "register same InputSession");
    }

    pub fn get<D>(&self) -> Option<&InputSession<T, D, R>>
    where
        D: Clone + Ord + Debug + 'static,
    {
        let tid = TypeId::of::<D>();
        let bundle = self.inputs.get(&tid)?;
        Some(bundle.handle.downcast_ref().unwrap())
    }

    pub fn get_mut<D>(&mut self) -> Option<&mut InputSession<T, D, R>>
    where
        D: Clone + Ord + Debug + 'static,
    {
        let tid = TypeId::of::<D>();
        let bundle = self.inputs.get_mut(&tid)?;
        Some(bundle.handle.downcast_mut().unwrap())
    }

    pub fn update<D>(&mut self, value: D, change: R)
    where
        D: Clone + Ord + Debug + 'static,
    {
        let handle = self.get_mut::<D>().expect("not registered");
        handle.update(value, change);
    }

    pub fn update_at<D>(&mut self, value: D, time: T, change: R)
    where
        D: Clone + Ord + Debug + 'static,
    {
        let handle = self.get_mut::<D>().expect("not registered");
        handle.update_at(value, time, change);
    }

    pub fn get_collection<D, G>(&mut self, scope: &mut G) -> Option<Collection<G, D, R>>
    where
        G: TimelyInput<Timestamp = T>,
        D: Clone + Ord + Debug + 'static,
    {
        let handle = self.get_mut::<D>()?;
        Some(handle.to_collection(scope))
    }

    pub fn advance_and_flush(&mut self, frontier: T) {
        for (_, bundle) in &mut self.inputs {
            (bundle.advance_fn)(&mut bundle.handle, frontier.clone());
            (bundle.flush_fn)(&mut bundle.handle)
        }
    }
}
