#[macro_export]
macro_rules! gen_tpch_app {
    ($n:expr, $($name:ident),*) => {

        paste::paste! {
            #[derive(Clone)]
            pub struct [<Q $n>];

            type Answer = [<Q $n Answer>];

            impl crate::TpchQuery for [<Q $n>] {
                type Answer = Answer;

                fn new() -> Self {
                    [<Q $n>]
                }

                fn load(handle: &Handle<Self>, path: &str, batch_size: usize) -> usize {
                    let mut batches = 0;

                    $(
                        let data = crate::util::load_input::<$name>(path, <$name as FileName>::FILE_NAME, batch_size).unwrap();
                        batches += data.len();
                        for batch in data {
                            handle.update(batch.into());
                        }
                    )*
                    batches
                }
            }
        }

        pub enum Update {
            $($name(Vec<$name>)),*
        }

        $(
        impl From<Vec<$name>> for Update {
            fn from(update: Vec<$name>) -> Update {
                Update::$name(update)
            }
        }
        )*

        impl Update {
            fn push_into(self, state: WorkerState<'_>)  {
                match self {
                    $(Update::$name(v) => {state.input_group.insert_batch::<$name>(v);})*
                }
            }
        }

        #[derive(Clone)]
        pub struct Query {
            pub sender: Sender<Vec<Answer>>,
        }

        impl Query {
            fn query(self, time: SysTime, state: WorkerState<'_>) {
                let mut trace = state
                    .trace_group
                    .get::<AnswerTrace<Answer>>()
                    .unwrap()
                    .clone();

                let task = move || {
                    if trace_beyond(&mut trace, &time) {
                        let data = collect_key_trace(&mut trace, &time);
                        let _ = self.sender.send(data);
                        PeekResult::Done
                    } else {
                        PeekResult::NotReady
                    }
                };
                state.peeks.push(Box::new(task));
            }
        }
    }
}
