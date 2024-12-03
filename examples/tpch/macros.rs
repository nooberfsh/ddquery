#[macro_export]
macro_rules! gen_update {
    ($($name:ident),*) => {
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
    };
}

#[macro_export]
macro_rules! load_inputs {
    ($handle:expr, $path:expr, $batch_size:expr, $($name:ident),*) => {
        {
            let mut batches = 0;

            $(
                let data = load_input::<$name>($path, <$name as FileName>::FILE_NAME, $batch_size).unwrap();
                batches += data.len();
                for batch in data {
                    $handle.update(batch.into());
                }
            )*
            batches
        }
    };
}

#[macro_export]
macro_rules! gen_query {
    ($answer:ident) => {
        #[derive(Clone)]
        pub struct Query {
            pub sender: Sender<Vec<$answer>>,
        }

        impl Query {
            fn query(self, time: SysTime, state: WorkerState<'_>) {
                let mut trace = state
                    .trace_group
                    .get::<AnswerTrace<$answer>>()
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
    };
}
