use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crossbeam_channel::Receiver;
use differential_dataflow::trace::{Cursor, TraceReader};
use timely::dataflow::InputHandle;
use timely::{PartialOrder, WorkerConfig};
use timely::communication::WorkerGuards;
use timely::progress::Antichain;
use timely::progress::frontier::AntichainRef;
use tokio::sync::mpsc::UnboundedSender;

use crate::error::Error;
use crate::gid::GID;
use crate::name::Name;
use crate::row::Row;
use crate::typedef::{GenericWorker, Timestamp, Trace};

#[derive(Clone)]
pub enum WorkerCommand {
    CreateInput {
        name: Name,
    },
    CreateDerive {
        name: Name,
        f: Arc<dyn for <'a> Fn(&mut WorkerContext<'a>) -> Option<Trace> + Send + Sync + 'static>,
    },
    Upsert {
        name: Name,
        time: Timestamp,
        key: Row,
        value: Option<Row>,
    },
    Query {
        gid: GID,
        name: Name,
        time: Timestamp,
        key: Row,
        tx: UnboundedSender<Result<Vec<Row>, Error>>,
    },
    AdvanceInput {
        time: Timestamp,
    },
    AllowCompaction(Vec<(Name, Timestamp)>),
    Shutdown,
}

pub struct WorkerContext<'a> {
    pub worker: &'a mut GenericWorker,
    pub state: &'a WorkerState,
}

#[derive(Default)]
pub struct WorkerState {
    pub inputs: HashMap<Name, InputHandle<Timestamp, (Row, Option<Row>, Timestamp)>>,
    pub trace: HashMap<Name, Trace>,
}

pub struct Worker<'a>
{
    worker_id: usize,
    state: WorkerState,
    pending_queries: Vec<PendingQuery>,
    cmd_rx: Receiver<WorkerCommand>,
    worker: &'a mut GenericWorker,
}

pub struct Config {
    pub cmd_rxs: Vec<Receiver<WorkerCommand>>,
    pub timely_worker: WorkerConfig,
}

pub fn serve(config: Config) -> Result<WorkerGuards<()>, Error> {
    let workers = config.cmd_rxs.len();
    assert!(workers > 0);

    let command_rxs: Mutex<Vec<_>> =
        Mutex::new(config.cmd_rxs.into_iter().map(Some).collect());

    let tokio_executor = tokio::runtime::Handle::current();
    timely::execute::execute(
        timely::Config {
            communication: timely::CommunicationConfig::Process(workers),
            worker: config.timely_worker,
        },
        move |timely_worker| {
            assert_eq!(timely_worker.peers(), workers);

            let _tokio_guard = tokio_executor.enter();
            let cmd_rx = command_rxs.lock().unwrap()[timely_worker.index()]
                .take()
                .unwrap();
            let worker_id = timely_worker.index();
            Worker {
                worker_id,
                state: WorkerState::default(),
                pending_queries: vec![],
                cmd_rx,
                worker: timely_worker,
            }
            .run()
        },
    ).map_err(Error::FailedToStartWorkers)
}

impl<'a> Worker<'a>
{
    fn run(mut self) {
        loop {
            self.maintenance();
            self.worker.step_or_park(None);

            self.process_pending_queries();

            let cmds: Vec<_> = self.cmd_rx.try_iter().collect();
            for cmd in cmds {
                if let WorkerCommand::Shutdown = cmd {
                    break;
                }
                self.handle_command(cmd);
            }
        }
    }

    fn init_log(&mut self) {
        todo!()
    }

    fn maintenance(&mut self) {
        for (_, trace) in &mut self.state.trace {
            let mut antichain = Antichain::new();
            trace.read_upper(&mut antichain);
            trace.set_physical_compaction(antichain.borrow());
        }
    }

    fn process_pending_queries(&mut self) {
        for query in &mut self.pending_queries {
            query.attempt();
        }
        self.pending_queries.retain(|q| !q.finished());
    }

    fn allow_compaction(&mut self, frontier: Vec<(Name, Timestamp)>) {
        for (name, time) in frontier {
            let trace = self.state.trace.get_mut(&name).unwrap();
            trace.set_logical_compaction(AntichainRef::new(&[time]));
        }
    }

    fn ctx(&mut self) -> WorkerContext<'_> {
        WorkerContext {
            worker: &mut *self.worker,
            state: &self.state
        }
    }

    fn handle_command(&mut self, cmd: WorkerCommand) {
        match cmd {
            WorkerCommand::CreateInput { name} => {
                let input = InputHandle::new();
                self.state.inputs.insert(name, input);
            },
            WorkerCommand::CreateDerive {name, f} => {
                if let Some(trace) = f(&mut self.ctx()) {
                    self.state.trace.insert(name, trace);
                }
            },
            WorkerCommand::Query {gid, name, time, key, tx} => {
                let mut trace = self.state.trace.get(&name).unwrap().clone();
                trace.set_logical_compaction(AntichainRef::new(&[time.clone()]));
                trace.set_physical_compaction(AntichainRef::new(&[]));
                let mut query = PendingQuery{
                    gid,
                    name,
                    time,
                    key,
                    trace,
                    tx: Some(tx),
                };
                if !query.attempt() {
                    self.pending_queries.push(query);
                }
            }
            WorkerCommand::Upsert {name, time, key, value} => {
                let input = self.state.inputs.get_mut(&name).unwrap();
                input.send((key, value, time));
            },
            WorkerCommand::AdvanceInput {time} => {
                for input in self.state.inputs.values_mut() {
                    input.advance_to(time.clone())
                }
            },
            WorkerCommand::AllowCompaction(frontier) => {
                self.allow_compaction(frontier);
            },
            WorkerCommand::Shutdown => unreachable!(),
        }
    }
}

struct PendingQuery {
    gid: GID,
    name: Name,
    time: Timestamp,
    key: Row,
    trace: Trace,
    tx: Option<UnboundedSender<Result<Vec<Row>, Error>>>,
}

impl PendingQuery {
    fn finished(&self) -> bool {
        self.tx.is_none()
    }

    fn attempt(&mut self) -> bool {
        let mut upper = Antichain::new();
        self.trace.read_upper(&mut upper);
        if upper.less_equal(&self.time) {
            return false
        }

        let ret = read_key(&mut self.trace, &self.key, &self.time);
        self.tx.take().unwrap().send(Ok(ret)).unwrap();

        true
    }
}

fn read_key(trace: &mut Trace, key: &Row, time: &Timestamp) -> Vec<Row> {
    let mut ret = vec![];
    let (mut cursor, storage) = trace.cursor();
    cursor.seek_key(&storage, key);
    if cursor.get_key(&storage) == Some(key) {
        while let Some(val) = cursor.get_val(&storage) {
            let mut count = 0;
            cursor.map_times(&storage, |dtime, diff| {
                if dtime.less_equal(&time) {
                    count += *diff
                }
            });
            assert!(count >= 0);
            for _ in 0..count {
                ret.push(val.clone());
            }
            cursor.step_val(&storage);
        }
    }
    ret
}
