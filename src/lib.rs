#![allow(clippy::type_complexity)]
#![allow(clippy::new_without_default)]

use std::sync::{Arc, Mutex};

use crossbeam::channel::{Receiver, Sender};
use timely::communication::{Allocate, WorkerGuards};
use timely::dataflow::Scope;
use timely::progress::Timestamp;
use timely::worker::Worker;
use timely::Config;

use crate::command::{ClientCommand, ControlCommand, ServerCommand};
use crate::internal::{
    SysInternal, SysInternalCoord, SysInternalInput, SysInternalTrace, SysInternalWorker,
};
use crate::timely_util::dd_input::DDInputGroup;
use crate::timely_util::trace_group::TraceGroup;
use crate::timely_util::upsert_input::UpsertInputGroup;

mod command;
pub mod internal;
pub mod timely_util;
pub mod timestamp;

pub use timestamp::SysTime;

pub type SysDiff = i64;

pub enum PeekResult {
    NotReady,
    Done,
}

pub type PeekTask = Box<dyn FnMut() -> PeekResult>;

pub struct WorkerContext<'w, A: Allocate> {
    // trace
    pub trace_group: TraceGroup<SysTime>,
    // input, all input's time should be equal
    pub upsert_input_group: UpsertInputGroup<SysTime, SysDiff>,
    pub input_group: DDInputGroup<SysTime, SysDiff>,
    // peaks
    pub peeks: Vec<PeekTask>,
    pub frontier: SysTime,
    pub worker: &'w mut Worker<A>,
    pub shutdown: bool,
}

pub struct WorkerState<'a> {
    // trace
    pub trace_group: &'a mut TraceGroup<SysTime>,
    // input, all input's time should be equal
    pub upsert_input_group: &'a mut UpsertInputGroup<SysTime, SysDiff>,
    pub input_group: &'a mut DDInputGroup<SysTime, SysDiff>,
    // peaks
    pub peeks: &'a mut Vec<PeekTask>,
    pub frontier: &'a SysTime,
}

impl<'w, A: Allocate> WorkerContext<'w, A> {
    pub fn new(worker: &'w mut Worker<A>) -> Self {
        WorkerContext {
            trace_group: TraceGroup::new(),
            upsert_input_group: UpsertInputGroup::new(),
            input_group: DDInputGroup::new(),
            worker,
            peeks: vec![],
            frontier: SysTime::minimum(),
            shutdown: false,
        }
    }

    fn state(&mut self) -> WorkerState<'_> {
        WorkerState {
            trace_group: &mut self.trace_group,
            upsert_input_group: &mut self.upsert_input_group,
            input_group: &mut self.input_group,
            peeks: &mut self.peeks,
            frontier: &self.frontier,
        }
    }

    fn worker_and_state(&mut self) -> (&mut Worker<A>, WorkerState<'_>) {
        let worker = &mut *self.worker;
        let state = WorkerState {
            trace_group: &mut self.trace_group,
            upsert_input_group: &mut self.upsert_input_group,
            input_group: &mut self.input_group,
            peeks: &mut self.peeks,
            frontier: &self.frontier,
        };
        (worker, state)
    }

    pub fn handle_peeks(&mut self) {
        let mut new_peeks = vec![];
        for mut task in std::mem::take(&mut self.peeks) {
            let res = task();
            match res {
                PeekResult::NotReady => new_peeks.push(task),
                PeekResult::Done => {}
            }
        }
        self.peeks = new_peeks;
    }

    pub fn handle_control_command(&mut self, cmd: ControlCommand) {
        match cmd {
            ControlCommand::AdvanceTimestamp(time) => {
                assert_eq!(self.frontier.step_forward(), time);
                let prev_time = self.frontier;
                self.frontier = time;
                self.upsert_input_group.advance_to(self.frontier);
                self.input_group.advance_and_flush(self.frontier);
                self.trace_group.logical_compaction(prev_time);
            }
            ControlCommand::CollectInternal(tx) => {
                let trace_bundle_info = self.trace_group.collect_info();
                let mut trace_info = Vec::with_capacity(trace_bundle_info.len());
                for bundle in trace_bundle_info {
                    let info = SysInternalTrace {
                        name: bundle.name,
                        logical_compaction: bundle
                            .logical_compaction
                            .into_option()
                            .expect("trace should not be closed"),
                        physical_compaction: bundle
                            .physical_compaction
                            .into_option()
                            .expect("physical compaction should not be empty"),
                    };
                    trace_info.push(info);
                }
                let upsert_input_bundle_info = self.upsert_input_group.collect_info();
                let mut upsert_input_info = Vec::with_capacity(upsert_input_bundle_info.len());
                for bundle in upsert_input_bundle_info {
                    let info = SysInternalInput {
                        name: bundle.name.to_string(),
                        time: bundle.time,
                    };
                    upsert_input_info.push(info);
                }

                let input_bundle_info = self.input_group.collect_info();
                let mut input_info = Vec::with_capacity(input_bundle_info.len());
                for bundle in input_bundle_info {
                    let info = SysInternalInput {
                        name: bundle.name.to_string(),
                        time: bundle.time,
                    };
                    input_info.push(info);
                }

                let worker_info = SysInternalWorker {
                    index: self.worker.index(),
                    frontier: self.frontier,
                    upsert_input_info,
                    input_info,
                    trace_info,
                };
                let _ = tx.send(worker_info);
            }
            ControlCommand::Shutdown => self.shutdown = true,
        }
    }
}

pub struct Coord<A: App> {
    workers: usize,
    frontier: SysTime,
    worker_guards: WorkerGuards<()>,
    worker_txs: Vec<Sender<ServerCommand<A::Query, A::Update>>>,
}

impl<A: App> Coord<A> {
    fn advance_input(&mut self) {
        self.frontier = self.frontier.step_forward();
        let cmd = ControlCommand::AdvanceTimestamp(self.frontier);
        self.broadcast(cmd);
    }

    fn query_time(&self) -> SysTime {
        self.frontier
            .step_back()
            .expect("initial query time not advanced")
    }

    fn send(&self, idx: usize, cmd: ServerCommand<A::Query, A::Update>) {
        self.worker_txs[idx].send(cmd).unwrap();
        self.worker_guards.guards()[idx].thread().unpark();
    }

    fn broadcast(&self, cmd: impl Into<ServerCommand<A::Query, A::Update>> + Clone) {
        for tx in &self.worker_txs {
            let cmd = cmd.clone().into();
            tx.send(cmd).unwrap();
        }
        for handle in self.worker_guards.guards() {
            handle.thread().unpark();
        }
    }
}

pub trait App: Clone + Sized + 'static {
    type Query: Clone + Send + 'static;
    type Update: Send + 'static;

    fn name(&self) -> &str;

    fn dataflow<G: Scope<Timestamp = SysTime>>(scope: &mut G, state: WorkerState<'_>);

    fn handle_query(query: Self::Query, time: SysTime, state: WorkerState<'_>);

    fn handle_update(update: Self::Update, state: WorkerState<'_>);

    fn start(&self, workers: usize) -> Handle<Self> {
        // client channels
        let (client_tx, client_rx) = crossbeam::channel::unbounded();
        let name = self.name();
        std::thread::Builder::new()
            .name(name.into())
            .spawn(move || start_coord::<Self>(workers, client_rx))
            .unwrap();

        Handle {
            inner: Arc::new(HandleInner { tx: client_tx }),
        }
    }
}

fn start_coord<A: App>(workers: usize, client_rx: Receiver<ClientCommand<A::Query, A::Update>>) {
    let mut td_config = Config::process(workers);
    let dd_config = differential_dataflow::Config {
        idle_merge_effort: Some(1000),
    };
    differential_dataflow::configure(&mut td_config.worker, &dd_config);

    // server channels
    let mut worker_txs = Vec::with_capacity(workers);
    let mut worker_rxs = Vec::with_capacity(workers);
    for _ in 0..workers {
        let (tx, rx) = crossbeam::channel::unbounded();
        worker_txs.push(tx);
        worker_rxs.push(rx);
    }

    let worker_guards = run_timely_workers::<A>(td_config, worker_rxs);

    let mut coord = Coord::<A> {
        workers,
        frontier: SysTime::minimum(),
        worker_guards,
        worker_txs,
    };

    coord.advance_input();

    loop {
        let cmd = match client_rx.recv() {
            Ok(d) => d,
            Err(_) => unreachable!(), // client channel 在关闭前会发送 Shutdown 命令
        };

        match cmd {
            ClientCommand::Query(q) => {
                let time = coord.query_time();
                coord.broadcast((q, time));
            }
            ClientCommand::Update(update) => {
                let cmd = ServerCommand::Update(update);
                // TODO: maybe more accurate idx?
                coord.send(0, cmd);
                coord.advance_input();
            }
            ClientCommand::CollectInternal(sender) => {
                let (tx, rx) = crossbeam::channel::unbounded();
                let cmd = ControlCommand::CollectInternal(tx);
                coord.broadcast(cmd);
                let mut worker_data = Vec::with_capacity(coord.workers);
                while let Ok(d) = rx.recv() {
                    worker_data.push(d);
                }
                worker_data.sort_by_key(|d| d.index);
                let coord_data = SysInternalCoord {
                    workers: coord.workers,
                    frontier: coord.frontier,
                };
                let ret = SysInternal {
                    coord: coord_data,
                    workers: worker_data,
                };
                let _ = sender.send(ret);
            }
            ClientCommand::DropApp => {
                coord.broadcast(ControlCommand::Shutdown);
                break;
            }
        }
    }
}

fn run_timely_workers<A: App>(
    config: Config,
    worker_rxs: Vec<Receiver<ServerCommand<A::Query, A::Update>>>,
) -> WorkerGuards<()> {
    let workers = worker_rxs.len();
    assert!(workers > 0);
    let msg_rxs: Mutex<Vec<_>> = Mutex::new(worker_rxs.into_iter().map(Some).collect());

    timely::execute(config, move |worker| {
        let rx = msg_rxs.lock().unwrap()[worker.index() % workers]
            .take()
            .unwrap();

        let mut ctx = WorkerContext::new(worker);
        {
            let (worker, state) = ctx.worker_and_state();
            worker.dataflow::<SysTime, _, _>(|scope| A::dataflow(scope, state));
        }

        while !ctx.shutdown {
            // do some maintenance
            ctx.trace_group.physical_compaction();

            ctx.worker.step_or_park(None);

            // handle commands
            let commands: Vec<_> = rx.try_iter().collect();
            for cmd in commands {
                match cmd {
                    ServerCommand::Query(query, time) => {
                        let state = ctx.state();
                        assert_eq!(state.frontier, time.step_forward());
                        A::handle_query(query, time, state);
                    }
                    ServerCommand::Update(update) => {
                        A::handle_update(update, ctx.state());
                    }
                    ServerCommand::ControlCommand(cmd) => ctx.handle_control_command(cmd),
                }
            }
            ctx.handle_peeks();
        }
    })
    .unwrap()
}

#[derive(Clone)]
pub struct Handle<A: App> {
    inner: Arc<HandleInner<A>>,
}

impl<A: App> Handle<A> {
    pub fn query(&self, query: A::Query) {
        let cmd = ClientCommand::Query(query);
        self.inner.tx.send(cmd).unwrap();
    }

    pub fn update(&self, update: A::Update) {
        let cmd = ClientCommand::Update(update);
        self.inner.tx.send(cmd).unwrap();
    }

    pub fn collect_internal_data(&self) -> SysInternal {
        let (tx, rx) = crossbeam::channel::unbounded();
        let cmd = ClientCommand::CollectInternal(tx);
        self.inner.tx.send(cmd).unwrap();
        rx.recv().unwrap()
    }
}

struct HandleInner<A: App> {
    tx: Sender<ClientCommand<A::Query, A::Update>>,
}

impl<A: App> Drop for HandleInner<A> {
    fn drop(&mut self) {
        let cmd = ClientCommand::DropApp;
        self.tx.send(cmd).unwrap();
    }
}
