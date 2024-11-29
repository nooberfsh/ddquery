use std::cmp::Reverse;

use crossbeam::channel::Sender;
use ddquery::timely_util::{collect_key_trace, trace_beyond};
use ddquery::{App, Handle, PeekResult, SysDiff, SysTime, WorkerState};
use differential_dataflow::difference::{IsZero, Multiply, Semigroup};
use differential_dataflow::operators::arrange::ArrangeBySelf;
use differential_dataflow::operators::*;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use timely::dataflow::Scope;

use crate::models::*;
use crate::AnswerTrace;

#[derive(Clone)]
pub struct Q02;

impl Q02 {
    pub fn results(handle: &Handle<Q02>) -> Vec<Q02Answer> {
        let (tx, rx) = crossbeam::channel::unbounded();
        handle.query(Query { sender: tx });

        let mut res = vec![];
        while let Ok(d) = rx.recv() {
            res.extend(d);
        }
        res.sort_by(|x, y| {
            (Reverse(&x.s_acctbal), &x.n_name, &x.s_name, x.p_partkey).cmp(&(
                Reverse(&y.s_acctbal),
                &y.n_name,
                &y.s_name,
                y.p_partkey,
            ))
        });
        res.drain(100..);
        res
    }
}

#[derive(Clone)]
pub struct Query {
    pub sender: Sender<Vec<Q02Answer>>,
}

pub enum Update {
    Part(Vec<Part>),
    Supplier(Vec<Supplier>),
    PartSupp(Vec<PartSupp>),
    Nation(Vec<Nation>),
    Region(Vec<Region>),
}

impl App for Q02 {
    type Query = Query;
    type Update = Update;

    fn name(&self) -> &str {
        "q02"
    }

    fn dataflow<G: Scope<Timestamp = SysTime>>(scope: &mut G, state: WorkerState<'_>) {
        // 1. SIZE = 15;
        // 2. TYPE = BRASS;
        // 3. REGION = EUROPE

        let part = state
            .input_group
            .alloc_collection::<Part, _>(scope)
            .filter(|x| x.size == 15 && x.typ.ends_with("BRASS"))
            .map(|p| (p.part_key, p));
        let supplier = state
            .input_group
            .alloc_collection::<Supplier, _>(scope)
            .map(|s| (s.nation_key, s));
        let part_supp = state
            .input_group
            .alloc_collection::<PartSupp, _>(scope)
            .map(|ps| (ps.supp_key, ps));
        let nation = state
            .input_group
            .alloc_collection::<Nation, _>(scope)
            .map(|s| (s.region_key, s));
        let region = state
            .input_group
            .alloc_collection::<Region, _>(scope)
            .flat_map(|x| {
                if x.name == "EUROPE" {
                    Some((x.region_key, x))
                } else {
                    None
                }
            });

        let combined = nation
            .join_map(&region, |_, nation, _| (nation.nation_key, nation.clone()))
            .join_map(&supplier, |_, n, s| (s.supp_key, (n.clone(), s.clone())))
            .join_map(&part_supp, |_, (n, s), ps| {
                (ps.part_key, (n.clone(), s.clone(), ps.supplycost))
            })
            .join_map(&part, |_, (n, s, cost), p| {
                (
                    Q02Answer {
                        s_acctbal: s.acctbal,
                        s_name: s.name.clone(),
                        n_name: n.name.clone(),
                        p_partkey: p.part_key,
                        p_mfgr: p.mfgr.clone(),
                        s_address: s.address.clone(),
                        s_phone: s.phone.clone(),
                        s_comment: s.comment.clone(),
                    },
                    *cost,
                )
            });
        // min price
        let min_cost = combined
            .explode(|(x, cost)| Some((x.p_partkey, Min { cost })))
            .count_total_core::<SysDiff>()
            .map(|(k, m)| (k, m.cost));

        let arranged = combined
            .map(|(answer, cost)| ((answer.p_partkey, cost), answer))
            .semijoin(&min_cost)
            .map(|(_, v)| v)
            .arrange_by_self();

        let trace: AnswerTrace<Q02Answer> = arranged.trace;
        state.trace_group.register_trace(trace);
    }

    fn handle_query(query: Self::Query, time: SysTime, state: WorkerState<'_>) {
        let mut trace = state
            .trace_group
            .get::<AnswerTrace<Q02Answer>>()
            .unwrap()
            .clone();

        let task = move || {
            if trace_beyond(&mut trace, &time) {
                let data = collect_key_trace(&mut trace, &time);
                let _ = query.sender.send(data);
                PeekResult::Done
            } else {
                PeekResult::NotReady
            }
        };
        state.peeks.push(Box::new(task));
    }

    fn handle_update(update: Self::Update, state: WorkerState<'_>) {
        match update {
            Update::Part(v) => {
                state.input_group.insert_batch::<Part>(v);
            }
            Update::Supplier(v) => {
                state.input_group.insert_batch::<Supplier>(v);
            }
            Update::PartSupp(v) => {
                state.input_group.insert_batch::<PartSupp>(v);
            }
            Update::Nation(v) => {
                state.input_group.insert_batch::<Nation>(v);
            }
            Update::Region(v) => {
                state.input_group.insert_batch::<Region>(v);
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq, Hash)]
struct Min {
    cost: Decimal,
}

impl Multiply<SysDiff> for Min {
    type Output = Self;
    fn multiply(self, rhs: &SysDiff) -> Self::Output {
        // monotonic input
        assert!(*rhs > 0);
        self
    }
}

impl IsZero for Min {
    // monotonic input
    fn is_zero(&self) -> bool {
        false
    }
}

impl Semigroup for Min {
    fn plus_equals(&mut self, rhs: &Self) {
        if rhs.cost < self.cost {
            self.cost = rhs.cost
        }
    }
}
