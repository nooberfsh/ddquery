use chrono::{Months, NaiveDate};
use crossbeam::channel::Sender;
use ddquery::timely_util::{collect_key_trace, trace_beyond};
use ddquery::{App, Handle, PeekResult, SysDiff, SysTime, WorkerState};
use differential_dataflow::operators::arrange::ArrangeBySelf;
use differential_dataflow::operators::*;
use timely::dataflow::Scope;

use crate::models::*;
use crate::{gen_tpch_app, AnswerTrace, TpchResults};

gen_tpch_app!(04, Order, LineItem);

impl TpchResults for Q04 {
    fn results(handle: &Handle<Q04>) -> Vec<Q04Answer> {
        let (tx, rx) = crossbeam::channel::unbounded();
        handle.query(Query { sender: tx });

        let mut res = vec![];
        while let Ok(d) = rx.recv() {
            res.extend(d);
        }
        res.sort_by(|x, y| x.o_orderpriority.cmp(&y.o_orderpriority));
        res
    }
}

impl App for Q04 {
    type Query = Query;
    type Update = Update;

    fn name(&self) -> &str {
        "q04"
    }

    fn dataflow<G: Scope<Timestamp = SysTime>>(scope: &mut G, state: WorkerState<'_>) {
        // 1. DATE = 1993-07-01.

        let date = NaiveDate::from_ymd_opt(1993, 7, 1).unwrap();
        let end_date = date + Months::new(3);
        let orders = state
            .input_group
            .alloc_collection::<Order, _>(scope)
            .filter(move |x| x.order_date >= date && x.order_date < end_date)
            .map(|x| (x.order_key, x));

        let lineitem = state
            .input_group
            .alloc_collection::<LineItem, _>(scope)
            .filter(move |x| x.commit_date < x.receipt_date)
            .map(|x| x.order_key)
            .distinct_total_core::<SysDiff>();

        let arranged = orders
            .semijoin(&lineitem)
            .map(|(_, o)| o.order_priority)
            .count_total_core::<SysDiff>()
            .map(|(k, v)| to_answer(k, v))
            .arrange_by_self();

        let trace: AnswerTrace<Q04Answer> = arranged.trace;
        state.trace_group.register_trace(trace);
    }

    fn handle_query(query: Self::Query, time: SysTime, state: WorkerState<'_>) {
        query.query(time, state);
    }

    fn handle_update(update: Self::Update, state: WorkerState<'_>) {
        update.push_into(state);
    }
}

fn to_answer(o_orderpriority: String, order_count: i64) -> Q04Answer {
    Q04Answer {
        o_orderpriority,
        order_count,
    }
}
