use std::cmp::Reverse;

use chrono::NaiveDate;
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
use crate::{gen_tpch_app, AnswerTrace, TpchResults};

gen_tpch_app!(03, Customer, Order, LineItem);

impl TpchResults for Q03 {
    fn results(handle: &Handle<Q03>) -> Vec<Q03Answer> {
        let (tx, rx) = crossbeam::channel::unbounded();
        handle.query(Query { sender: tx });

        let mut res = vec![];
        while let Ok(d) = rx.recv() {
            res.extend(d);
        }
        res.sort_by(|x, y| {
            (Reverse(&x.revenue), &x.o_orderdate).cmp(&(Reverse(&y.revenue), &y.o_orderdate))
        });
        for d in &mut res {
            d.revenue.rescale(2);
        }
        res.drain(10..);
        res
    }
}

impl App for Q03 {
    type Query = Query;
    type Update = Update;

    fn name(&self) -> &str {
        "q03"
    }

    fn dataflow<G: Scope<Timestamp = SysTime>>(scope: &mut G, state: WorkerState<'_>) {
        // 1. SEGMENT = BUILDING;
        // 2. DATE = 1995-03-15.

        let date = NaiveDate::from_ymd_opt(1995, 3, 15).unwrap();
        let one = Decimal::from(1);
        let customer = state
            .input_group
            .alloc_collection::<Customer, _>(scope)
            .filter(|x| x.mktsegment == "BUILDING")
            .map(|x| (x.cust_key, x));
        let orders = state
            .input_group
            .alloc_collection::<Order, _>(scope)
            .filter(move |x| x.order_date < date)
            .map(|x| (x.cust_key, x));

        let lineitem = state
            .input_group
            .alloc_collection::<LineItem, _>(scope)
            .filter(move |x| x.ship_date > date)
            .map(|x| (x.order_key, x));

        let arranged = customer
            .join_map(&orders, |_, _, order| (order.order_key, order.clone()))
            .join_map(&lineitem, move |_, order, line_item| {
                (
                    (line_item.order_key, order.order_date, order.ship_priority),
                    line_item.extended_price * (one - line_item.discount),
                )
            })
            .explode(|(k, v)| Some((k, Sum { revenue: v })))
            .count_total_core::<SysDiff>()
            .map(|(k, v)| to_answer(k, v))
            .arrange_by_self();

        let trace: AnswerTrace<Q03Answer> = arranged.trace;
        state.trace_group.register_trace(trace);
    }

    fn handle_query(query: Self::Query, time: SysTime, state: WorkerState<'_>) {
        query.query(time, state);
    }

    fn handle_update(update: Self::Update, state: WorkerState<'_>) {
        update.push_into(state);
    }
}

fn to_answer(
    (l_orderkey, o_orderdate, o_shippriority): (u64, NaiveDate, i32),
    sum: Sum,
) -> Q03Answer {
    Q03Answer {
        l_orderkey,
        revenue: sum.revenue,
        o_orderdate,
        o_shippriority,
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq, Hash)]
struct Sum {
    revenue: Decimal,
}

impl Multiply<SysDiff> for Sum {
    type Output = Self;
    fn multiply(self, rhs: &SysDiff) -> Self::Output {
        // monotonic input
        assert!(*rhs > 0);
        Sum {
            revenue: self.revenue * Decimal::from(*rhs),
        }
    }
}

impl IsZero for Sum {
    // monotonic input
    fn is_zero(&self) -> bool {
        false
    }
}

impl Semigroup for Sum {
    fn plus_equals(&mut self, rhs: &Self) {
        self.revenue += rhs.revenue;
    }
}
