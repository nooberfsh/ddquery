use std::cmp::Reverse;

use chrono::{Months, NaiveDate};
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
use crate::{gen_query, gen_update};

#[derive(Clone)]
pub struct Q05;

impl Q05 {
    pub fn results(handle: &Handle<Q05>) -> Vec<Q05Answer> {
        let (tx, rx) = crossbeam::channel::unbounded();
        handle.query(Query { sender: tx });

        let mut res = vec![];
        while let Ok(d) = rx.recv() {
            res.extend(d);
        }
        res.sort_by_key(|x| Reverse(x.revenue));
        for d in &mut res {
            d.revenue.rescale(2);
        }
        res
    }
}

gen_query!(Q05Answer);
gen_update!(Customer, Order, LineItem, Supplier, Nation, Region);

impl App for Q05 {
    type Query = Query;
    type Update = Update;

    fn name(&self) -> &str {
        "q05"
    }

    fn dataflow<G: Scope<Timestamp = SysTime>>(scope: &mut G, state: WorkerState<'_>) {
        // 1. REGION = ASIA;
        // 2. DATE = 1994-01-01.

        let date = NaiveDate::from_ymd_opt(1994, 1, 1).unwrap();
        let end_date = date + Months::new(12);
        let one = Decimal::from(1);

        let supplier = state
            .input_group
            .alloc_collection::<Supplier, _>(scope)
            .map(|s| (s.nation_key, s));
        let nation = state
            .input_group
            .alloc_collection::<Nation, _>(scope)
            .map(|s| (s.region_key, s));
        let region = state
            .input_group
            .alloc_collection::<Region, _>(scope)
            .flat_map(|x| {
                if x.name == "ASIA" {
                    Some((x.region_key, x))
                } else {
                    None
                }
            });

        let customer = state
            .input_group
            .alloc_collection::<Customer, _>(scope)
            .map(|x| (x.cust_key, x));
        let orders = state
            .input_group
            .alloc_collection::<Order, _>(scope)
            .filter(move |x| x.order_date >= date && x.order_date < end_date)
            .map(|x| (x.cust_key, x));

        let lineitem = state
            .input_group
            .alloc_collection::<LineItem, _>(scope)
            .filter(move |x| x.ship_date > date)
            .map(|x| (x.order_key, x));

        let x = customer
            .join_map(&orders, |_, customer, order| {
                (order.order_key, (customer.clone(), order.clone()))
            })
            .join_map(&lineitem, move |_, (customer, _), line_item| {
                (
                    (line_item.supp_key, customer.nation_key),
                    line_item.extended_price * (one - line_item.discount),
                )
            });

        let y = nation
            .join_map(&region, |_, nation, _| (nation.nation_key, nation.clone()))
            .join_map(&supplier, |_, n, s| {
                ((s.supp_key, s.nation_key), n.name.clone())
            });

        let arranged = x
            .join_map(&y, |_, v, n| (n.clone(), *v))
            .explode(|(k, v)| Some((k, Sum { revenue: v })))
            .count_total_core::<SysDiff>()
            .map(|(k, v)| to_answer(k, v))
            .arrange_by_self();

        let trace: AnswerTrace<Q05Answer> = arranged.trace;
        state.trace_group.register_trace(trace);
    }

    fn handle_query(query: Self::Query, time: SysTime, state: WorkerState<'_>) {
        query.query(time, state);
    }

    fn handle_update(update: Self::Update, state: WorkerState<'_>) {
        update.push_into(state);
    }
}

fn to_answer(n_name: String, sum: Sum) -> Q05Answer {
    Q05Answer {
        n_name,
        revenue: sum.revenue,
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
