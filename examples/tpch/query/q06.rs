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
use crate::{gen_tpch_app, AnswerTrace, TpchResults};

gen_tpch_app!(06, LineItem);

impl TpchResults for Q06 {
    fn results(handle: &Handle<Self>) -> Vec<Self::Answer> {
        let (tx, rx) = crossbeam::channel::unbounded();
        handle.query(Query { sender: tx });

        let mut res = vec![];
        while let Ok(d) = rx.recv() {
            res.extend(d);
        }
        for d in &mut res {
            d.revenue.rescale(2);
        }
        res
    }
}

impl App for Q06 {
    type Query = Query;
    type Update = Update;

    fn name(&self) -> &str {
        "q06"
    }

    fn dataflow<G: Scope<Timestamp = SysTime>>(scope: &mut G, state: WorkerState<'_>) {
        // 1. DATE = 1994-01-01;
        // 2. DISCOUNT = 0.06;
        // 3. QUANTITY = 24.

        let date = NaiveDate::from_ymd_opt(1994, 1, 1).unwrap();
        let end_date = date + Months::new(12);
        let shift = Decimal::from_str_exact("0.01").unwrap();
        let discount = Decimal::from_str_exact("0.06").unwrap();
        let discount_a = discount - shift;
        let discount_b = discount + shift;

        let lineitem = state
            .input_group
            .alloc_collection::<LineItem, _>(scope)
            .filter(move |x| {
                x.ship_date >= date
                    && x.ship_date < end_date
                    && x.discount >= discount_a
                    && x.discount <= discount_b
                    && x.quantity < 24
            })
            .map(|x| ((), x.extended_price * x.discount));

        let arranged = lineitem
            .explode(|(k, v)| Some((k, Sum { revenue: v })))
            .count_total_core::<SysDiff>()
            .map(|(k, v)| to_answer(k, v))
            .arrange_by_self();

        let trace: AnswerTrace<Q06Answer> = arranged.trace;
        state.trace_group.register_trace(trace);
    }

    fn handle_query(query: Self::Query, time: SysTime, state: WorkerState<'_>) {
        query.query(time, state);
    }

    fn handle_update(update: Self::Update, state: WorkerState<'_>) {
        update.push_into(state);
    }
}

fn to_answer(_: (), sum: Sum) -> Q06Answer {
    Q06Answer {
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
