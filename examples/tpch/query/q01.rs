use chrono::{Days, NaiveDate};
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
use crate::{gen_query, gen_update, AnswerTrace};

#[derive(Clone)]
pub struct Q01;

impl Q01 {
    pub fn results(handle: &Handle<Q01>) -> Vec<Q01Answer> {
        let (tx, rx) = crossbeam::channel::unbounded();
        handle.query(Query { sender: tx });

        let mut res = vec![];
        while let Ok(d) = rx.recv() {
            for mut e in d {
                e.sum_base_price.rescale(2);
                e.sum_disc_price.rescale(2);
                e.sum_charge.rescale(2);
                e.avg_qty.rescale(2);
                e.avg_disc.rescale(2);
                e.avg_price.rescale(2);
                res.push(e);
            }
        }
        res.sort_by(|l, r| (l.return_flag, l.line_status).cmp(&(r.return_flag, r.line_status)));
        res
    }
}

gen_query!(Q01Answer);
gen_update!(LineItem);

impl App for Q01 {
    type Query = Query;
    type Update = Update;

    fn name(&self) -> &str {
        "q01"
    }

    fn dataflow<G: Scope<Timestamp = SysTime>>(scope: &mut G, state: WorkerState<'_>) {
        let date = NaiveDate::from_ymd_opt(1998, 12, 01).unwrap();
        let date = date - Days::new(90);
        let one = Decimal::from(1);
        let lineitem = state.input_group.alloc_collection::<LineItem, _>(scope);
        let arranged = lineitem
            .explode(move |li| {
                let disc_price = li.extended_price * (one - li.discount);
                if li.ship_date <= date {
                    Some((
                        (li.return_flag, li.line_status),
                        Sum {
                            quantity: li.quantity,
                            extended_price: li.extended_price,
                            disc_price,
                            charge: disc_price * (one + li.tax),
                            discount: li.discount,
                            count: 1,
                        },
                    ))
                } else {
                    None
                }
            })
            .count_total_core::<SysDiff>()
            .map(|(k, v)| to_answer(k, v))
            .arrange_by_self();

        let trace: AnswerTrace<Q01Answer> = arranged.trace;
        state.trace_group.register_trace(trace);
    }

    fn handle_query(query: Self::Query, time: SysTime, state: WorkerState<'_>) {
        query.query(time, state);
    }

    fn handle_update(update: Self::Update, state: WorkerState<'_>) {
        update.push_into(state);
    }
}

fn to_answer((return_flag, line_status): (char, char), sum: Sum) -> Q01Answer {
    let count = Decimal::from(sum.count);
    Q01Answer {
        return_flag,
        line_status,
        sum_qty: Decimal::from(sum.quantity),
        sum_base_price: sum.extended_price,
        sum_disc_price: sum.disc_price,
        sum_charge: sum.charge,
        avg_qty: Decimal::from(sum.quantity) / count,
        avg_price: sum.extended_price / count,
        avg_disc: sum.discount / count,
        count_order: sum.count,
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq, Hash)]
struct Sum {
    quantity: i64,
    extended_price: Decimal,
    disc_price: Decimal,
    charge: Decimal,
    discount: Decimal,
    count: u64,
}

impl Multiply<SysDiff> for Sum {
    type Output = Self;
    fn multiply(self, rhs: &SysDiff) -> Self::Output {
        // monotonic input
        assert!(*rhs > 0);
        let urhs = *rhs as u64;
        Sum {
            quantity: self.quantity * rhs,
            extended_price: self.extended_price * Decimal::from(*rhs),
            disc_price: self.disc_price * Decimal::from(*rhs),
            charge: self.charge * Decimal::from(*rhs),
            discount: self.discount * Decimal::from(*rhs),
            count: self.count * urhs,
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
        self.quantity += rhs.quantity;
        self.extended_price += rhs.extended_price;
        self.disc_price += rhs.disc_price;
        self.charge += rhs.charge;
        self.discount += rhs.discount;
        self.count += rhs.count;
    }
}
