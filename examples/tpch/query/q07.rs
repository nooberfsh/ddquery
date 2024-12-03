use chrono::{Datelike, NaiveDate};
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

gen_tpch_app!(07, Supplier, LineItem, Order, Customer, Nation);

impl TpchResults for Q07 {
    fn results(handle: &Handle<Self>) -> Vec<Self::Answer> {
        let (tx, rx) = crossbeam::channel::unbounded();
        handle.query(Query { sender: tx });

        let mut res = vec![];
        while let Ok(d) = rx.recv() {
            res.extend(d);
        }
        res.sort_by(|x, y| {
            (&x.supp_nation, &x.cust_nation, x.l_year).cmp(&(
                &y.supp_nation,
                &y.cust_nation,
                y.l_year,
            ))
        });
        for d in &mut res {
            d.revenue.rescale(2);
        }
        res
    }
}

impl App for Q07 {
    type Query = Query;
    type Update = Update;

    fn name(&self) -> &str {
        "q07"
    }

    fn dataflow<G: Scope<Timestamp = SysTime>>(scope: &mut G, state: WorkerState<'_>) {
        // 1. NATION1 = FRANCE;
        // 2. NATION2 = GERMANY

        let date = NaiveDate::from_ymd_opt(1995, 1, 1).unwrap();
        let end_date = NaiveDate::from_ymd_opt(1996, 12, 31).unwrap();
        let one = Decimal::from(1);

        let nation = state
            .input_group
            .alloc_collection::<Nation, _>(scope)
            .filter(|x| x.name == "FRANCE" || x.name == "GERMANY");

        let nation_ids = nation.map(|x| x.nation_key);

        let supplier = state
            .input_group
            .alloc_collection::<Supplier, _>(scope)
            .map(|x| (x.nation_key, x))
            .semijoin(&nation_ids)
            .map(|(_, s)| (s.supp_key, s));
        let customer = state
            .input_group
            .alloc_collection::<Customer, _>(scope)
            .map(|x| (x.nation_key, x))
            .semijoin(&nation_ids)
            .map(|(_, x)| (x.cust_key, x));
        let orders = state
            .input_group
            .alloc_collection::<Order, _>(scope)
            .map(|x| (x.cust_key, x));

        let lineitem = state
            .input_group
            .alloc_collection::<LineItem, _>(scope)
            .filter(move |x| x.ship_date >= date && x.ship_date <= end_date)
            .map(|x| (x.order_key, x));

        let x = customer
            .join_map(&orders, |_, customer, order| {
                (order.order_key, customer.clone())
            })
            .join_map(&lineitem, move |_, customer, line_item| {
                (
                    line_item.supp_key,
                    (
                        customer.clone(),
                        (
                            line_item.ship_date.year(),
                            line_item.extended_price * (one - line_item.discount),
                        ),
                    ),
                )
            })
            .join_map(&supplier, move |_, (customer, (year, v)), supplier| {
                ((supplier.nation_key, customer.nation_key), (*year, *v))
            });

        let key_nation = nation.map(|s| ((), s));

        let nation_pairs = key_nation
            .join_map(&key_nation, |_, from_nation, to_nation| {
                (from_nation.clone(), to_nation.clone())
            })
            .flat_map(|(x, y)| {
                if x.nation_key == y.nation_key {
                    None
                } else {
                    Some(((x.nation_key, y.nation_key), (x, y)))
                }
            });

        let arranged = x
            .join_map(&nation_pairs, |_, (year, revenue), (from, to)| {
                (from.name.clone(), to.name.clone(), *year, *revenue)
            })
            .explode(|(from, to, year, revenue)| Some(((from, to, year), Sum { revenue })))
            .count_total_core::<SysDiff>()
            .map(|(k, v)| to_answer(k, v))
            .arrange_by_self();

        let trace: AnswerTrace<Q07Answer> = arranged.trace;
        state.trace_group.register_trace(trace);
    }

    fn handle_query(query: Self::Query, time: SysTime, state: WorkerState<'_>) {
        query.query(time, state);
    }

    fn handle_update(update: Self::Update, state: WorkerState<'_>) {
        update.push_into(state);
    }
}

fn to_answer((from, to, year): (String, String, i32), sum: Sum) -> Q07Answer {
    Q07Answer {
        supp_nation: from,
        cust_nation: to,
        l_year: year,
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
