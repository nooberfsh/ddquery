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

gen_tpch_app!(08, Part, Supplier, LineItem, Order, Customer, Nation, Region);

impl TpchResults for Q08 {
    fn results(handle: &Handle<Self>) -> Vec<Self::Answer> {
        let (tx, rx) = crossbeam::channel::unbounded();
        handle.query(Query { sender: tx });

        let mut res = vec![];
        while let Ok(d) = rx.recv() {
            res.extend(d);
        }
        res.sort_by_key(|x| x.o_year);
        for d in &mut res {
            d.mkt_share.rescale(2);
        }
        res
    }
}

impl App for Q08 {
    type Query = Query;
    type Update = Update;

    fn name(&self) -> &str {
        "q08"
    }

    fn dataflow<G: Scope<Timestamp = SysTime>>(scope: &mut G, state: WorkerState<'_>) {
        // 1. NATION = BRAZIL;
        // 2. REGION = AMERICA;
        // 3. TYPE = ECONOMY ANODIZED STEEL.

        let date = NaiveDate::from_ymd_opt(1995, 1, 1).unwrap();
        let end_date = NaiveDate::from_ymd_opt(1996, 12, 31).unwrap();
        let one = Decimal::from(1);

        let region = state
            .input_group
            .alloc_collection::<Region, _>(scope)
            .flat_map(|x| {
                if x.name == "AMERICA" {
                    Some(x.region_key)
                } else {
                    None
                }
            });
        let nation = state.input_group.alloc_collection::<Nation, _>(scope);
        let customer = state
            .input_group
            .alloc_collection::<Customer, _>(scope)
            .map(|x| (x.nation_key, x));
        let orders = state
            .input_group
            .alloc_collection::<Order, _>(scope)
            .filter(move |x| x.order_date >= date && x.order_date <= end_date)
            .map(|x| (x.cust_key, x));

        let x = nation
            .map(|x| (x.region_key, x))
            .semijoin(&region)
            .map(|(_, x)| (x.nation_key, x))
            .join_map(&customer, |_, _, customer| customer.cust_key);

        let y = orders.semijoin(&x).map(|(_, o)| (o.order_key, o));

        let part = state
            .input_group
            .alloc_collection::<Part, _>(scope)
            .filter(|x| x.typ == "ECONOMY ANODIZED STEEL")
            .map(|x| (x.part_key, x));

        let lineitem = state
            .input_group
            .alloc_collection::<LineItem, _>(scope)
            .map(|x| (x.part_key, x));

        let supplier = state
            .input_group
            .alloc_collection::<Supplier, _>(scope)
            .map(|x| (x.nation_key, x))
            .map(|(_, s)| (s.supp_key, s));

        let z = part
            .join_map(&lineitem, |_, _, line_item| {
                (line_item.supp_key, line_item.clone())
            })
            .join_map(&supplier, move |_, line_item, supplier| {
                (
                    line_item.order_key,
                    (
                        line_item.extended_price * (one - line_item.discount),
                        supplier.nation_key,
                    ),
                )
            })
            .join_map(&y, |_, (volume, nation_key), order| {
                (order.order_date.year(), *nation_key, *volume)
            });

        let total = z
            .explode(|(year, _, volume)| Some((year, Sum { revenue: volume })))
            .count_total_core::<SysDiff>();

        let specific_nation = nation.filter(|x| x.name == "BRAZIL").map(|x| x.nation_key);
        let specific = z
            .map(|(y, n, v)| (n, (y, v)))
            .semijoin(&specific_nation)
            .explode(|(_, (year, volume))| Some((year, Sum { revenue: volume })))
            .count_total_core::<SysDiff>();

        // TODO: we should use left join here
        let arranged = total
            .join_map(&specific, |year, year_volume, specific_volume| {
                to_answer(year, year_volume, specific_volume)
            })
            .arrange_by_self();

        let trace: AnswerTrace<Q08Answer> = arranged.trace;
        state.trace_group.register_trace(trace);
    }

    fn handle_query(query: Self::Query, time: SysTime, state: WorkerState<'_>) {
        query.query(time, state);
    }

    fn handle_update(update: Self::Update, state: WorkerState<'_>) {
        update.push_into(state);
    }
}

fn to_answer(year: &i32, year_volume: &Sum, specific_volume: &Sum) -> Q08Answer {
    Q08Answer {
        o_year: *year,
        mkt_share: specific_volume.revenue / year_volume.revenue,
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
