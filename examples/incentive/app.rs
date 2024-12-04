use crossbeam::channel::Sender;
use ddquery::timely_util::{collect_key_trace, trace_beyond};
use ddquery::{App, Handle, PeekResult, SysTime, WorkerState};
use differential_dataflow::operators::arrange::{ArrangeByKey, ArrangeBySelf};
use differential_dataflow::trace::{Cursor, TraceReader};
use timely::dataflow::Scope;
use timely::progress::Antichain;
use timely::PartialOrder;

use crate::dataflows::*;
use crate::error::Error;
use crate::models::*;
use crate::typedef::{ErrorTrace, SalesMonthKey, SalesRevenueAccuTrace};

#[derive(Clone)]
pub struct IncentiveApp;

#[derive(Clone)]
pub struct IncentiveHandle {
    handle: Handle<IncentiveApp>,
}

#[derive(Clone, Debug)]
pub enum Query {
    QuerySalesRevenueAccu {
        sales_ldap: String,
        month: Month,
        sender: Sender<Result<i64, Vec<Error>>>,
    },
}

#[derive(Clone, Debug)]
pub enum Update {
    UpsertBelonging(Belonging),
    DeleteBelonging { uid: u64, month: Month },
    UpsertSalesOrg(SalesOrg),
    DeleteSalesOrg { sales_ldap: String, month: Month },
    UpsertRevenue(Revenue),
    DeleteRevenue { uid: u64, month: Month },
}

impl IncentiveHandle {
    pub fn query_sales_revenue_accu(
        &self,
        sales_ldap: impl Into<String>,
        month: Month,
    ) -> Result<i64, Vec<Error>> {
        let (tx, rx) = crossbeam::channel::unbounded();
        let cmd = Query::QuerySalesRevenueAccu {
            sales_ldap: sales_ldap.into(),
            month,
            sender: tx,
        };
        self.handle.query(cmd);
        let mut revenue = 0;
        let mut errors = vec![];
        while let Ok(d) = rx.recv() {
            match d {
                Ok(d) => {
                    if d != 0 {
                        revenue = d;
                    }
                }
                Err(e) => errors.extend(e),
            }
        }
        if errors.is_empty() {
            Ok(revenue)
        } else {
            Err(errors)
        }
    }

    pub fn upsert_belonging(&self, belonging: Belonging) {
        let cmd = Update::UpsertBelonging(belonging);
        self.handle.update(cmd);
    }

    pub fn delete_belonging(&self, uid: u64, month: Month) {
        let cmd = Update::DeleteBelonging { uid, month };
        self.handle.update(cmd);
    }

    pub fn upsert_sales_org(&self, sales_org: SalesOrg) {
        let cmd = Update::UpsertSalesOrg(sales_org);
        self.handle.update(cmd);
    }

    pub fn delete_sales_org(&self, sales_ldap: String, month: Month) {
        let cmd = Update::DeleteSalesOrg { sales_ldap, month };
        self.handle.update(cmd);
    }

    pub fn upsert_revenue(&self, revenue: Revenue) {
        let cmd = Update::UpsertRevenue(revenue);
        self.handle.update(cmd);
    }

    pub fn delete_revenue(&self, uid: u64, month: Month) {
        let cmd = Update::DeleteRevenue { uid, month };
        self.handle.update(cmd);
    }
}

pub fn start(workers: usize) -> IncentiveHandle {
    let handle = IncentiveApp.start(workers);
    IncentiveHandle { handle }
}

impl App for IncentiveApp {
    type Query = Query;
    type Update = Update;

    fn name(&self) -> &str {
        "incentive"
    }

    fn dataflow<G: Scope<Timestamp = SysTime>>(scope: &mut G, worker_state: WorkerState<'_>) {
        let belonging_collection = worker_state
            .upsert_input_group
            .alloc_collection::<Belonging, _>(scope);
        let sales_org_collection = worker_state
            .upsert_input_group
            .alloc_collection::<SalesOrg, _>(scope);
        let revenue_collection = worker_state
            .upsert_input_group
            .alloc_collection::<Revenue, _>(scope);

        let (subordinate_collection, subordinate_error) = subordinate(sales_org_collection);
        let sales_revenue_collection = sales_revenue(belonging_collection, revenue_collection);
        let sales_revenue_accu_arrange =
            sales_revenue_accu(subordinate_collection, sales_revenue_collection)
                .map(|r| ((r.sales_ldap.clone(), r.month.clone()), r))
                .arrange_by_key();
        let error_arrange = subordinate_error.arrange_by_self();

        worker_state
            .trace_group
            .register_trace(sales_revenue_accu_arrange.trace);
        worker_state.trace_group.register_trace(error_arrange.trace);
    }

    fn handle_query(query: Self::Query, time: SysTime, state: WorkerState<'_>) {
        match query {
            Query::QuerySalesRevenueAccu {
                sales_ldap,
                month,
                sender,
            } => {
                let mut trace = state
                    .trace_group
                    .get::<SalesRevenueAccuTrace>()
                    .unwrap()
                    .clone();
                let mut error_trace = state.trace_group.get::<ErrorTrace>().unwrap().clone();

                let key = (sales_ldap, month);
                let task = move || {
                    if trace_beyond(&mut trace, &time) && trace_beyond(&mut error_trace, &time) {
                        // ready
                        let errors = collect_key_trace(&mut error_trace, &time);
                        let res = if errors.is_empty() {
                            let revenue = read_key(&mut trace, &time, &key).unwrap_or_default();
                            Ok(revenue)
                        } else {
                            Err(errors)
                        };
                        let _ = sender.send(res);
                        PeekResult::Done
                    } else {
                        PeekResult::NotReady
                    }
                };
                state.peeks.push(Box::new(task));
            }
        }
    }

    fn handle_update(update: Self::Update, state: WorkerState<'_>) {
        match update {
            Update::UpsertBelonging(belonging) => state.upsert_input_group.upsert(belonging),
            Update::DeleteBelonging { uid, month } => {
                state.upsert_input_group.delete::<Belonging>((uid, month))
            }
            Update::UpsertSalesOrg(sales_org) => state.upsert_input_group.upsert(sales_org),
            Update::DeleteSalesOrg { sales_ldap, month } => state
                .upsert_input_group
                .delete::<SalesOrg>((sales_ldap, month)),
            Update::UpsertRevenue(revenue) => state.upsert_input_group.upsert(revenue),
            Update::DeleteRevenue { uid, month } => {
                state.upsert_input_group.delete::<Revenue>((uid, month))
            }
        }
    }
}

pub fn read_key(
    trace: &mut SalesRevenueAccuTrace,
    time: &SysTime,
    key: &SalesMonthKey,
) -> Option<i64> {
    let mut upper = Antichain::new();
    trace.read_upper(&mut upper);
    assert!(!upper.less_equal(time));
    assert!(trace.get_logical_compaction().less_equal(time));

    let mut ret = None;
    let (mut cursor, storage) = trace.cursor();
    cursor.seek_key(&storage, key);

    if cursor.get_key(&storage) == Some(key) {
        let mut values = vec![];
        while let Some(val) = cursor.get_val(&storage) {
            let mut count = 0;
            cursor.map_times(&storage, |dtime, diff| {
                if dtime.less_equal(time) {
                    count += diff;
                }
            });
            assert!(
                count == 0 || count == 1,
                "invalid count for key: {:?}, count: {}",
                key,
                count
            );
            if count == 1 {
                values.push(val.revenue);
            }
            cursor.step_val(&storage);
        }

        assert!(
            values.len() == 0 || values.len() == 1,
            "invalid count for key: {:?}, count: {}",
            key,
            values.len()
        );

        ret = values.pop();
    }
    ret
}
