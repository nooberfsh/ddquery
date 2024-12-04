use crate::error::Error;
use crate::models::{SalesOrg, SalesSubordinate};
use ddquery::SysDiff;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::operators::*;
use differential_dataflow::Collection;
use timely::dataflow::Scope;
use timely::order::TotalOrder;

macro_rules! if_some {
    ($flag:expr, $some_v:expr) => {
        if $flag {
            Some($some_v)
        } else {
            None
        }
    };
}

// - ensure uniqueness of (sales_ldap, month)
// - detect cycle
// - detect invalid leader
pub fn subordinate<G>(
    sales_org: Collection<G, SalesOrg, SysDiff>,
) -> (
    Collection<G, SalesSubordinate, SysDiff>,
    Collection<G, Error, SysDiff>,
)
where
    G: Scope,
    G::Timestamp: Lattice + Ord + TotalOrder,
{
    // check uniqueness
    let non_unique = sales_org
        .map(|s| (s.sales_ldap, s.month))
        // **Arrange**
        .count_total_core()
        .flat_map(|((ldap, month), r)| {
            if_some!(
                r != 1,
                Error::NonUnique {
                    sales_ldap: ldap,
                    month,
                    count: r,
                }
            )
        });

    // check invalid leader
    let leader = sales_org
        .flat_map(|s| s.leader.map(|l| ((l, s.month), ())))
        // **Arrange**
        .distinct_total_core();
    let all = sales_org
        .map(|s| (s.sales_ldap, s.month))
        // **Arrange**
        .distinct_total_core();
    let invalid_leader = leader.antijoin(&all).map(|(l, _)| Error::InvalidLeader {
        sales_ldap: l.0,
        month: l.1,
    });

    let base = sales_org.flat_map(|s| s.leader.map(|l| ((s.sales_ldap, s.month), l)));
    let base_arrange = base.arrange_by_key();

    let res = base.iterate(|accum| {
        let org = base_arrange.enter(&accum.scope());
        let base = base.enter(&accum.scope());
        accum
            .map(|(k, v)| ((v, k.1), k.0))
            .arrange_by_key()
            .join_core(&org, |k, sales_ldap, next_leader| {
                Some(((sales_ldap.clone(), k.1), next_leader.clone()))
            })
            .concat(&base)
            // 避免在存在环的情况无法收敛,
            .distinct_core()
    });

    // detect cycle
    let cycle = res.flat_map(|((ldap, month), leader)| {
        if_some!(
            ldap == leader,
            Error::Cycle {
                sales_ldap: ldap,
                month
            }
        )
    });

    let selfs = sales_org.map(|s| ((s.sales_ldap.clone(), s.month), s.sales_ldap));
    let ok = res
        .filter(|((ldap, _), leader)| ldap != leader)
        .concat(&selfs)
        .map(|((sales_ldap, month), leader)| SalesSubordinate {
            sales_ldap: leader,
            subordinate_ldap: sales_ldap,
            month,
        });
    let error = non_unique.concat(&invalid_leader).concat(&cycle);
    (ok, error)
}

#[cfg(test)]
mod tests {
    use differential_dataflow::input::Input;
    use differential_dataflow::operators::arrange::ArrangeBySelf;
    use timely::dataflow::operators::probe::Handle;
    use timely::dataflow::operators::Probe;
    use timely::progress::Antichain;

    use crate::util::{compact_unique_vec_trace, is_empty_trace, trace_to_vec_key_trace};

    use super::*;

    macro_rules! sync {
        ($input:expr, $probe:expr, $worker:expr) => {{
            let time = $input.time().clone() + 1;
            $input.advance_to(time);
            $input.flush();
            $worker.step_while(|| $probe.less_than(&time));
        }};
    }

    fn newx(sales_ldap: impl Into<String>, leader: Option<&str>, month: u64) -> SalesOrg {
        SalesOrg {
            sales_ldap: sales_ldap.into(),
            leader: leader.map(|x| x.into()),
            month,
        }
    }

    fn newy(
        sales_ldap: impl Into<String>,
        subordinate_ldap: impl Into<String>,
        month: u64,
    ) -> SalesSubordinate {
        SalesSubordinate {
            sales_ldap: sales_ldap.into(),
            subordinate_ldap: subordinate_ldap.into(),
            month,
        }
    }

    #[test]
    fn test_subordinate_ok() {
        timely::execute_directly(move |worker| {
            let mut probe = Handle::new();
            let (mut input, mut subordinate, error) = worker.dataflow::<u64, _, _>(|scope| {
                let (input, sales_org) = scope.new_collection();
                let (ok, error) = subordinate(sales_org);
                let ok_arrange = ok.arrange_by_self();
                let error_arrange = error.arrange_by_self();

                ok_arrange.stream.probe_with(&mut probe);
                error_arrange.stream.probe_with(&mut probe);

                (input, ok_arrange.trace, error_arrange.trace)
            });

            {
                input.update(newx("a", Some("b"), 1), 1);
                input.update(newx("b", None, 1), 1);
                sync!(input, probe, worker);

                let data = trace_to_vec_key_trace(&mut subordinate);
                let compacted = compact_unique_vec_trace(data, Antichain::new());

                assert_eq!(compacted.len(), 3);
                assert_eq!(compacted[0], newy("a", "a", 1));
                assert_eq!(compacted[1], newy("b", "a", 1));
                assert_eq!(compacted[2], newy("b", "b", 1));
                assert!(is_empty_trace(&error))
            }

            {
                input.update(newx("c", Some("b"), 1), 1);
                sync!(input, probe, worker);

                let data = trace_to_vec_key_trace(&mut subordinate);
                let compacted = compact_unique_vec_trace(data, Antichain::new());

                assert_eq!(compacted.len(), 5);
                assert_eq!(compacted[0], newy("a", "a", 1));
                assert_eq!(compacted[1], newy("b", "a", 1));
                assert_eq!(compacted[2], newy("b", "b", 1));
                assert_eq!(compacted[3], newy("b", "c", 1));
                assert_eq!(compacted[4], newy("c", "c", 1));
                assert!(is_empty_trace(&error))
            }

            {
                input.update(newx("a", Some("b"), 1), -1);
                sync!(input, probe, worker);

                let data = trace_to_vec_key_trace(&mut subordinate);
                let compacted = compact_unique_vec_trace(data, Antichain::new());

                assert_eq!(compacted.len(), 3);
                assert_eq!(compacted[0], newy("b", "b", 1));
                assert_eq!(compacted[1], newy("b", "c", 1));
                assert_eq!(compacted[2], newy("c", "c", 1));
                assert!(is_empty_trace(&error))
            }

            {
                input.update(newx("b", None, 2), 1);
                sync!(input, probe, worker);

                let data = trace_to_vec_key_trace(&mut subordinate);
                let compacted = compact_unique_vec_trace(data, Antichain::new());

                assert_eq!(compacted.len(), 4);
                assert_eq!(compacted[0], newy("b", "b", 1));
                assert_eq!(compacted[1], newy("b", "b", 2));
                assert_eq!(compacted[2], newy("b", "c", 1));
                assert_eq!(compacted[3], newy("c", "c", 1));
                assert!(is_empty_trace(&error))
            }
        });
    }

    #[test]
    fn test_subordinate_multi_level_ok() {
        timely::execute_directly(move |worker| {
            let mut probe = Handle::new();
            let (mut input, mut subordinate, error) = worker.dataflow::<u64, _, _>(|scope| {
                let (input, sales_org) = scope.new_collection();
                let (ok, error) = subordinate(sales_org);
                let ok_arrange = ok.arrange_by_self();
                let error_arrange = error.arrange_by_self();

                ok_arrange.stream.probe_with(&mut probe);
                error_arrange.stream.probe_with(&mut probe);

                (input, ok_arrange.trace, error_arrange.trace)
            });

            {
                input.update(newx("a", Some("b"), 1), 1);
                input.update(newx("b", Some("c"), 1), 1);
                input.update(newx("c", Some("d"), 1), 1);
                input.update(newx("d", None, 1), 1);
                sync!(input, probe, worker);

                let data = trace_to_vec_key_trace(&mut subordinate);
                let compacted = compact_unique_vec_trace(data, Antichain::new());

                assert_eq!(compacted.len(), 10);
                assert_eq!(compacted[0], newy("a", "a", 1));
                assert_eq!(compacted[1], newy("b", "a", 1));
                assert_eq!(compacted[2], newy("b", "b", 1));
                assert_eq!(compacted[3], newy("c", "a", 1));
                assert_eq!(compacted[4], newy("c", "b", 1));
                assert_eq!(compacted[5], newy("c", "c", 1));
                assert_eq!(compacted[6], newy("d", "a", 1));
                assert_eq!(compacted[7], newy("d", "b", 1));
                assert_eq!(compacted[8], newy("d", "c", 1));
                assert_eq!(compacted[9], newy("d", "d", 1));
                assert!(is_empty_trace(&error))
            }
        });
    }

    #[test]
    fn test_subordinate_error() {
        timely::execute_directly(move |worker| {
            let mut probe = Handle::new();
            let (mut input, _, mut error) = worker.dataflow::<u64, _, _>(|scope| {
                let (input, sales_org) = scope.new_collection();
                let (ok, error) = subordinate(sales_org);
                let ok_arrange = ok.arrange_by_self();
                let error_arrange = error.arrange_by_self();

                ok_arrange.stream.probe_with(&mut probe);
                error_arrange.stream.probe_with(&mut probe);

                (input, ok_arrange.trace, error_arrange.trace)
            });

            {
                input.update(newx("a", Some("b"), 1), 1);
                sync!(input, probe, worker);

                let data = trace_to_vec_key_trace(&mut error);
                let compacted = compact_unique_vec_trace(data, Antichain::new());
                assert_eq!(compacted.len(), 1);
                assert_eq!(
                    compacted[0],
                    Error::InvalidLeader {
                        sales_ldap: "b".into(),
                        month: 1,
                    }
                )
            }

            {
                input.update(newx("b", None, 1), 1);
                sync!(input, probe, worker);

                let data = trace_to_vec_key_trace(&mut error);
                let compacted = compact_unique_vec_trace(data, Antichain::new());
                assert!(compacted.is_empty());
            }

            {
                input.update(newx("b", Some("c"), 1), 1);
                input.update(newx("c", None, 1), 1);
                sync!(input, probe, worker);

                let data = trace_to_vec_key_trace(&mut error);
                let compacted = compact_unique_vec_trace(data, Antichain::new());
                assert_eq!(compacted.len(), 1);
                assert_eq!(
                    compacted[0],
                    Error::NonUnique {
                        sales_ldap: "b".into(),
                        month: 1,
                        count: 2,
                    }
                )
            }

            {
                input.update(newx("b", Some("c"), 1), -1);
                sync!(input, probe, worker);

                let data = trace_to_vec_key_trace(&mut error);
                let compacted = compact_unique_vec_trace(data, Antichain::new());
                assert!(compacted.is_empty());
            }
        });
    }

    #[test]
    fn test_subordinate_error_cycle() {
        timely::execute_directly(move |worker| {
            let mut probe = Handle::new();
            let (mut input, mut subordinate, mut error) = worker.dataflow::<u64, _, _>(|scope| {
                let (input, sales_org) = scope.new_collection();
                let (ok, error) = subordinate(sales_org);
                let ok_arrange = ok.arrange_by_self();
                let error_arrange = error.arrange_by_self();

                ok_arrange.stream.probe_with(&mut probe);
                error_arrange.stream.probe_with(&mut probe);

                (input, ok_arrange.trace, error_arrange.trace)
            });

            {
                // 即便有 cycle 存在, 我们需要确保 ok_trace 也是符合预期的
                input.update(newx("a", Some("b"), 1), 1);
                input.update(newx("b", Some("a"), 1), 1);
                sync!(input, probe, worker);

                let data = trace_to_vec_key_trace(&mut subordinate);
                let compacted = compact_unique_vec_trace(data, Antichain::new());
                assert_eq!(compacted.len(), 4);
                assert_eq!(compacted[0], newy("a", "a", 1));
                assert_eq!(compacted[1], newy("a", "b", 1));
                assert_eq!(compacted[2], newy("b", "a", 1));
                assert_eq!(compacted[3], newy("b", "b", 1));

                let data = trace_to_vec_key_trace(&mut error);
                let compacted = compact_unique_vec_trace(data, Antichain::new());
                assert_eq!(compacted.len(), 2);
                assert_eq!(
                    compacted[0],
                    Error::Cycle {
                        sales_ldap: "a".into(),
                        month: 1,
                    }
                );
                assert_eq!(
                    compacted[1],
                    Error::Cycle {
                        sales_ldap: "b".into(),
                        month: 1,
                    }
                );
            }
        });
    }

    #[test]
    fn test_subordinate_error_cycle2() {
        timely::execute_directly(move |worker| {
            let mut probe = Handle::new();
            let (mut input, _, mut error) = worker.dataflow::<u64, _, _>(|scope| {
                let (input, sales_org) = scope.new_collection();
                let (ok, error) = subordinate(sales_org);
                let ok_arrange = ok.arrange_by_self();
                let error_arrange = error.arrange_by_self();

                ok_arrange.stream.probe_with(&mut probe);
                error_arrange.stream.probe_with(&mut probe);

                (input, ok_arrange.trace, error_arrange.trace)
            });

            {
                input.update(newx("a", Some("b"), 1), 1);
                input.update(newx("b", Some("c"), 1), 1);
                input.update(newx("c", Some("a"), 1), 1);
                input.update(newx("a", Some("d"), 1), 1);
                input.update(newx("d", None, 1), 1);
                sync!(input, probe, worker);

                let data = trace_to_vec_key_trace(&mut error);
                let compacted = compact_unique_vec_trace(data, Antichain::new());
                println!("{:#?}", compacted);
                assert_eq!(compacted.len(), 4);
                assert_eq!(
                    compacted[0],
                    Error::NonUnique {
                        sales_ldap: "a".into(),
                        month: 1,
                        count: 2,
                    }
                );
                assert_eq!(
                    compacted[1],
                    Error::Cycle {
                        sales_ldap: "a".into(),
                        month: 1,
                    }
                );
                assert_eq!(
                    compacted[2],
                    Error::Cycle {
                        sales_ldap: "b".into(),
                        month: 1,
                    }
                );
                assert_eq!(
                    compacted[3],
                    Error::Cycle {
                        sales_ldap: "c".into(),
                        month: 1,
                    }
                );
            }
        });
    }

    #[test]
    fn test_subordinate_error_cycle3() {
        timely::execute_directly(move |worker| {
            let mut probe = Handle::new();
            let (mut input, _, mut error) = worker.dataflow::<u64, _, _>(|scope| {
                let (input, sales_org) = scope.new_collection();
                let (ok, error) = subordinate(sales_org);
                let ok_arrange = ok.arrange_by_self();
                let error_arrange = error.arrange_by_self();

                ok_arrange.stream.probe_with(&mut probe);
                error_arrange.stream.probe_with(&mut probe);

                (input, ok_arrange.trace, error_arrange.trace)
            });

            {
                input.update(newx("a", Some("b"), 1), 1);
                input.update(newx("b", Some("c"), 1), 1);
                input.update(newx("c", Some("a"), 1), 1);
                input.update(newx("a", Some("b2"), 1), 1);
                input.update(newx("b2", Some("c2"), 1), 1);
                input.update(newx("c2", Some("a"), 1), 1);
                sync!(input, probe, worker);

                let data = trace_to_vec_key_trace(&mut error);
                let compacted = compact_unique_vec_trace(data, Antichain::new());
                println!("{:#?}", compacted);
                assert_eq!(compacted.len(), 6);
                assert_eq!(
                    compacted[0],
                    Error::NonUnique {
                        sales_ldap: "a".into(),
                        month: 1,
                        count: 2,
                    }
                );
                assert_eq!(
                    compacted[1],
                    Error::Cycle {
                        sales_ldap: "a".into(),
                        month: 1,
                    }
                );
                assert_eq!(
                    compacted[2],
                    Error::Cycle {
                        sales_ldap: "b".into(),
                        month: 1,
                    }
                );
                assert_eq!(
                    compacted[3],
                    Error::Cycle {
                        sales_ldap: "b2".into(),
                        month: 1,
                    }
                );
                assert_eq!(
                    compacted[4],
                    Error::Cycle {
                        sales_ldap: "c".into(),
                        month: 1,
                    }
                );
                assert_eq!(
                    compacted[5],
                    Error::Cycle {
                        sales_ldap: "c2".into(),
                        month: 1,
                    }
                );
            }
        });
    }
}
