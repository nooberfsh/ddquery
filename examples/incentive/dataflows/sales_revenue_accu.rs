use crate::models::{SalesRevenue, SalesSubordinate};
use ddquery::SysDiff;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::operators::*;
use differential_dataflow::Collection;
use timely::dataflow::Scope;
use timely::order::TotalOrder;

pub fn sales_revenue_accu<G>(
    subordinate: Collection<G, SalesSubordinate, SysDiff>,
    sales_revenue: Collection<G, SalesRevenue, SysDiff>,
) -> Collection<G, SalesRevenue, SysDiff>
where
    G: Scope,
    G::Timestamp: Lattice + Ord + TotalOrder,
{
    let subordinate_arrange = subordinate
        .map(|s| ((s.subordinate_ldap, s.month), s.sales_ldap))
        .arrange_by_key();

    let sales_revenue_arrange = sales_revenue
        .map(|s| ((s.sales_ldap, s.month), s.revenue))
        .arrange_by_key();

    subordinate_arrange
        .join_core(&sales_revenue_arrange, |(_, month), sales_ldap, revenue| {
            Some(((sales_ldap.clone(), *month), *revenue))
        })
        .explode(|(key, revenue)| Some((key, revenue)))
        .count_total_core()
        .map(|((sales_ldap, month), revenue)| SalesRevenue {
            sales_ldap,
            month,
            revenue,
        })
}
