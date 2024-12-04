use crate::models::{Belonging, Revenue, SalesRevenue};
use ddquery::SysDiff;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::operators::*;
use differential_dataflow::Collection;
use timely::dataflow::Scope;
use timely::order::TotalOrder;

pub fn sales_revenue<G>(
    belonging: Collection<G, Belonging, SysDiff>,
    user_revenue: Collection<G, Revenue, SysDiff>,
) -> Collection<G, SalesRevenue, SysDiff>
where
    G: Scope,
    G::Timestamp: Lattice + Ord,
{
    let belonging_arrange = belonging
        .map(|b| ((b.uid, b.month), (b.sales_ldap)))
        .arrange_by_key();
    let user_revenue_arrange = user_revenue
        .map(|u| ((u.uid, u.month), u.revenue))
        .arrange_by_key();
    belonging_arrange
        .join_core(&user_revenue_arrange, |(_, month), sales_ldap, revenue| {
            Some(((sales_ldap.clone(), month.clone()), *revenue))
        })
        .arrange_by_key()
        .reduce_named("Reduce", |_key, input, output| {
            let mut s_revenue: i64 = 0;
            for (r, d) in input {
                s_revenue += (**r) * (*d as i64);
            }
            if s_revenue != 0 {
                output.push((s_revenue, 1));
            }
        })
        .map(|((sales_ldap, month), revenue)| SalesRevenue {
            sales_ldap,
            month,
            revenue,
        })
}

pub fn sales_revenue_in_place<G>(
    belonging: Collection<G, Belonging, SysDiff>,
    user_revenue: Collection<G, Revenue, SysDiff>,
) -> Collection<G, SalesRevenue, SysDiff>
where
    G: Scope,
    G::Timestamp: Lattice + Ord + TotalOrder,
{
    let belonging_arrange = belonging
        .map(|b| ((b.uid, b.month), b.sales_ldap))
        .arrange_by_key();
    let user_revenue_arrange = user_revenue
        .map(|u| ((u.uid, u.month), u.revenue))
        .arrange_by_key();
    belonging_arrange
        .join_core(&user_revenue_arrange, |(_, month), sales_ldap, revenue| {
            Some(((sales_ldap.clone(), month.clone()), *revenue))
        })
        .explode(|(key, revenue)| Some((key, revenue)))
        .count_total_core()
        .map(|((sales_ldap, month), revenue)| SalesRevenue {
            sales_ldap,
            month,
            revenue,
        })
}
