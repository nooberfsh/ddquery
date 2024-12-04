pub mod app;
pub mod dataflows;
pub mod error;
pub mod models;
pub mod typedef;
pub mod util;

use crate::models::*;

fn main() {
    let handle = app::start(4);
    handle.upsert_belonging(Belonging::new(1, "s1", 202401));
    handle.upsert_revenue(Revenue::new(1, 3, 202401));
    handle.upsert_sales_org(SalesOrg::new("s1", None::<String>, 202401));
    let res = handle.query_sales_revenue_accu("s1", 202401);
    assert_eq!(res, Ok(3));

    handle.upsert_sales_org(SalesOrg::new("s1", Some("s2"), 202401));
    let res = handle.query_sales_revenue_accu("s1", 202401);
    assert!(res.is_err());

    handle.upsert_sales_org(SalesOrg::new("s2", None::<String>, 202401));
    let res = handle.query_sales_revenue_accu("s1", 202401);
    assert_eq!(res, Ok(3));
    let res = handle.query_sales_revenue_accu("s2", 202401);
    assert_eq!(res, Ok(3));

    handle.upsert_revenue(Revenue::new(2, 5, 202401));
    handle.upsert_belonging(Belonging::new(2, "s2", 202401));
    let res = handle.query_sales_revenue_accu("s2", 202401);
    assert_eq!(res, Ok(8));
}
