use serde::{Deserialize, Serialize};

use crate::models::Month;

#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub enum Error {
    NonUnique {
        sales_ldap: String,
        month: Month,
        count: i64,
    },
    InvalidLeader {
        sales_ldap: String,
        month: Month,
    },
    Cycle {
        sales_ldap: String,
        month: Month,
    },
}
