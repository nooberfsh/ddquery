use ddquery::timely_util::upsert_input::UpsertInput;
use serde::{Deserialize, Serialize};

/// format: yyyyMM, e.g. 202405
pub type Month = u64;

///////////////////////// input ////////////////////////////////
#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct Belonging {
    pub uid: u64,
    pub sales_ldap: String,
    pub month: Month,
}

impl UpsertInput for Belonging {
    type Key = (u64, Month);
    fn get_key(&self) -> Self::Key {
        (self.uid, self.month)
    }
}

impl Belonging {
    pub fn new(uid: u64, sales_ldap: impl Into<String>, month: Month) -> Self {
        Belonging {
            uid,
            sales_ldap: sales_ldap.into(),
            month,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct SalesOrg {
    pub sales_ldap: String,
    pub leader: Option<String>,
    pub month: Month,
}

impl UpsertInput for SalesOrg {
    type Key = (String, Month);
    fn get_key(&self) -> Self::Key {
        (self.sales_ldap.clone(), self.month)
    }
}

impl SalesOrg {
    pub fn new<T>(sales_ldap: impl Into<String>, leader: Option<T>, month: Month) -> Self
    where
        T: Into<String>,
    {
        SalesOrg {
            sales_ldap: sales_ldap.into(),
            leader: leader.map(Into::into),
            month,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct Revenue {
    pub uid: u64,
    pub revenue: i64,
    pub month: Month,
}

impl UpsertInput for Revenue {
    type Key = (u64, Month);
    fn get_key(&self) -> Self::Key {
        (self.uid, self.month)
    }
}

impl Revenue {
    pub fn new(uid: u64, revenue: i64, month: Month) -> Self {
        Revenue {
            uid,
            revenue,
            month,
        }
    }
}

///////////////////////// output ////////////////////////////////
#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct SalesSubordinate {
    pub sales_ldap: String,
    pub subordinate_ldap: String,
    pub month: Month,
}

///////////////////////// output ////////////////////////////////
#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct SalesRevenue {
    pub sales_ldap: String,
    pub revenue: i64,
    pub month: Month,
}
