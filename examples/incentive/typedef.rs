use ddquery::{SysDiff, SysTime};
use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::trace::implementations::ord_neu::{OrdKeySpine, OrdValSpine};

use crate::error::Error;
use crate::models::{Month, SalesRevenue};

pub type SalesMonthKey = (String, Month);
pub type SalesMonthRangeKey = (String, Month, Month);
pub type UidMonthKey = (u64, Month);
pub type SalesRevenueAccuTrace =
    TraceAgent<OrdValSpine<SalesMonthKey, SalesRevenue, SysTime, SysDiff>>;
pub type ErrorTrace = TraceAgent<OrdKeySpine<Error, SysTime, SysDiff>>;
