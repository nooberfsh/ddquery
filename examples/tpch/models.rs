use std::fmt::Debug;
use std::str::FromStr;

use anyhow::{Error, Result};
use chrono::NaiveDate;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::util::{parse_field, parse_field_trim};

pub static DELIM: &str = "|";

pub trait FileName {
    const FILE_NAME: &'static str;
}

#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct Part {
    pub part_key: u64,
    pub name: String,
    pub mfgr: String,
    pub brand: String,
    pub typ: String,
    pub size: i32,
    pub container: String,
    pub retail_price: Decimal,
    pub comment: String,
}

impl FileName for Part {
    const FILE_NAME: &'static str = "part.tbl";
}

impl FromStr for Part {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        let mut fields = s.split(DELIM);
        let ret = Part {
            part_key: parse_field(&mut fields)?,
            name: parse_field(&mut fields)?,
            mfgr: parse_field(&mut fields)?,
            brand: parse_field(&mut fields)?,
            typ: parse_field(&mut fields)?,
            size: parse_field(&mut fields)?,
            container: parse_field(&mut fields)?,
            retail_price: parse_field(&mut fields)?,
            comment: parse_field_trim(&mut fields)?,
        };
        Ok(ret)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct PartSupp {
    pub part_key: u64,
    pub supp_key: u64,
    pub availqty: i32,
    pub supplycost: Decimal,
    pub comment: String,
}

impl FileName for PartSupp {
    const FILE_NAME: &'static str = "partsupp.tbl";
}

impl FromStr for PartSupp {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        let mut fields = s.split(DELIM);
        let ret = PartSupp {
            part_key: parse_field(&mut fields)?,
            supp_key: parse_field(&mut fields)?,
            availqty: parse_field(&mut fields)?,
            supplycost: parse_field(&mut fields)?,
            comment: parse_field_trim(&mut fields)?,
        };
        Ok(ret)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct Supplier {
    pub supp_key: u64,
    pub name: String,
    pub address: String,
    pub nation_key: u64,
    pub phone: String,
    pub acctbal: Decimal,
    pub comment: String,
}

impl FileName for Supplier {
    const FILE_NAME: &'static str = "supplier.tbl";
}

impl FromStr for Supplier {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        let mut fields = s.split(DELIM);
        let ret = Supplier {
            supp_key: parse_field(&mut fields)?,
            name: parse_field(&mut fields)?,
            address: parse_field_trim(&mut fields)?,
            nation_key: parse_field(&mut fields)?,
            phone: parse_field(&mut fields)?,
            acctbal: parse_field(&mut fields)?,
            comment: parse_field_trim(&mut fields)?,
        };
        Ok(ret)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct Customer {
    pub cust_key: u64,
    pub name: String,
    pub address: String,
    pub nation_key: u64,
    pub phone: String,
    pub acctbal: Decimal,
    pub mktsegment: String,
    pub comment: String,
}

impl FromStr for Customer {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        let mut fields = s.split(DELIM);
        let ret = Customer {
            cust_key: parse_field(&mut fields)?,
            name: parse_field(&mut fields)?,
            address: parse_field(&mut fields)?,
            nation_key: parse_field(&mut fields)?,
            phone: parse_field(&mut fields)?,
            acctbal: parse_field(&mut fields)?,
            mktsegment: parse_field(&mut fields)?,
            comment: parse_field_trim(&mut fields)?,
        };
        Ok(ret)
    }
}

impl FileName for Customer {
    const FILE_NAME: &'static str = "customer.tbl";
}

#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct Order {
    pub order_key: u64,
    pub cust_key: u64,
    pub order_status: char,
    pub total_price: Decimal,
    pub order_date: NaiveDate,
    pub order_priority: String,
    pub clerk: String,
    pub ship_priority: i32,
    pub comment: String,
}

impl FromStr for Order {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        let mut fields = s.split(DELIM);
        let ret = Order {
            order_key: parse_field(&mut fields)?,
            cust_key: parse_field(&mut fields)?,
            order_status: parse_field(&mut fields)?,
            total_price: parse_field(&mut fields)?,
            order_date: parse_field(&mut fields)?,
            order_priority: parse_field(&mut fields)?,
            clerk: parse_field(&mut fields)?,
            ship_priority: parse_field(&mut fields)?,
            comment: parse_field_trim(&mut fields)?,
        };
        Ok(ret)
    }
}

impl FileName for Order {
    const FILE_NAME: &'static str = "orders.tbl";
}

#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct LineItem {
    pub order_key: u64,
    pub part_key: u64,
    pub supp_key: u64,
    pub line_number: i32,
    pub quantity: i64,
    pub extended_price: Decimal,
    pub discount: Decimal,
    pub tax: Decimal,
    pub return_flag: char,
    pub line_status: char,
    pub ship_date: NaiveDate,
    pub commit_date: NaiveDate,
    pub receipt_date: NaiveDate,
    pub ship_instruct: String,
    pub ship_mode: String,
    pub comment: String,
}

impl FromStr for LineItem {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        let mut fields = s.split(DELIM);
        let ret = LineItem {
            order_key: parse_field(&mut fields)?,
            part_key: parse_field(&mut fields)?,
            supp_key: parse_field(&mut fields)?,
            line_number: parse_field(&mut fields)?,
            quantity: parse_field(&mut fields)?,
            extended_price: parse_field(&mut fields)?,
            discount: parse_field(&mut fields)?,
            tax: parse_field(&mut fields)?,
            return_flag: parse_field(&mut fields)?,
            line_status: parse_field(&mut fields)?,
            ship_date: parse_field(&mut fields)?,
            commit_date: parse_field(&mut fields)?,
            receipt_date: parse_field(&mut fields)?,
            ship_instruct: parse_field(&mut fields)?,
            ship_mode: parse_field(&mut fields)?,
            comment: parse_field_trim(&mut fields)?,
        };
        Ok(ret)
    }
}

impl FileName for LineItem {
    const FILE_NAME: &'static str = "lineitem.tbl";
}

#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct Nation {
    pub nation_key: u64,
    pub name: String,
    pub region_key: u64,
    pub comment: String,
}

impl FromStr for Nation {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        let mut fields = s.split(DELIM);
        let ret = Nation {
            nation_key: parse_field(&mut fields)?,
            name: parse_field(&mut fields)?,
            region_key: parse_field(&mut fields)?,
            comment: parse_field_trim(&mut fields)?,
        };
        Ok(ret)
    }
}

impl FileName for Nation {
    const FILE_NAME: &'static str = "nation.tbl";
}

#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct Region {
    pub region_key: u64,
    pub name: String,
    pub comment: String,
}

impl FromStr for Region {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        let mut fields = s.split(DELIM);
        let ret = Region {
            region_key: parse_field(&mut fields)?,
            name: parse_field(&mut fields)?,
            comment: parse_field_trim(&mut fields)?,
        };
        Ok(ret)
    }
}

impl FileName for Region {
    const FILE_NAME: &'static str = "region.tbl";
}

#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct Q01Answer {
    pub return_flag: char,
    pub line_status: char,
    pub sum_qty: Decimal,
    pub sum_base_price: Decimal,
    pub sum_disc_price: Decimal,
    pub sum_charge: Decimal,
    pub avg_qty: Decimal,
    pub avg_price: Decimal,
    pub avg_disc: Decimal,
    pub count_order: u64,
}

impl FromStr for Q01Answer {
    type Err = Error;
    fn from_str(line: &str) -> std::result::Result<Self, Self::Err> {
        let mut fields = line.split('|');
        let ret = Q01Answer {
            return_flag: parse_field_trim(&mut fields)?,
            line_status: parse_field_trim(&mut fields)?,
            sum_qty: parse_field_trim(&mut fields)?,
            sum_base_price: parse_field_trim(&mut fields)?,
            sum_disc_price: parse_field_trim(&mut fields)?,
            sum_charge: parse_field_trim(&mut fields)?,
            avg_qty: parse_field_trim(&mut fields)?,
            avg_price: parse_field_trim(&mut fields)?,
            avg_disc: parse_field_trim(&mut fields)?,
            count_order: parse_field_trim(&mut fields)?,
        };
        Ok(ret)
    }
}

impl FileName for Q01Answer {
    const FILE_NAME: &'static str = "q1.out";
}

#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct Q02Answer {
    pub s_acctbal: Decimal,
    pub s_name: String,
    pub n_name: String,
    pub p_partkey: u64,
    pub p_mfgr: String,
    pub s_address: String,
    pub s_phone: String,
    pub s_comment: String,
}

impl FromStr for Q02Answer {
    type Err = Error;
    fn from_str(line: &str) -> std::result::Result<Self, Self::Err> {
        let mut fields = line.split('|');
        let ret = Q02Answer {
            s_acctbal: parse_field_trim(&mut fields)?,
            s_name: parse_field_trim(&mut fields)?,
            n_name: parse_field_trim(&mut fields)?,
            p_partkey: parse_field_trim(&mut fields)?,
            p_mfgr: parse_field_trim(&mut fields)?,
            s_address: parse_field_trim(&mut fields)?,
            s_phone: parse_field_trim(&mut fields)?,
            s_comment: parse_field_trim(&mut fields)?,
        };
        Ok(ret)
    }
}

impl FileName for Q02Answer {
    const FILE_NAME: &'static str = "q2.out";
}

#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct Q03Answer {
    pub l_orderkey: u64,
    pub revenue: Decimal,
    pub o_orderdate: NaiveDate,
    pub o_shippriority: i32,
}

impl FromStr for Q03Answer {
    type Err = Error;
    fn from_str(line: &str) -> std::result::Result<Self, Self::Err> {
        let mut fields = line.split('|');
        let ret = Q03Answer {
            l_orderkey: parse_field_trim(&mut fields)?,
            revenue: parse_field_trim(&mut fields)?,
            o_orderdate: parse_field_trim(&mut fields)?,
            o_shippriority: parse_field_trim(&mut fields)?,
        };
        Ok(ret)
    }
}

impl FileName for Q03Answer {
    const FILE_NAME: &'static str = "q3.out";
}

#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct Q04Answer {
    pub o_orderpriority: String,
    pub order_count: i64,
}

impl FromStr for Q04Answer {
    type Err = Error;
    fn from_str(line: &str) -> std::result::Result<Self, Self::Err> {
        let mut fields = line.split('|');
        let ret = Q04Answer {
            o_orderpriority: parse_field_trim(&mut fields)?,
            order_count: parse_field_trim(&mut fields)?,
        };
        Ok(ret)
    }
}

impl FileName for Q04Answer {
    const FILE_NAME: &'static str = "q4.out";
}

#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct Q05Answer {
    pub n_name: String,
    pub revenue: Decimal,
}

impl FromStr for Q05Answer {
    type Err = Error;
    fn from_str(line: &str) -> std::result::Result<Self, Self::Err> {
        let mut fields = line.split('|');
        let ret = Q05Answer {
            n_name: parse_field_trim(&mut fields)?,
            revenue: parse_field_trim(&mut fields)?,
        };
        Ok(ret)
    }
}

impl FileName for Q05Answer {
    const FILE_NAME: &'static str = "q5.out";
}
