use anyhow::{bail, Error};
use ddquery::{App, Handle, SysDiff, SysTime};
use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::trace::implementations::ord_neu::OrdKeySpine;
use std::fmt::Debug;
use std::str::FromStr;
use std::time::Instant;

use crate::models::*;
use crate::query::q01::Q01;
use crate::query::q02::Q02;
use crate::query::q03::Q03;
use crate::query::q04::Q04;
use crate::query::q05::Q05;
use crate::util::load_output;

mod macros;
mod models;
mod query;
mod util;

pub type AnswerTrace<T> = TraceAgent<OrdKeySpine<T, SysTime, SysDiff>>;

const DEFAULT_WORKER_THREADS: usize = 4;
const DEFAULT_BATCH_SIZE: usize = 1000;
static DEFAULT_DATA_SET_PATH: &str = "dataset";

pub trait TpchQuery: App {
    type Answer: FileName + FromStr<Err = Error> + Eq + PartialEq + Debug;

    fn new() -> Self;

    fn load(handle: &Handle<Self>, path: &str, batch_size: usize) -> usize;

    fn load_answer() -> anyhow::Result<Vec<Self::Answer>> {
        load_output::<Self::Answer>("examples/tpch/answers", Self::Answer::FILE_NAME)
    }
}

pub trait TpchResults: TpchQuery {
    fn results(handle: &Handle<Self>) -> Vec<Self::Answer>;
}

fn run<T: TpchResults>(workers: usize, batch_size: usize, path: &str) -> anyhow::Result<()> {
    let tpch = T::new();
    println!("running query: {}", tpch.name());
    let handle = tpch.start(workers);

    let expected = T::load_answer()?;
    let batches = T::load(&handle, path, batch_size);

    let start = Instant::now();
    let res = T::results(&handle);
    assert_eq!(res, expected);

    println!(
        "compute {} finish, time: {:?}: batches: {batches}",
        tpch.name(),
        start.elapsed()
    );
    Ok(())
}

fn main() -> anyhow::Result<()> {
    let workers = DEFAULT_WORKER_THREADS;
    let batch_size = DEFAULT_BATCH_SIZE;
    let data_set = DEFAULT_DATA_SET_PATH;
    let d: usize = std::env::args().nth(1).unwrap().parse().unwrap();
    match d {
        1 => run::<Q01>(workers, batch_size, data_set)?,
        2 => run::<Q02>(workers, batch_size, data_set)?,
        3 => run::<Q03>(workers, batch_size, data_set)?,
        4 => run::<Q04>(workers, batch_size, data_set)?,
        5 => run::<Q05>(workers, batch_size, data_set)?,
        _ => bail!("query {d} not implemented yet"),
    }
    Ok(())
}
