use std::time::Instant;

use ddquery::{App, SysDiff, SysTime};
use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::trace::implementations::ord_neu::OrdKeySpine;

use crate::models::*;
use crate::query::q01::Q01;
use crate::query::q02::Q02;
use crate::query::q03::Q03;
use crate::util::{load_input, load_output};

mod macros;
mod models;
mod query;
mod util;

pub type AnswerTrace<T> = TraceAgent<OrdKeySpine<T, SysTime, SysDiff>>;

const DEFAULT_WORKER_THREADS: usize = 4;
const DEFAULT_BATCH_SIZE: usize = 1000;

fn main() {
    q01();
    q02();
    q03();
}

fn q01() {
    let handle = Q01.start(DEFAULT_WORKER_THREADS);

    let path = "dataset";
    let expected = load_output::<Q01Answer>("examples/tpch/answers", "q1.out").unwrap();
    let batches = load_inputs!(handle, path, DEFAULT_BATCH_SIZE, LineItem);

    let start = Instant::now();
    let res = Q01::results(&handle);
    assert_eq!(res, expected);

    println!(
        "compute finish, time: {:?}: batches: {batches}",
        start.elapsed()
    );

    let internal = handle.collect_internal_data();
    println!("internal: {:#?}", internal);
}

fn q02() {
    let handle = Q02.start(DEFAULT_WORKER_THREADS);

    let path = "dataset";
    let expected = load_output::<Q02Answer>("examples/tpch/answers", "q2.out").unwrap();
    let batches = load_inputs!(
        handle,
        path,
        DEFAULT_BATCH_SIZE,
        Part,
        Supplier,
        PartSupp,
        Nation,
        Region
    );

    let start = Instant::now();
    let res = Q02::results(&handle);
    assert_eq!(res, expected);

    println!(
        "compute finish, time: {:?}: batches: {batches}",
        start.elapsed()
    );
}

fn q03() {
    let handle = Q03.start(DEFAULT_WORKER_THREADS);

    let path = "dataset";
    let expected = load_output::<Q03Answer>("examples/tpch/answers", "q3.out").unwrap();
    let batches = load_inputs!(handle, path, DEFAULT_BATCH_SIZE, Customer, Order, LineItem);

    let start = Instant::now();
    let res = Q03::results(&handle);
    assert_eq!(res, expected);

    println!(
        "compute finish, time: {:?}: batches: {batches}",
        start.elapsed()
    );
}
