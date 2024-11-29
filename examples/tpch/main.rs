use std::time::Instant;

use ddquery::{App, SysDiff, SysTime};
use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::trace::implementations::ord_neu::OrdKeySpine;

use crate::models::*;
use crate::query::q01::Q01;
use crate::query::q02;
use crate::query::q02::Q02;
use crate::util::{load_input, load_output};

mod models;
mod query;
mod util;

pub type AnswerTrace<T> = TraceAgent<OrdKeySpine<T, SysTime, SysDiff>>;

const DEFAULT_WORKER_THREADS: usize = 4;
const DEFAULT_BATCH_SIZE: usize = 1000;

fn main() {
    q01();
    q02();
}

fn q01() {
    let handle = Q01.start(DEFAULT_WORKER_THREADS);

    let path = "dataset";
    let name = "lineitem.tbl";
    let expected = load_output::<Q01Answer>("examples/tpch/answers", "q1.out").unwrap();
    let data = load_input::<LineItem>(path, name, DEFAULT_BATCH_SIZE).unwrap();
    let batches = data.len();

    let start = Instant::now();
    for batch in data {
        handle.update(batch);
    }
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
    let mut batches = 0;

    {
        let data = load_input::<Part>(path, "part.tbl", DEFAULT_BATCH_SIZE).unwrap();
        batches += data.len();
        for batch in data {
            handle.update(q02::Update::Part(batch));
        }

        let data = load_input::<Supplier>(path, "supplier.tbl", DEFAULT_BATCH_SIZE).unwrap();
        batches += data.len();
        for batch in data {
            handle.update(q02::Update::Supplier(batch));
        }

        let data = load_input::<PartSupp>(path, "partsupp.tbl", DEFAULT_BATCH_SIZE).unwrap();
        batches += data.len();
        for batch in data {
            handle.update(q02::Update::PartSupp(batch));
        }

        let data = load_input::<Nation>(path, "nation.tbl", DEFAULT_BATCH_SIZE).unwrap();
        batches += data.len();
        for batch in data {
            handle.update(q02::Update::Nation(batch));
        }

        let data = load_input::<Region>(path, "region.tbl", DEFAULT_BATCH_SIZE).unwrap();
        batches += data.len();
        for batch in data {
            handle.update(q02::Update::Region(batch));
        }
    }

    let start = Instant::now();
    let res = Q02::results(&handle);
    assert_eq!(res, expected);

    println!(
        "compute finish, time: {:?}: batches: {batches}",
        start.elapsed()
    );
}
