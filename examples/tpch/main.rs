use std::time::Instant;

use ddquery::{App, SysDiff, SysTime};
use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::trace::implementations::ord_neu::OrdKeySpine;

use crate::models::{LineItem, Q01Answer};
use crate::query::q01::Q01;
use crate::util::{load_input, load_output};

mod models;
mod query;
mod util;

pub type AnswerTrace<T> = TraceAgent<OrdKeySpine<T, SysTime, SysDiff>>;

const DEFAULT_WORKER_THREADS: usize = 8;
const DEFAULT_BATCH_SIZE: usize = 1000;

fn main() {
    q01();
}

fn q01() {
    let handle = Q01.start(DEFAULT_WORKER_THREADS);

    let path = "dataset";
    let name = "lineitem.tbl";
    let expected = load_output::<Q01Answer>("dataset/answers", "q1.out").unwrap();
    let data = load_input::<LineItem>(path, name, DEFAULT_BATCH_SIZE).unwrap();

    let start = Instant::now();
    for batch in data {
        handle.update(batch);
    }
    let res = Q01::results(&handle);
    assert_eq!(res, expected);

    println!("compute: {:?}", start.elapsed());
}
