use std::time::Duration;
use timely::WorkerConfig;

#[tokio::test]
async fn smoke() {
    tracing_subscriber::fmt::init();
    let config = ddquery::Config {
        workers: 1,
        worker_config: WorkerConfig::default(),
    };
    let handle = ddquery::start(config).await.unwrap();

    handle.create_input_and_trace("t1").await.unwrap();

    handle.insert("t1", 1, 103).await.unwrap();
    let d = handle.query_one("t1", 1).await.unwrap();
    assert_eq!(d, Some(103));

    handle.delete("t1", 1).await.unwrap();
    let d = handle.query_one("t1", 1).await.unwrap();
    assert_eq!(d, None);
}
