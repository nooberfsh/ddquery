#[derive(Debug)]
pub enum Error {
    InputNotExists,
    TraceNotExist,
    FailedToStartWorkers(String),
    AlreadyExists,
}
