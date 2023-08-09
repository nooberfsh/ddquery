use crate::name::Name;

#[derive(Debug)]
pub enum Error {
    InputNotExists(Name),
    TraceNotExist(Name),
    FailedToStartWorkers(String),
    InputAlreadyExists(Name),
    TraceAlreadyExists(Name),
}
