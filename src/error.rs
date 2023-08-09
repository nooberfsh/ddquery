use crate::name::Name;

#[derive(Debug)]
pub enum Error {
    InputNotExists,
    TraceNotExist(Name),
    FailedToStartWorkers(String),
    NameAlreadyExists,
}
