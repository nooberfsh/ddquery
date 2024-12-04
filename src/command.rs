use crossbeam::channel::Sender;

use crate::internal::{SysInternal, SysInternalWorker};
use crate::SysTime;

#[derive(Debug)]
pub enum ClientCommand<Q, U> {
    Query(Q),
    Update(U),
    CollectInternal(Sender<SysInternal>),
    DropApp,
}

#[derive(Debug)]
pub enum ServerCommand<Q, U> {
    Query(Q, SysTime),
    Update(U),
    ControlCommand(ControlCommand),
}

#[derive(Clone, Debug)]
pub enum ControlCommand {
    AdvanceTimestamp(SysTime),
    CollectInternal(Sender<SysInternalWorker>),
    Shutdown,
}

impl<Q, U> From<ControlCommand> for ServerCommand<Q, U> {
    fn from(value: ControlCommand) -> Self {
        ServerCommand::ControlCommand(value)
    }
}

impl<Q, U> From<(Q, SysTime)> for ServerCommand<Q, U> {
    fn from((q, t): (Q, SysTime)) -> Self {
        ServerCommand::Query(q, t)
    }
}
