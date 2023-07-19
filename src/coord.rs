use timely::communication::Allocate;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::timely::Timestamp;

pub struct TraceManager {

}

pub struct Context<T> {
    epoch: T,
}

// coord => worker
pub enum CoordCommand {
    CreateInput {
        name: String,
    }
}

// worker => coord
pub enum Response {

}

pub struct Coord {
    epoch: Timestamp,

}

impl Coord {
    pub async fn run(&mut self) {
        // match self.req_rx.recv().await.unwrap() {
        //     Request::CreateInput { .. } => {}
        //     Request::CreateDerive { .. } => {}
        //     Request::Query { .. } => {}
        //      => {}
        // }
    }
}

