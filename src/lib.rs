use crate::handle::Handle;

pub mod row;
pub mod coord;
pub mod timely;
pub mod worker;
pub mod handle;
pub mod name;
pub mod error;
pub mod gid;
pub mod catalog;

pub struct Config {

}

async fn start(config: Config) -> Handle {
    todo!()
}