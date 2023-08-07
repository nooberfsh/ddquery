pub mod row;
pub mod coord;
pub mod typedef;
pub mod worker;
pub mod handle;
pub mod name;
pub mod error;
pub mod gid;
pub mod catalog;

pub struct Config {
}

async fn start(config: Config) -> Result<handle::Handle, error::Error> {
    todo!()
}