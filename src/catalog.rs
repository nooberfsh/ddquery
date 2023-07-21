use std::collections::HashSet;

use crate::error::Error;
use crate::name::Name;

pub struct Catalog {
    input: HashSet<Name>,
    trace: HashSet<Name>,
}

impl Catalog {
    pub fn create_input(&mut self, name: Name) -> Result<(), Error> {
        self.check_name(&name)?;
        self.input.insert(name);
        Ok(())
    }

    pub fn create_trace(&mut self, name: Name) -> Result<(), Error> {
        self.check_name(&name)?;
        self.trace.insert(name);
        Ok(())
    }

    pub fn check_name(&self, name: &Name) -> Result<(), Error> {
        todo!()
    }
}
