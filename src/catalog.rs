use std::collections::HashSet;

use differential_dataflow::Hashable;

use crate::error::Error;
use crate::name::Name;
use crate::row::Row;

pub struct Catalog {
    workers: usize,
    input: HashSet<Name>,
    trace: HashSet<Name>,
}

impl Catalog {
    pub fn new(workers: usize) -> Self {
        Catalog {
            workers,
            input: HashSet::new(),
            trace: HashSet::new(),
        }
    }

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

    pub fn input_exists(&self, name: &Name) -> bool {
        self.input.get(name).is_some()
    }

    pub fn trace_exists(&self, name: &Name) -> bool {
        self.trace.get(name).is_some()
    }

    pub fn determine_trace_worker(&self, name: &Name, key: &Row) -> Result<usize, Error> {
        if self.trace_exists(name) {
            Ok(key.hashed() as usize % self.workers)
        } else {
            Err(Error::TraceNotExist)
        }
    }

    fn check_name(&self, name: &Name) -> Result<(), Error> {
        if self.input.contains(name) || self.trace.contains(name) {
            Err(Error::NameAlreadyExists)
        } else {
            Ok(())
        }
    }
}
