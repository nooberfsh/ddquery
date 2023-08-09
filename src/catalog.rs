use std::collections::HashSet;

use differential_dataflow::Hashable;

use crate::error::Error;
use crate::name::Name;
use crate::typedef::Data;

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

    pub fn create_input_and_trace(&mut self, name: Name) -> Result<(), Error> {
        if self.input_exists(&name) {
            Err(Error::InputAlreadyExists(name))
        } else if self.trace_exists(&name) {
            Err(Error::TraceAlreadyExists(name))
        } else {
            self.input.insert(name.clone());
            self.trace.insert(name);
            Ok(())
        }
    }

    pub fn create_input(&mut self, name: Name) -> Result<(), Error> {
        if self.input_exists(&name) {
            Err(Error::InputAlreadyExists(name))
        } else {
            self.input.insert(name);
            Ok(())
        }
    }

    pub fn create_trace(&mut self, name: Name) -> Result<(), Error> {
        if self.trace_exists(&name) {
            Err(Error::TraceAlreadyExists(name))
        } else {
            self.trace.insert(name);
            Ok(())
        }
    }

    pub fn input_exists(&self, name: &Name) -> bool {
        self.input.get(name).is_some()
    }

    pub fn trace_exists(&self, name: &Name) -> bool {
        self.trace.get(name).is_some()
    }

    pub fn determine_trace_worker<K: Data>(&self, name: &Name, key: &K) -> Result<usize, Error> {
        if self.trace_exists(name) {
            Ok(key.hashed() as usize % self.workers)
        } else {
            Err(Error::TraceNotExist(name.clone()))
        }
    }
}
