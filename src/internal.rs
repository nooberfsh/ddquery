use crate::SysTime;

#[derive(Clone, Debug)]
pub struct SysInternal {
    pub coord: SysInternalCoord,
    // ordered by worker index
    pub workers: Vec<SysInternalWorker>,
}

#[derive(Clone, Debug)]
pub struct SysInternalCoord {
    pub workers: usize,
    pub frontier: SysTime,
}

#[derive(Clone, Debug)]
pub struct SysInternalWorker {
    pub index: usize,
    pub frontier: SysTime,
    pub upsert_input_info: Vec<SysInternalInput>,
    pub input_info: Vec<SysInternalInput>,
    pub trace_info: Vec<SysInternalTrace>,
}

#[derive(Clone, Debug)]
pub struct SysInternalTrace {
    pub name: String,
    pub logical_compaction: SysTime,
    pub physical_compaction: SysTime,
}

#[derive(Clone, Debug)]
pub struct SysInternalInput {
    pub name: String,
    pub time: SysTime,
}
