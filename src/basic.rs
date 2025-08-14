use std::collections::HashMap;
use std::io;
use std::sync::Arc;

use datafusion::common::arrow;

use arrow::array::ArrayRef;
use arrow::array::Float32Array;
use arrow::array::RecordBatch;
use arrow::array::StringArray;
use arrow::array::UInt32Array;
use arrow::array::UInt64Array;

use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;

use datafusion::datasource::MemTable;

use sysinfo::Pid;
use sysinfo::Process;
use sysinfo::ProcessesToUpdate;
use sysinfo::System;

pub struct BasicProcInfo {
    pub pid: u32,
    pub cpu_usage: f32,
    pub cwd: Option<String>,
    pub exec: String,
    pub group_id: Option<String>,
    pub memory: u64,
    pub name: String,
    pub parent: Option<u32>,
    pub runtime: u64,
    pub session_id: Option<u32>,
    pub start_time: u64,
    pub status: String,
    pub user_id: String,
    pub virtual_memory: u64,
}

impl From<&Process> for BasicProcInfo {
    fn from(p: &Process) -> Self {
        let pid = p.pid().as_u32();
        let cpu_usage = p.cpu_usage();
        let cwd = p.cwd().map(|c| c.to_string_lossy().into_owned());
        let exec = p
            .exe()
            .map(|e| e.to_string_lossy().into_owned())
            .unwrap_or_default();
        let group_id = p.group_id().map(|g| g.to_string());
        let memory = p.memory();
        let name = p.name().to_string_lossy().into_owned();
        let parent = p.parent().map(|p| p.as_u32());
        let runtime = p.run_time();
        let session_id = p.session_id().map(|s| s.as_u32());
        let start_time = p.start_time();
        let status = format!("{}", p.status());
        let user_id = p.user_id().map(|u| u.to_string()).unwrap_or_default();
        let virtual_memory = p.virtual_memory();

        BasicProcInfo {
            pid,
            cpu_usage,
            cwd,
            exec,
            group_id,
            memory,
            name,
            parent,
            runtime,
            session_id,
            start_time,
            status,
            user_id,
            virtual_memory,
        }
    }
}

pub fn sys2procs(s: &System) -> &HashMap<Pid, Process> {
    s.processes()
}

pub fn update_all(s: &mut System) -> usize {
    s.refresh_processes(ProcessesToUpdate::All, true)
}

pub fn update_pid(s: &mut System, pid: u32) -> usize {
    let p: Pid = Pid::from_u32(pid);
    s.refresh_processes(ProcessesToUpdate::Some(&[p]), true)
}

pub fn sys2basic_procs(s: &System) -> impl Iterator<Item = BasicProcInfo> {
    s.processes().values().map(BasicProcInfo::from)
}

pub fn pid2basic_proc(s: &System, pid: u32) -> Option<BasicProcInfo> {
    s.process(Pid::from_u32(pid)).map(BasicProcInfo::from)
}

pub fn schema_new() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("pid", DataType::UInt32, false),
        Field::new("cpu_usage", DataType::Float32, false),
        Field::new("cwd", DataType::Utf8, true),
        Field::new("exec", DataType::Utf8, false),
        Field::new("group_id", DataType::Utf8, true),
        Field::new("memory", DataType::UInt64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("parent", DataType::UInt32, true),
        Field::new("runtime", DataType::UInt64, false),
        Field::new("session_id", DataType::UInt32, true),
        Field::new("start_time", DataType::UInt64, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("user_id", DataType::Utf8, false),
        Field::new("virtual_memory", DataType::UInt64, false),
    ]))
}

pub fn procs2batch<I>(procs: I) -> Result<RecordBatch, io::Error>
where
    I: Iterator<Item = BasicProcInfo>,
{
    let mut pid: Vec<u32> = Vec::new();
    let mut cpu_usage: Vec<f32> = Vec::new();
    let mut cwd: Vec<Option<String>> = Vec::new();
    let mut exec: Vec<String> = Vec::new();
    let mut group_id: Vec<Option<String>> = Vec::new();
    let mut memory: Vec<u64> = Vec::new();
    let mut name: Vec<String> = Vec::new();
    let mut parent: Vec<Option<u32>> = Vec::new();
    let mut runtime: Vec<u64> = Vec::new();
    let mut session_id: Vec<Option<u32>> = Vec::new();
    let mut start_time: Vec<u64> = Vec::new();
    let mut status: Vec<String> = Vec::new();
    let mut user_id: Vec<String> = Vec::new();
    let mut virtual_memory: Vec<u64> = Vec::new();

    for p in procs {
        pid.push(p.pid);
        cpu_usage.push(p.cpu_usage);
        cwd.push(p.cwd);
        exec.push(p.exec);
        group_id.push(p.group_id);
        memory.push(p.memory);
        name.push(p.name);
        parent.push(p.parent);
        runtime.push(p.runtime);
        session_id.push(p.session_id);
        start_time.push(p.start_time);
        status.push(p.status);
        user_id.push(p.user_id);
        virtual_memory.push(p.virtual_memory);
    }

    let pid_arr: ArrayRef = Arc::new(UInt32Array::from(pid));
    let cpu_arr: ArrayRef = Arc::new(Float32Array::from(cpu_usage));
    let cwd_arr: ArrayRef = Arc::new(StringArray::from(cwd));
    let exec_arr: ArrayRef = Arc::new(StringArray::from(exec));
    let group_arr: ArrayRef = Arc::new(StringArray::from(group_id));
    let memory_arr: ArrayRef = Arc::new(UInt64Array::from(memory));
    let name_arr: ArrayRef = Arc::new(StringArray::from(name));
    let parent_arr: ArrayRef = Arc::new(UInt32Array::from(parent));
    let runtime_arr: ArrayRef = Arc::new(UInt64Array::from(runtime));
    let session_arr: ArrayRef = Arc::new(UInt32Array::from(session_id));
    let start_arr: ArrayRef = Arc::new(UInt64Array::from(start_time));
    let status_arr: ArrayRef = Arc::new(StringArray::from(status));
    let user_arr: ArrayRef = Arc::new(StringArray::from(user_id));
    let virt_arr: ArrayRef = Arc::new(UInt64Array::from(virtual_memory));

    let schema: Arc<Schema> = schema_new();

    RecordBatch::try_new(
        schema,
        vec![
            pid_arr,
            cpu_arr,
            cwd_arr,
            exec_arr,
            group_arr,
            memory_arr,
            name_arr,
            parent_arr,
            runtime_arr,
            session_arr,
            start_arr,
            status_arr,
            user_arr,
            virt_arr,
        ],
    )
    .map_err(io::Error::other)
}

pub fn pid2batch(s: &System, pid: u32) -> Result<RecordBatch, io::Error> {
    let ob: Option<BasicProcInfo> = pid2basic_proc(s, pid);
    let v: Vec<BasicProcInfo> = ob.map(|b| vec![b]).unwrap_or_default();
    procs2batch(v.into_iter())
}

pub fn sys2batch(s: &System) -> Result<RecordBatch, io::Error> {
    let basic_procs = sys2basic_procs(s);
    procs2batch(basic_procs)
}

pub fn batch2table(b: RecordBatch) -> Result<MemTable, io::Error> {
    MemTable::try_new(b.schema(), vec![vec![b]]).map_err(io::Error::other)
}

pub fn sys2mtab(s: &System) -> Result<MemTable, io::Error> {
    let batch = sys2batch(s)?;
    batch2table(batch)
}
