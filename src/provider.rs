use core::fmt;

use std::io;

use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::RwLock;

use sysinfo::System;

use datafusion::common::arrow;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;

use datafusion::common::DataFusionError;
use datafusion::common::ScalarValue;

use datafusion::execution::TaskContext;

use datafusion::datasource::TableProvider;

use datafusion::logical_expr::Expr;
use datafusion::logical_expr::Operator;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::logical_expr::TableType;

use datafusion::catalog::Session;

use datafusion::physical_expr::EquivalenceProperties;

use datafusion::physical_plan::DisplayAs;
use datafusion::physical_plan::DisplayFormatType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::PlanProperties;
use datafusion::physical_plan::RecordBatchStream;

use datafusion::physical_plan::memory::MemoryStream;

use datafusion::physical_plan::execution_plan::Boundedness;
use datafusion::physical_plan::execution_plan::EmissionType;

#[derive(Debug)]
pub struct ProcProvider {
    sys: Arc<RwLock<System>>,
    sch: Arc<Schema>,
}

impl ProcProvider {
    pub fn new() -> Self {
        let sys = System::new();
        let sch = crate::basic::schema_new();
        Self {
            sys: Arc::new(RwLock::new(sys)),
            sch,
        }
    }
}

impl Default for ProcProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl ProcProvider {
    pub fn to_plan(
        &self,
        proj: Option<&Vec<usize>>,
        pid: Option<u32>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let pe = ProcExec::new(proj, self.sch.clone(), self.sys.clone(), pid)?;
        Ok(Arc::new(pe))
    }
}

#[async_trait::async_trait]
impl TableProvider for ProcProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<Schema> {
        self.sch.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _session: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let s: Arc<Schema> = self.sch.clone();
        let sys: Arc<RwLock<_>> = self.sys.clone();

        let mut opid: Option<u32> = None;
        for filt in filters {
            match filt {
                Expr::BinaryExpr(be) => {
                    let op: &Operator = &be.op;
                    if op != &Operator::Eq {
                        continue;
                    }

                    let lft: &Expr = &be.left;
                    let colname: &str = match lft {
                        Expr::Column(c) => &c.name,
                        _ => "",
                    };
                    if colname != "pid" {
                        continue;
                    }

                    let rht: &Expr = &be.right;
                    let oval: Option<&ScalarValue> = match rht {
                        Expr::Literal(sval, _) => Some(sval),
                        _ => None,
                    };

                    let pval: Option<u32> = match oval {
                        Some(&ScalarValue::UInt32(ou)) => ou,
                        _ => None,
                    };
                    opid = pval;
                }
                _ => continue,
            }
        }

        let pe = ProcExec::new(projection, s, sys, opid)?;
        Ok(Arc::new(pe))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
}

#[derive(Debug)]
pub struct ProcExec {
    sys: Arc<RwLock<System>>,
    pid: Option<u32>,
    projected: Arc<Schema>,
    props: PlanProperties,
}

impl ProcExec {
    pub fn new(
        projections: Option<&Vec<usize>>,
        schema: Arc<Schema>,
        sys: Arc<RwLock<System>>,
        pid: Option<u32>,
    ) -> Result<Self, DataFusionError> {
        let projected = Self::sch2projected(&schema, projections)?;

        let props = Self::sch2props(projected.clone());

        Ok(Self {
            sys,
            pid,
            projected,
            props,
        })
    }

    pub fn sch2projected(
        s: &Arc<Schema>,
        projections: Option<&Vec<usize>>,
    ) -> Result<Arc<Schema>, DataFusionError> {
        datafusion::physical_plan::project_schema(s, projections)
    }

    pub fn sch2props(s: Arc<Schema>) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(s),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
    }
}

impl ProcExec {
    pub fn sys2batch(&self) -> Result<RecordBatch, io::Error> {
        let lck: &RwLock<_> = &self.sys;
        let mut guard = lck
            .try_write()
            .map_err(|_| io::Error::other("unable to lock the sys obj"))?;
        let msys: &mut System = &mut guard;
        crate::basic::update_all(msys);

        let sys: &System = &guard;
        crate::basic::sys2batch(sys)
    }

    pub fn pid2batch(&self, pid: u32) -> Result<RecordBatch, io::Error> {
        let lck: &RwLock<_> = &self.sys;
        let mut guard = lck
            .try_write()
            .map_err(|_| io::Error::other("unable to lock the sys obj"))?;
        let msys: &mut System = &mut guard;
        crate::basic::update_pid(msys, pid);

        let sys: &System = &guard;
        crate::basic::pid2batch(sys, pid)
    }

    pub fn to_batch(&self) -> Result<RecordBatch, io::Error> {
        match self.pid {
            Some(p) => self.pid2batch(p),
            None => self.sys2batch(),
        }
    }

    pub fn to_memstrm(&self) -> Result<MemoryStream, DataFusionError> {
        let b: RecordBatch = self.to_batch()?;
        MemoryStream::try_new(vec![b], self.projected.clone(), None)
    }
}

impl DisplayAs for ProcExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ProcExec")
    }
}

impl ExecutionPlan for ProcExec {
    fn name(&self) -> &str {
        "proc_exec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<(dyn ExecutionPlan + 'static)>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<(dyn ExecutionPlan + 'static)>>,
    ) -> Result<Arc<(dyn ExecutionPlan + 'static)>, DataFusionError> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<Pin<Box<dyn RecordBatchStream + Send + 'static>>, DataFusionError> {
        let mstr: MemoryStream = self.to_memstrm()?;
        Ok(Box::pin(mstr))
    }
}
