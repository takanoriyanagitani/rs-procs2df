use std::io;
use std::process::ExitCode;
use std::sync::Arc;

use rs_procs2df::datafusion;

use datafusion::prelude::*;

use rs_procs2df::provider::ProcProvider;

fn env2pid() -> Option<u32> {
    std::env::var("PROC_PID")
        .ok()
        .and_then(|s| str::parse(&s).ok())
}

fn env2name() -> Option<String> {
    std::env::var("PROC_NAME_LIKE").ok()
}

async fn sub() -> Result<(), io::Error> {
    let ctx = SessionContext::new();

    let pp: ProcProvider = ProcProvider::new();

    ctx.register_table("proc_table", Arc::new(pp))?;

    let query: String = std::env::var("ENV_SQL").unwrap_or_default();

    let mut df = ctx.sql(&query).await?;

    let opid: Option<u32> = env2pid();
    if let Some(p) = opid {
        df = df.filter(col("pid").eq(lit(p)))?;
    }

    let oname: Option<String> = env2name();
    if let Some(n) = oname {
        df = df.filter(col("name").like(lit(n)))?;
    }

    df.show().await?;

    Ok(())
}

#[tokio::main]
async fn main() -> ExitCode {
    match sub().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{e}");
            ExitCode::FAILURE
        }
    }
}
