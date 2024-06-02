use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use crate::core::Error;
use crate::invoker::Invoker;
use crate::models::{Context, JudgeSolutionTaskConfig, ObjectStore, SolutionStore};

use super::{Task, TaskProcess};

pub struct JudgeSolutionTask {
    solution_store: Arc<SolutionStore>,
}

impl JudgeSolutionTask {
    pub fn new(invoker: &Invoker) -> Self {
        Self {
            solution_store: invoker.solution_store(),
        }
    }
}

#[async_trait::async_trait]
impl TaskProcess for JudgeSolutionTask {
    async fn run(self: Box<Self>, task: Task, _shutdown: CancellationToken) -> Result<(), Error> {
        let config: JudgeSolutionTaskConfig = task.parse_config().await?;
        let solution = self
            .solution_store
            .get(Context::new(), config.solution_id)
            .await?
            .ok_or("Cannot find solution")?;
        todo!()
    }
}
