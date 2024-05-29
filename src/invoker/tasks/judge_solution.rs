use tokio_util::sync::CancellationToken;

use crate::core::Error;
use crate::invoker::Invoker;
use crate::models::JudgeSolutionTaskConfig;

use super::{Task, TaskProcess};

pub struct JudgeSolutionTask {}

impl JudgeSolutionTask {
    pub fn new(_invoker: &Invoker) -> Self {
        Self {}
    }
}

#[async_trait::async_trait]
impl TaskProcess for JudgeSolutionTask {
    async fn run(&self, task: Task, _shutdown: CancellationToken) -> Result<(), Error> {
        let _config: JudgeSolutionTaskConfig = task.parse_config().await?;
        todo!()
    }
}
