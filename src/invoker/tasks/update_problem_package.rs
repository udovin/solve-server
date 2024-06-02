use tokio_util::sync::CancellationToken;

use crate::core::Error;
use crate::invoker::Invoker;
use crate::models::UpdateProblemPackageTaskConfig;

use super::{Task, TaskProcess};

pub struct UpdateProblemPackageTask {}

impl UpdateProblemPackageTask {
    pub fn new(_invoker: &Invoker) -> Self {
        Self {}
    }
}

#[async_trait::async_trait]
impl TaskProcess for UpdateProblemPackageTask {
    async fn run(self: Box<Self>, task: Task, _shutdown: CancellationToken) -> Result<(), Error> {
        let _config: UpdateProblemPackageTaskConfig = task.parse_config().await?;
        todo!()
    }
}
