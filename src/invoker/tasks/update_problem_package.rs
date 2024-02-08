use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use crate::core::Error;
use crate::invoker::Invoker;

use super::{TaskGuard, TaskProcess};

pub struct UpdateProblemPackageTask {}

impl UpdateProblemPackageTask {
    pub fn new(_invoker: &Invoker) -> Self {
        Self {}
    }
}

#[async_trait::async_trait]
impl TaskProcess for UpdateProblemPackageTask {
    async fn run(&self, _task: Arc<TaskGuard>, _shutdown: CancellationToken) -> Result<(), Error> {
        todo!()
    }
}
