use std::sync::Arc;

use slog::Logger;
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;

use crate::core::Error;
use crate::invoker::{Invoker, TempDir};
use crate::models::{Context, JudgeSolutionTaskConfig, ObjectStore, Problem, Solution};

use super::{Task, TaskProcess};

pub struct JudgeSolutionTask {
    invoker: Arc<Invoker>,
    temp_dir: Option<TempDir>,
}

impl JudgeSolutionTask {
    pub fn new(invoker: Arc<Invoker>) -> Self {
        Self {
            invoker,
            temp_dir: None,
        }
    }
}

const SOLUTION_SOURCE_PATH: &str = "solution.src";
const SOLUTION_BINARY_PATH: &str = "solution.bin";

impl JudgeSolutionTask {
    async fn prepare_temp_dir(&mut self) -> Result<(), Error> {
        self.temp_dir = Some(self.invoker.create_temp_dir()?);
        Ok(())
    }

    async fn prepare_solution(
        &mut self,
        solution: &Solution,
        logger: &Logger,
    ) -> Result<(), Error> {
        let source_path = self.temp_dir.as_ref().unwrap().join(SOLUTION_SOURCE_PATH);
        slog::debug!(
            logger,
            "Prepare solution";
            "source_path" => source_path.display()
        );
        if let Some(id) = solution.content_id {
            let file = self.invoker.file_manager().load(id).await?;
            tokio::fs::copy(file.path(), &source_path).await?;
        } else if let Some(content) = &solution.content {
            let mut file = tokio::fs::File::create(&source_path).await?;
            file.write_all(content.as_bytes()).await?;
            file.flush().await?;
        }
        let binary_path = self.temp_dir.as_ref().unwrap().join(SOLUTION_BINARY_PATH);
        slog::debug!(
            logger,
            "Compile solution";
            "binary_path" => binary_path.display()
        );
        todo!()
    }

    async fn prepare_problem(&mut self, _problem: &Problem, _logger: &Logger) -> Result<(), Error> {
        todo!()
    }
}

#[async_trait::async_trait]
impl TaskProcess for JudgeSolutionTask {
    async fn run(
        mut self: Box<Self>,
        task: Task,
        logger: slog::Logger,
        _shutdown: CancellationToken,
    ) -> Result<(), Error> {
        let config: JudgeSolutionTaskConfig = task.parse_config().await?;
        let solution = self
            .invoker
            .solution_store()
            .get(Context::new(), config.solution_id)
            .await?
            .ok_or(format!("Cannot find solution: {}", config.solution_id))?;
        let problem = self
            .invoker
            .problem_store()
            .get(Context::new(), solution.problem_id)
            .await?
            .ok_or(format!("Cannot find problem: {}", solution.problem_id))?;
        self.prepare_temp_dir().await?;
        self.prepare_solution(&solution, &logger).await?;
        self.prepare_problem(&problem, &logger).await?;
        todo!()
    }
}
