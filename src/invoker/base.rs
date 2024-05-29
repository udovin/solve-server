use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::config;
use crate::core::{Core, Error};
use crate::managers::tasks::Task;
use crate::models::{TaskKind, TaskStatus};

use super::tasks::{JudgeSolutionTask, TaskProcess, UpdateProblemPackageTask};

pub struct Invoker {
    core: Arc<Core>,
    workers: u32,
}

impl Invoker {
    pub fn new(core: Arc<Core>, config: &config::Invoker) -> Self {
        Self {
            core,
            workers: config.workers,
        }
    }

    pub async fn run(self, shutdown: CancellationToken) -> Result<(), Error> {
        let this = Arc::new(self);
        let mut join_set = tokio::task::JoinSet::new();
        for i in 0..this.workers {
            let this = this.clone();
            let logger = this.core.logger().new(slog::o!("worker" => i + 1));
            join_set.spawn(this.run_worker(shutdown.clone(), logger));
        }
        while let Some(res) = join_set.join_next().await {
            res??;
        }
        Ok(())
    }

    async fn run_worker(
        self: Arc<Self>,
        shutdown: CancellationToken,
        logger: slog::Logger,
    ) -> Result<(), Error> {
        slog::info!(logger, "Running invoker");
        let task_manager = self.core.task_manager();
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    break;
                }
                task = task_manager.take_task() => {
                    let task = match task {
                        Ok(Some(task)) => task,
                        Ok(None) => {
                            slog::debug!(logger, "Task queue is empty");
                            let delay = Duration::from_millis((800 + rand::random::<u16>() % 400) as u64);
                            let sleep = tokio::time::timeout(delay, shutdown.cancelled());
                            if let Ok(()) = sleep.await {
                                break;
                            }
                            continue;
                        }
                        Err(err) => {
                            slog::warn!(logger, "Cannot get task"; "error" => err.to_string());
                            continue;
                        }
                    };
                    let task_id = task.get_id().await;
                    let task_kind = task.get_kind().await;
                    let logger = logger
                        .new(slog::o!("task_id" => task_id, "kind" => task_kind.to_string()));
                    if let Err(err) = self.run_task(task, logger.clone()).await {
                        slog::error!(logger, "Task failed"; "error" => err.to_string());
                    } else {
                        slog::info!(logger, "Task succeeded");
                    }
                }
            }
        }
        slog::info!(logger, "Invoker completed");
        Ok(())
    }

    async fn run_task(&self, task: Task, logger: slog::Logger) -> Result<(), Error> {
        slog::info!(logger, "Executing task");
        let task_kind = task.get_kind().await;
        let task_impl = match self.new_task_process(task_kind).await {
            Ok(v) => v,
            Err(err) => {
                if let Err(err) = task.set_status(TaskStatus::Failed).await {
                    slog::error!(logger, "Unable to set failed task status"; "error" => err.to_string());
                }
                return Err(err);
            }
        };
        let shutdown = CancellationToken::new();
        let pinger_task = task.spawn_pinger(shutdown.clone(), logger.clone());
        let result = task_impl.run(task.clone(), shutdown.clone()).await;
        shutdown.cancel();
        pinger_task.await.unwrap();
        match result {
            Ok(()) => {
                if let Err(err) = task.set_status(TaskStatus::Succeeded).await {
                    slog::error!(logger, "Unable to set succeeded task status"; "error" => err.to_string());
                    return Err(err);
                }
                Ok(())
            }
            Err(err) => {
                if let Err(err) = task.set_status(TaskStatus::Failed).await {
                    slog::error!(logger, "Unable to set failed task status"; "error" => err.to_string());
                }
                Err(err)
            }
        }
    }

    async fn new_task_process(&self, kind: TaskKind) -> Result<Box<dyn TaskProcess>, Error> {
        Ok(match kind {
            TaskKind::JudgeSolution => Box::new(JudgeSolutionTask::new(self)),
            TaskKind::UpdateProblemPackage => Box::new(UpdateProblemPackageTask::new(self)),
            TaskKind::Unknown(v) => return Err(format!("unknown task kind: {}", v).into()),
        })
    }
}
