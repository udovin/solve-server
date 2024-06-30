use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::config;
use crate::core::{blocking_await, Core, Error};
use crate::managers::files::FileManager;
use crate::managers::tasks::Task;
use crate::models::{ProblemStore, SolutionStore, TaskKind, TaskStatus};

use super::safeexec;
use super::tasks::{JudgeSolutionTask, TaskProcess, UpdateProblemPackageTask};

pub struct Invoker {
    core: Arc<Core>,
    safeexec: Option<safeexec::Manager>,
    workers: u32,
    temp_dir: PathBuf,
    counter: AtomicUsize,
}

impl Invoker {
    pub fn new(core: Arc<Core>, config: &config::Invoker) -> Result<Self, Error> {
        std::fs::remove_dir_all(&config.temp_dir)?;
        std::fs::create_dir_all(&config.temp_dir)?;
        let safeexec = match &config.safeexec {
            Some(safeexec_config) => Some(safeexec::Manager::new(
                &config.temp_dir,
                &safeexec_config.cgroup,
            )?),
            None => None,
        };
        Ok(Self {
            core,
            safeexec,
            workers: config.workers,
            temp_dir: config.temp_dir.clone(),
            counter: AtomicUsize::default(),
        })
    }

    pub fn create_temp_dir(&self) -> Result<TempDir, Error> {
        let id = self.counter.fetch_add(1, Ordering::SeqCst);
        let path = self.temp_dir.join(format!("task-{id}"));
        if let Err(err) = std::fs::remove_dir_all(&path) {
            if err.kind() != std::io::ErrorKind::NotFound {
                Err(err)?
            }
        }
        std::fs::create_dir(&path)?;
        Ok(TempDir(path))
    }

    pub fn problem_store(&self) -> &ProblemStore {
        self.core.problem_store()
    }

    pub fn solution_store(&self) -> &SolutionStore {
        self.core.solution_store()
    }

    pub fn file_manager(&self) -> &FileManager {
        self.core.file_manager()
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
                            let delay = Duration::from_millis((800 + rand::random::<u16>() % 400) as u64);
                            let sleep = tokio::time::timeout(delay, shutdown.cancelled());
                            if let Ok(()) = sleep.await {
                                break;
                            }
                            continue;
                        }
                    };
                    let task_id = task.get_id().await;
                    let task_kind = task.get_kind().await;
                    let logger = logger
                        .new(slog::o!("task_id" => task_id, "kind" => task_kind.to_string()));
                    if let Err(err) = self.clone().run_task(task, logger.clone()).await {
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

    async fn run_task(self: Arc<Invoker>, task: Task, logger: slog::Logger) -> Result<(), Error> {
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
        let result = task_impl
            .run(task.clone(), logger.clone(), shutdown.clone())
            .await;
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

    async fn new_task_process(
        self: Arc<Invoker>,
        kind: TaskKind,
    ) -> Result<Box<dyn TaskProcess>, Error> {
        Ok(match kind {
            TaskKind::JudgeSolution => Box::new(JudgeSolutionTask::new(self)),
            TaskKind::UpdateProblemPackage => Box::new(UpdateProblemPackageTask::new(self)),
            TaskKind::Unknown(v) => return Err(format!("Unknown task kind: {}", v).into()),
        })
    }
}

pub struct TempDir(PathBuf);

impl TempDir {
    pub fn join<P: AsRef<Path>>(&self, path: P) -> PathBuf {
        self.0.join(path)
    }

    pub fn as_path(&self) -> &Path {
        self.0.as_path()
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        drop(blocking_await(tokio::fs::remove_dir_all(&self.0)));
    }
}
