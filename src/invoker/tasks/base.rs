use std::sync::Arc;
use std::time::Duration;

use solve_db::{IntoRow, Row};
use solve_db_types::{Instant, JSON};
pub use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::core::{Core, Error};
use crate::db::builder::column;
use crate::models::{Context, Event, ObjectStore, Task, TaskKind, TaskStatus};

pub struct TaskGuard {
    task: Mutex<Task>,
    stored_task: Mutex<Task>,
    core: Arc<Core>,
}

impl TaskGuard {
    pub fn new(task: Task, core: Arc<Core>) -> Arc<Self> {
        Arc::new(Self {
            task: Mutex::new(task.clone()),
            stored_task: Mutex::new(task),
            core,
        })
    }

    pub async fn get_kind(&self) -> TaskKind {
        let task = self.task.lock().await;
        task.kind
    }

    pub async fn get_config(&self) -> JSON {
        let task = self.task.lock().await;
        task.config.clone()
    }

    pub async fn get_status(&self) -> TaskStatus {
        let task = self.task.lock().await;
        task.status
    }

    pub async fn set_status(&self, status: TaskStatus) -> Result<(), Error> {
        let mut task = self.task.lock().await;
        let new_task = Task {
            status,
            ..task.clone()
        };
        *task = self.update(new_task, Instant::now()).await?;
        Ok(())
    }

    pub async fn get_state(&self) -> JSON {
        let task = self.task.lock().await;
        task.state.clone()
    }

    pub async fn set_state(&self, state: JSON) -> Result<(), Error> {
        let mut task = self.task.lock().await;
        let new_task = Task {
            state,
            ..task.clone()
        };
        *task = self.update(new_task, Instant::now()).await?;
        Ok(())
    }

    pub async fn set_deferred_state(&self, state: JSON) {
        let mut task = self.task.lock().await;
        task.state = state;
    }

    pub async fn ping(&self, duration: Duration) -> Result<(), Error> {
        let mut task = self.task.lock().await;
        let now = Instant::now();
        let new_task = Task {
            expire_time: Some(now + duration),
            ..task.clone()
        };
        *task = self.update(new_task, now).await?;
        Ok(())
    }

    pub async fn run_pinger(self: Arc<Self>, shutdown: CancellationToken, logger: slog::Logger) {
        loop {
            let sleep = tokio::time::timeout(Duration::from_secs(1), shutdown.cancelled());
            if let Ok(()) = sleep.await {
                return;
            }
            if self.is_expires_after(Duration::ZERO).await {
                shutdown.cancel();
                return;
            }
            if !self.is_expires_after(Duration::from_secs(15)).await {
                continue;
            }
            if let Err(err) = self.ping(Duration::from_secs(30)).await {
                slog::warn!(logger, "Cannot ping task"; "error" => err.to_string());
            }
            slog::debug!(logger, "Pinged task");
        }
    }

    async fn is_expires_after(&self, delta: Duration) -> bool {
        let task = self.task.lock().await;
        Self::is_expired(&task, Instant::now() + delta)
    }

    async fn update(&self, new_task: Task, now: Instant) -> Result<Task, Error> {
        let mut task = self.stored_task.lock().await;
        if Self::is_expired(&task, now) {
            return Err("task expired".into());
        }
        let store = self.core.tasks().expect("task store should be initialized");
        let event = store
            .update_where(
                Context::new(),
                new_task,
                column("kind")
                    .equal(task.kind)
                    .and(column("status").equal(task.status))
                    .and(column("expire_time").equal(task.expire_time)),
            )
            .await?;
        *task = event.into_object();
        Ok(task.clone())
    }

    fn is_expired(task: &Task, now: Instant) -> bool {
        match task.expire_time {
            Some(v) => v < now,
            None => true,
        }
    }
}

#[async_trait::async_trait]
pub trait TaskProcess: Send + Sync {
    async fn run(&self, task: Arc<TaskGuard>, shutdown: CancellationToken) -> Result<(), Error>;
}
