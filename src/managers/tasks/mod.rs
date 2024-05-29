use std::sync::Arc;
use std::time::Duration;

use serde::de::DeserializeOwned;
use solve_db_types::{Instant, JSON};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::core::Error;
use crate::db::builder::column;
use crate::models::{self, Context, Event, ObjectStore, TaskKind, TaskStatus};

pub struct TaskManager {
    tasks: Arc<models::TaskStore>,
}

impl TaskManager {
    pub fn new(tasks: Arc<models::TaskStore>) -> Self {
        Self { tasks }
    }

    pub async fn take_task(&self) -> Result<Option<Task>, Error> {
        let task = match self
            .tasks
            .take_task(Context::new(), Duration::from_secs(30))
            .await?
        {
            Some(v) => v,
            None => return Ok(None),
        };
        assert_eq!(task.status, TaskStatus::Running);
        let inner = Arc::new(TaskInner {
            task: Mutex::new(task.clone()),
            stored_task: Mutex::new(task),
            tasks: self.tasks.clone(),
        });
        Ok(Some(Task { inner }))
    }
}

struct TaskInner {
    task: Mutex<models::Task>,
    stored_task: Mutex<models::Task>,
    tasks: Arc<models::TaskStore>,
}

#[derive(Clone)]
pub struct Task {
    inner: Arc<TaskInner>,
}

impl Task {
    pub async fn get_id(&self) -> i64 {
        let task = self.inner.task.lock().await;
        task.id
    }

    pub async fn get_kind(&self) -> TaskKind {
        let task = self.inner.task.lock().await;
        task.kind
    }

    pub async fn get_status(&self) -> TaskStatus {
        let task = self.inner.task.lock().await;
        task.status
    }

    pub async fn parse_config<T: DeserializeOwned>(&self) -> Result<T, Error> {
        let task = self.inner.task.lock().await;
        task.parse_config()
    }

    pub async fn set_status(&self, status: TaskStatus) -> Result<(), Error> {
        let mut task = self.inner.task.lock().await;
        let new_task = models::Task {
            status,
            ..task.clone()
        };
        *task = self.update(new_task, Instant::now()).await?;
        Ok(())
    }

    pub async fn get_state(&self) -> JSON {
        let task = self.inner.task.lock().await;
        task.state.clone()
    }

    pub async fn set_state(&self, state: JSON) -> Result<(), Error> {
        let mut task = self.inner.task.lock().await;
        let new_task = models::Task {
            state,
            ..task.clone()
        };
        *task = self.update(new_task, Instant::now()).await?;
        Ok(())
    }

    pub async fn set_deferred_state(&self, state: JSON) {
        let mut task = self.inner.task.lock().await;
        task.state = state;
    }

    pub async fn ping(&self, duration: Duration) -> Result<(), Error> {
        let mut task = self.inner.task.lock().await;
        let now = Instant::now();
        let new_task = models::Task {
            expire_time: Some(now + duration),
            ..task.clone()
        };
        *task = self.update(new_task, now).await?;
        Ok(())
    }

    pub fn spawn_pinger(
        &self,
        shutdown: CancellationToken,
        logger: slog::Logger,
    ) -> JoinHandle<()> {
        let clone = Task {
            inner: self.inner.clone(),
        };
        tokio::spawn(clone.run_pinger(shutdown, logger))
    }

    async fn run_pinger(self, shutdown: CancellationToken, logger: slog::Logger) {
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
        let task = self.inner.task.lock().await;
        Self::is_expired(&task, Instant::now() + delta)
    }

    async fn update(&self, new_task: models::Task, now: Instant) -> Result<models::Task, Error> {
        let mut task = self.inner.stored_task.lock().await;
        if Self::is_expired(&task, now) {
            return Err("task expired".into());
        }
        let event = self
            .inner
            .tasks
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

    fn is_expired(task: &models::Task, now: Instant) -> bool {
        match task.expire_time {
            Some(v) => v < now,
            None => true,
        }
    }
}
