use std::sync::Arc;

use std::time::Duration;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use solve_db::{Database, FromRow, IntoRow, Value};
use solve_db_types::{Instant, JSON};

use crate::core::Error;
use crate::db::builder::{column, Select};
use crate::models::{write_tx_options, Context, ObjectStore};

use super::{object_store_impl, AsyncIter, BaseEvent, Event, Object, PersistentStore};

#[derive(Clone, Copy, Default, Debug, PartialEq, Value)]
#[repr(i8)]
pub enum TaskKind {
    #[default]
    JudgeSolution = 1,
    UpdateProblemPackage = 2,
    Unknown(i8),
}

impl std::fmt::Display for TaskKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskKind::JudgeSolution => f.write_str("judge_solution"),
            TaskKind::UpdateProblemPackage => f.write_str("update_problem_package"),
            TaskKind::Unknown(_) => f.write_str("unknown"),
        }
    }
}

#[derive(Clone, Copy, Default, Debug, PartialEq, Value)]
#[repr(i64)]
pub enum TaskStatus {
    #[default]
    Queued = 0,
    Running = 1,
    Succeeded = 2,
    Failed = 3,
    Unknown(i64),
}

#[derive(Clone, Default, Debug, FromRow, IntoRow)]
pub struct Task {
    pub id: i64,
    pub kind: TaskKind,
    pub config: JSON,
    pub status: TaskStatus,
    pub state: JSON,
    pub expire_time: Option<Instant>,
}

impl Task {
    pub fn set_config<T: Serialize>(&mut self, config: T) -> Result<(), Error> {
        self.config = serde_json::to_value(config)?.into();
        Ok(())
    }

    pub fn parse_config<T: DeserializeOwned>(&self) -> Result<T, Error> {
        Ok(serde_json::from_value(self.config.clone().into())?)
    }

    pub fn set_state<T: Serialize>(&mut self, state: T) -> Result<(), Error> {
        self.state = serde_json::to_value(state)?.into();
        Ok(())
    }

    pub fn parse_state<T: DeserializeOwned>(&self) -> Result<T, Error> {
        Ok(serde_json::from_value(self.state.clone().into())?)
    }
}

impl Object for Task {
    type Id = i64;

    fn id(&self) -> Self::Id {
        self.id
    }

    fn set_id(&mut self, id: Self::Id) {
        self.id = id;
    }

    fn is_valid(&self) -> bool {
        !matches!(self.kind, TaskKind::Unknown(_)) && !matches!(self.status, TaskStatus::Unknown(_))
    }
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct JudgeSolutionTaskConfig {
    solution_id: i64,
    #[serde(default, skip_serializing_if = "<&bool as std::ops::Not>::not")]
    enable_points: bool,
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct UpdateProblemPackageTaskConfig {
    problem_id: i64,
    file_id: i64,
    #[serde(default, skip_serializing_if = "<&bool as std::ops::Not>::not")]
    compile: bool,
}

pub type TaskEvent = BaseEvent<Task>;

pub struct TaskStore(PersistentStore<Task>);

impl TaskStore {
    pub fn new(db: Arc<Database>) -> Self {
        Self(PersistentStore::new(db, "solve_task", "solve_task_event"))
    }

    pub async fn take_task(
        &self,
        ctx: Context<'_, '_>,
        duration: Duration,
    ) -> Result<Option<Task>, Error> {
        if ctx.tx.is_some() {
            return Err("cannot take task in transaction".into());
        }
        let mut tx = self.0.db().transaction(write_tx_options()).await?;
        let task = {
            let mut rows = self
                .find(
                    Context::new().with_tx(&mut tx),
                    Select::new()
                        .with_where(column("status").equal(TaskStatus::Queued))
                        .with_limit(5),
                )
                .await?;
            loop {
                match rows.next().await {
                    Some(Ok(v)) => match v.kind {
                        TaskKind::Unknown(_) => continue,
                        _ => break v,
                    },
                    Some(Err(v)) => return Err(v),
                    None => return Ok(None),
                }
            }
        };
        let new_task = Task {
            status: TaskStatus::Running,
            expire_time: Some(Instant::now() + duration),
            ..task
        };
        let event = self
            .update_where(
                ctx.with_tx(&mut tx),
                new_task,
                column("kind")
                    .equal(task.kind)
                    .and(column("status").equal(task.status))
                    .and(column("expire_time").equal(task.expire_time)),
            )
            .await?;
        tx.commit().await?;
        Ok(Some(event.into_object()))
    }
}

object_store_impl!(TaskStore, Task, TaskEvent);
