use std::sync::Arc;

use std::time::Duration;

use crate::core::Error;
use crate::db::builder::{column, Select};
use crate::db::{Database, FromRow, IntoRow, Row, Value};
use crate::models::{write_tx_options, Context, ObjectStore};

use super::types::Instant;
use super::{now, object_store_impl, BaseEvent, Event, Object, PersistentStore, JSON};

#[derive(Clone, Copy, Default, Debug, PartialEq, Value)]
#[repr(i8)]
pub enum TaskKind {
    #[default]
    JudgeSolution = 1,
    UpdateProblemPackage = 2,
    Unknown(i8),
}

impl ToString for TaskKind {  
    fn to_string(&self) -> String {
        match self {
            TaskKind::JudgeSolution => "judge_solution",
            TaskKind::UpdateProblemPackage => "update_problem_package",
            TaskKind::Unknown(_) => "unknown",
        }
        .into()
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
        let task_row = {
            let mut rows = self
                .find(
                    Context::new().with_tx(&mut tx),
                    Select::new()
                        .with_where(column("status").equal(TaskStatus::Queued))
                        .with_limit(1),
                )
                .await?
                .into_raw();
            loop {
                match rows.next().await {
                    Some(Ok(v)) => 
                    match Task::from_row(&v)?.kind {
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
            expire_time: Some(now() + duration),
            ..Task::from_row(&task_row)?
        };
        let event = self
            .update_from(ctx.with_tx(&mut tx), new_task, task_row)
            .await?;
        tx.commit().await?;
        Ok(Some(event.into_object()))
    }
}

object_store_impl!(TaskStore, Task, TaskEvent);
