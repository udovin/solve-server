use std::sync::Arc;

use serde::{Deserialize, Serialize};
use solve_db::{Database, FromRow, IntoRow, Value};
use solve_db_types::{Instant, JSON};

use crate::core::Error;

use super::{object_store_impl, BaseEvent, Object, PersistentStore};

#[derive(Clone, Copy, Default, Debug, PartialEq, Value, Serialize, Deserialize)]
#[repr(i8)]
#[serde(rename_all = "snake_case")]
pub enum SolutionKind {
    #[default]
    ContestSolution = 1,
    Unknown(u8),
}

impl std::fmt::Display for SolutionKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.serialize(f)
    }
}

#[derive(Clone, Copy, Default, Debug, PartialEq, Value, Serialize, Deserialize)]
#[repr(i8)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    #[default]
    Accepted = 1,
    Rejected = 2,
    CompilationError = 3,
    TimeLimitExceeded = 4,
    MemoryLimitExceeded = 5,
    RuntimeError = 6,
    WrongAnswer = 7,
    PresentationError = 8,
    PartiallyAccepted = 9,
    Failed = 10,
    Unknown(i8),
}

impl std::fmt::Display for Verdict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.serialize(f)
    }
}

#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub struct JudgeReport {
    pub verdict: Verdict,
}

#[derive(Clone, Default, Debug, FromRow, IntoRow)]
pub struct Solution {
    pub id: i64,
    pub kind: SolutionKind,
    pub problem_id: i64,
    pub compiler_id: i64,
    pub author_id: i64,
    pub report: JSON,
    pub create_time: Instant,
    pub content: Option<String>,
    pub content_id: Option<i64>,
}

impl Solution {
    pub fn set_report(&mut self, report: Option<JudgeReport>) -> Result<(), Error> {
        self.report = serde_json::to_value(report)?.into();
        Ok(())
    }

    pub fn parse_report(&self) -> Result<Option<JudgeReport>, Error> {
        Ok(serde_json::from_value(self.report.clone().into())?)
    }
}

impl Object for Solution {
    type Id = i64;

    fn id(&self) -> Self::Id {
        self.id
    }

    fn set_id(&mut self, id: Self::Id) {
        self.id = id;
    }

    fn is_valid(&self) -> bool {
        !matches!(self.kind, SolutionKind::Unknown(_))
    }
}

pub type SolutionEvent = BaseEvent<Solution>;

pub struct SolutionStore(PersistentStore<Solution>);

impl SolutionStore {
    pub fn new(db: Arc<Database>) -> Self {
        Self(PersistentStore::new(
            db,
            "solve_solution",
            "solve_solution_event",
        ))
    }
}

object_store_impl!(SolutionStore, Solution, SolutionEvent);
