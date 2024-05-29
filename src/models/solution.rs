use std::sync::Arc;

use solve_db::{Database, FromRow, IntoRow, Value};
use solve_db_types::{Instant, JSON};

use super::{object_store_impl, BaseEvent, Object, PersistentStore};

#[derive(Clone, Copy, Default, Debug, PartialEq, Value)]
#[repr(i8)]
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
        match self {
            Verdict::Accepted => f.write_str("accepted"),
            Verdict::Rejected => f.write_str("rejected"),
            Verdict::CompilationError => f.write_str("compilation_error"),
            Verdict::TimeLimitExceeded => f.write_str("time_limit_exceeded"),
            Verdict::MemoryLimitExceeded => f.write_str("memory_limit_exceeded"),
            Verdict::RuntimeError => f.write_str("runtime_error"),
            Verdict::WrongAnswer => f.write_str("wrong_answer"),
            Verdict::PresentationError => f.write_str("presentation_error"),
            Verdict::PartiallyAccepted => f.write_str("partially_accepted"),
            Verdict::Failed => f.write_str("failed"),
            Verdict::Unknown(_) => f.write_str("unknown"),
        }
    }
}

#[derive(Clone, Default, Debug, FromRow, IntoRow)]
pub struct Solution {
    pub id: i64,
    pub problem_id: i64,
    pub compiler_id: i64,
    pub author_id: i64,
    pub report: JSON,
    pub create_time: Instant,
    pub content: Option<String>,
    pub content_id: Option<i64>,
}

impl Object for Solution {
    type Id = i64;

    fn id(&self) -> Self::Id {
        self.id
    }

    fn set_id(&mut self, id: Self::Id) {
        self.id = id;
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
