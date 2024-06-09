use std::sync::Arc;

use solve_db::{Database, FromRow, IntoRow};

use super::{object_store_impl, BaseEvent, Object, PersistentStore};

#[derive(Clone, Default, Debug, FromRow, IntoRow)]
pub struct Problem {
    pub id: i64,
}

impl Object for Problem {
    type Id = i64;

    fn id(&self) -> Self::Id {
        self.id
    }

    fn set_id(&mut self, id: Self::Id) {
        self.id = id;
    }
}

pub type ProblemEvent = BaseEvent<Problem>;

pub struct ProblemStore(PersistentStore<Problem>);

impl ProblemStore {
    pub fn new(db: Arc<Database>) -> Self {
        Self(PersistentStore::new(
            db,
            "solve_problem",
            "solve_problem_event",
        ))
    }
}

object_store_impl!(ProblemStore, Problem, ProblemEvent);
