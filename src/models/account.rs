use std::sync::Arc;

use serde::{Deserialize, Serialize};
use solve_db::{Database, FromRow, IntoRow, Value};

use super::{object_store_impl, BaseEvent, Object, PersistentStore};

#[derive(Clone, Copy, Default, Debug, PartialEq, Value, Serialize, Deserialize)]
#[repr(i8)]
#[serde(rename_all = "snake_case")]
pub enum AccountKind {
    #[default]
    User = 1,
    ScopeUser = 2,
    Scope = 3,
    Group = 4,
    Unknown(u8),
}

impl std::fmt::Display for AccountKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.serialize(f)
    }
}

#[derive(Clone, Default, Debug, FromRow, IntoRow)]
pub struct Account {
    pub id: i64,
    pub kind: AccountKind,
}

impl Object for Account {
    type Id = i64;

    fn id(&self) -> Self::Id {
        self.id
    }

    fn set_id(&mut self, id: Self::Id) {
        self.id = id;
    }

    fn is_valid(&self) -> bool {
        !matches!(self.kind, AccountKind::Unknown(_))
    }
}

pub type AccountEvent = BaseEvent<Account>;

pub struct AccountStore(PersistentStore<Account>);

impl AccountStore {
    pub fn new(db: Arc<Database>) -> Self {
        Self(PersistentStore::new(
            db,
            "solve_account",
            "solve_account_event",
        ))
    }
}

object_store_impl!(AccountStore, Account, AccountEvent);
