use std::sync::Arc;

use crate::core::Error;
use crate::db::{Database, FromRow, IntoRow, Row, Value};

use super::types::Instant;
use super::{object_store_impl, BaseEvent, Object, PersistentStore, JSON};

#[derive(Clone, Copy, Default, Debug, PartialEq, Value)]
#[repr(i8)]
pub enum FileStatus {
    #[default]
    Pending = 0,
    Available = 1,
    Unknown(i8),
}

impl ToString for FileStatus {  
    fn to_string(&self) -> String {
        match self {
            FileStatus::Pending => "pending",
            FileStatus::Available => "available",
            FileStatus::Unknown(_) => "unknown",
        }
        .into()
    }
}

#[derive(Clone, Default, Debug, FromRow, IntoRow)]
pub struct File {
    pub id: i64,
    pub status: FileStatus,
    pub expire_time: Option<Instant>,
    pub path: String,
    pub meta: JSON,
}

impl Object for File {
    type Id = i64;

    fn id(&self) -> Self::Id {
        self.id
    }

    fn set_id(&mut self, id: Self::Id) {
        self.id = id;
    }

    fn is_valid(&self) -> bool {
        !matches!(self.status, FileStatus::Unknown(_))
    }
}

pub type FileEvent = BaseEvent<File>;

pub struct FileStore(PersistentStore<File>);

impl FileStore {
    pub fn new(db: Arc<Database>) -> Self {
        Self(PersistentStore::new(db, "solve_file", "solve_file_event"))
    }
}

object_store_impl!(FileStore, File, FileEvent);
