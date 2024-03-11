use std::sync::Arc;

use crate::core::Error;
use crate::db::{Database, FromRow, IntoRow, Row, Value};

use super::types::Instant;
use super::{object_store_impl, BaseEvent, Object, PersistentStore, JSON};

#[derive(Clone, Copy, Default, Debug, PartialEq)]
#[repr(i64)]
pub enum FileStatus {
    #[default]
    Pending = 0,
    Available = 1,
    Unknown(i64),
}

impl TryFrom<Value> for FileStatus {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self, Error> {
        Ok(match value.try_into()? {
            0 => Self::Pending,
            1 => Self::Available,
            v => Self::Unknown(v),
        })
    }
}

impl From<FileStatus> for Value {
    fn from(value: FileStatus) -> Self {
        match value {
            FileStatus::Pending => 0,
            FileStatus::Available => 1,
            FileStatus::Unknown(v) => v,
        }
        .into()
    }
}

#[derive(Clone, Default, FromRow, IntoRow)]
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
