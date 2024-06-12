use std::sync::Arc;

use crate::core::Error;
use serde::{Deserialize, Serialize};
use solve_db::{Database, FromRow, IntoRow, Value};
use solve_db_types::{Instant, JSON};

use super::{object_store_impl, BaseEvent, Object, PersistentStore};

#[derive(Clone, Copy, Default, Debug, PartialEq, Value, Serialize, Deserialize)]
#[repr(i8)]
#[serde(rename_all = "snake_case")]
pub enum FileStatus {
    #[default]
    Pending = 0,
    Available = 1,
    Unknown(i8),
}

impl std::fmt::Display for FileStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.serialize(f)
    }
}

#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub struct FileMeta {
    pub name: String,
    pub size: Option<u64>,
    pub md5: Option<String>,
    pub sha3_224: Option<String>,
}

impl std::fmt::Display for FileMeta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.serialize(f)
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

impl File {
    pub fn set_meta(&mut self, meta: &FileMeta) -> Result<(), Error> {
        self.meta = serde_json::to_value(meta)?.into();
        Ok(())
    }

    pub fn parse_meta(&self) -> Result<FileMeta, Error> {
        Ok(serde_json::from_value(self.meta.clone().into())?)
    }
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
