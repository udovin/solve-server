use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;

use crate::core::{Core, Error};
use crate::db::builder::{column, Select};
use crate::models::{self, AsyncIter, Context, Event, FileStatus, ObjectStore};

#[async_trait::async_trait]
pub trait FileStorage: Send + Sync {}

#[derive(Clone)]
pub struct File {
    file: models::File,
    path: solve_cache::Object<PathBuf>,
}

impl File {
    pub async fn open(&self) -> Result<tokio::fs::File, std::io::Error> {
        tokio::fs::File::open(self.path.as_path()).await
    }
}

#[derive(Clone)]
struct FileStore(Arc<dyn FileStorage>);

#[async_trait::async_trait]
impl solve_cache::Store for FileStore {
    type Key = String;

    type Value = PathBuf;

    async fn load(&self, key: &String) -> Result<PathBuf, solve_cache::Error> {
        todo!()
    }

    async fn free(&self, key: &String, value: PathBuf) {
        todo!()
    }
}

pub trait FileHandle {
    fn name(&self) -> String;
}

pub struct FileManager {
    manager:
        solve_cache::Manager<FileStore, solve_cache::LruCache<String, PathBuf>, String, PathBuf>,
    core: Arc<Core>,
}

impl FileManager {
    pub fn new(storage: Arc<dyn FileStorage>, core: Arc<Core>) -> Self {
        let store = FileStore(storage);
        // TODO: Make dynamic capacity.
        let cache = solve_cache::LruCache::new(NonZeroUsize::new(1024).unwrap());
        Self {
            manager: solve_cache::Manager::new(store, cache),
            core,
        }
    }

    pub async fn download(&self, id: i64) -> Result<File, Error> {
        let files = self.core.files().unwrap();
        let file = files
            .find(
                Context::new(),
                Select::new().with_where(column("id").equal(id)),
            )
            .await?
            .next()
            .await
            .ok_or("file not found")??;
        let path = self.manager.load(&file.path).await?;
        Ok(File { file, path })
    }

    pub async fn upload(&self, file: &impl FileHandle) -> Result<PendingFile, Error> {
        todo!()
    }
}

pub struct PendingFile<'a> {
    model: models::File,
    files: &'a models::FileStore,
}

impl<'a> PendingFile<'a> {
    pub async fn confirm(self, ctx: models::Context<'_, '_>) -> Result<models::File, Error> {
        let mut model = self.model;
        model.status = FileStatus::Available;
        Ok(self
            .files
            .update_where(ctx, model, column("status").equal(FileStatus::Pending))
            .await?
            .into_object())
    }
}
