mod local_storage;

use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use local_storage::LocalStorage;

use crate::config::StorageConfig;
use crate::core::{Core, Error};
use crate::db::builder::{column, Select};
use crate::models::{self, AsyncIter, Context, Event, FileStatus, ObjectStore};

#[async_trait::async_trait]
pub trait FileStorage: Send + Sync {
    async fn load(&self, key: &str) -> Result<PathBuf, Error>;

    async fn free(&self, key: &str, value: PathBuf);

    async fn generate_key(&self) -> Result<String, Error>;

    async fn upload(&self, key: &str, file: Box<dyn FileHandle>) -> Result<(), Error>;
}

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
struct FileStore {
    storage: Arc<dyn FileStorage>,
}

#[async_trait::async_trait]
impl solve_cache::Store for FileStore {
    type Key = String;

    type Value = PathBuf;

    async fn load(&self, key: &String) -> Result<PathBuf, solve_cache::Error> {
        self.storage.load(key).await
    }

    async fn free(&self, key: &String, value: PathBuf) {
        self.storage.free(key, value).await
    }
}

pub trait FileHandle: std::io::Read + std::io::Seek + Send + Sync {
    fn name(&self) -> String;

    fn size(&self) -> Option<usize>;
}

type Cache = solve_cache::LruCache<String, PathBuf>;

pub struct FileManager {
    manager: solve_cache::Manager<FileStore, Cache, String, PathBuf>,
    storage: Arc<dyn FileStorage>,
    files: Arc<models::FileStore>,
}

impl FileManager {
    pub fn new(storage: Arc<dyn FileStorage>, files: Arc<models::FileStore>) -> Self {
        let store = FileStore {
            storage: storage.clone(),
        };
        // TODO: Make dynamic capacity.
        let cache = solve_cache::LruCache::new(NonZeroUsize::new(1024).unwrap());
        Self {
            manager: solve_cache::Manager::new(store, cache),
            storage,
            files,
        }
    }

    pub async fn download(&self, id: i64) -> Result<File, Error> {
        let file = self
            .files
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

    pub async fn upload(&self, file: Box<dyn FileHandle>) -> Result<PendingFile, Error> {
        let key = self.storage.generate_key().await?;
        let meta = models::FileMeta {
            name: file.name(),
            size: file.size(),
        };
        let mut model = models::File {
            status: models::FileStatus::Pending,
            expire_time: Some(solve_db_types::Instant::now() + Duration::from_secs(60)),
            path: key.clone(),
            ..Default::default()
        };
        model.set_meta(&meta)?;
        let event = self.files.create(Context::new(), model).await?;
        self.storage.upload(&key, file).await?;
        Ok(PendingFile {
            model: event.into_object(),
            files: self.files.clone(),
        })
    }

    pub async fn delete(&self, id: i64) -> Result<(), Error> {
        todo!()
    }
}

pub struct PendingFile {
    model: models::File,
    files: Arc<models::FileStore>,
}

impl PendingFile {
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

pub fn new_storage(config: &StorageConfig) -> Result<Arc<dyn FileStorage>, Error> {
    match config {
        StorageConfig::Local(config) => {
            let storage = LocalStorage::new(&config.files_dir)?;
            Ok(Arc::new(storage))
        }
        StorageConfig::S3(config) => unimplemented!(),
    }
}
