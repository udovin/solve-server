mod local_storage;

use std::io::Read;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use local_storage::LocalStorage;
use solve_db_types::Instant;

use crate::config::StorageConfig;
use crate::core::Error;
use crate::db::builder::{column, Select};
use crate::models::{self, AsyncIter, Context, Event, FileMeta, FileStatus, ObjectStore};

pub struct UploadResult {
    pub size: u64,
    pub md5: String,
    pub sha3_224: String,
}

#[async_trait::async_trait]
pub trait FileStorage: Send + Sync {
    async fn load(&self, key: &str) -> Result<PathBuf, Error>;

    async fn free(&self, key: &str, value: PathBuf);

    async fn generate_key(&self) -> Result<String, Error>;

    async fn upload(&self, key: &str, file: Pin<Box<dyn FileInfo>>) -> Result<UploadResult, Error>;

    async fn delete(&self, key: &str) -> Result<(), Error>;
}

#[derive(Clone)]
pub struct File {
    file: models::File,
    path: solve_cache::Object<PathBuf>,
}

impl File {
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn parse_meta(&self) -> Result<FileMeta, Error> {
        self.file.parse_meta()
    }

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

#[async_trait::async_trait]
pub trait FileInfo: Send + Sync {
    fn name(&self) -> Option<String>;

    fn size(&self) -> Option<u64>;

    fn path(&self) -> Option<PathBuf>;

    fn into_reader(self: Pin<Box<Self>>) -> Box<dyn Read + Send + Sync>;
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

    pub async fn load(&self, id: i64) -> Result<File, Error> {
        let file = self
            .files
            .find(
                Context::new(),
                Select::new().with_where(column("id").equal(id)),
            )
            .await?
            .next()
            .await
            .ok_or("File not found")??;
        if file.status != models::FileStatus::Available {
            Err(format!("File has invalid status: {}", file.status))?;
        }
        let path = self.manager.load(&file.path).await?;
        Ok(File { file, path })
    }

    pub async fn upload<T: FileInfo + 'static>(&self, file: T) -> Result<PendingFile, Error> {
        let key = self.storage.generate_key().await?;
        let meta = models::FileMeta {
            name: file.name().unwrap_or_default(),
            size: file.size(),
            ..Default::default()
        };
        let mut model = models::File {
            status: models::FileStatus::Pending,
            expire_time: Some(Instant::now() + Duration::from_secs(60)),
            path: key.clone(),
            ..Default::default()
        };
        model.set_meta(&meta)?;
        let event = self.files.create(Context::new(), model).await?;
        let result = self.storage.upload(&key, Box::pin(file)).await?;
        let new_meta = models::FileMeta {
            size: Some(result.size),
            md5: Some(result.md5),
            sha3_224: Some(result.sha3_224),
            ..meta
        };
        let mut model = event.into_object();
        model.set_meta(&new_meta)?;
        Ok(PendingFile {
            model,
            files: self.files.clone(),
        })
    }

    pub async fn delete(&self, id: i64) -> Result<(), Error> {
        let model = match self.files.get(Context::new(), id).await? {
            Some(v) => v,
            None => Err("File does not exist")?,
        };
        let mut expire_time = Instant::now() + Duration::from_secs(60);
        if matches!(model.status, models::FileStatus::Pending) {
            if let Some(time) = model.expire_time {
                if Instant::now() < time {
                    Err("Cannot delete not uploaded file")?;
                }
                expire_time = time;
            }
        }
        let key = model.path.clone();
        let status = model.status.clone();
        let model = models::File {
            status: models::FileStatus::Pending,
            expire_time: Some(expire_time),
            ..model
        };
        self.files
            .update_where(Context::new(), model, column("status").equal(status))
            .await?;
        self.storage.delete(&key).await?;
        self.files
            .delete_where(
                Context::new(),
                id,
                column("status").equal(models::FileStatus::Pending),
            )
            .await?;
        Ok(())
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
        model.expire_time = None;
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
        StorageConfig::S3(_config) => unimplemented!(),
    }
}
