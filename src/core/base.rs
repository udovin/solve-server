use std::future::Future;
use std::sync::Arc;

use slog::Drain;
use solve_db::Database;

use crate::config::Config;
use crate::db::new_database;
use crate::managers::files::{new_storage, FileManager, FileStorage};
use crate::models::{FileStore, TaskStore};

pub type Error = Box<dyn std::error::Error + Send + Sync>;

pub struct Core {
    logger: slog::Logger,
    db: Arc<Database>,
    storage: Option<Arc<dyn FileStorage>>,
    files: Option<Arc<FileStore>>,
    tasks: Option<Arc<TaskStore>>,
    file_manager: Option<Arc<FileManager>>,
}

impl Core {
    pub fn new(config: &Config) -> Result<Self, Error> {
        let db = Arc::new(new_database(&config.db)?);
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::FullFormat::new(decorator)
            .use_file_location()
            .build()
            .fuse();
        let drain = slog_async::Async::new(drain)
            .chan_size(4096)
            .overflow_strategy(slog_async::OverflowStrategy::DropAndReport)
            .build()
            .fuse();
        let drain = drain.filter_level(get_log_level(&config.log_level)).fuse();
        let logger = slog::Logger::root(drain, slog::o!());
        let storage = match &config.storage {
            Some(v) => Some(new_storage(v)?),
            None => None,
        };
        Ok(Self {
            logger,
            db,
            storage,
            files: None,
            tasks: None,
            file_manager: None,
        })
    }

    pub fn logger(&self) -> &slog::Logger {
        &self.logger
    }

    pub fn db(&self) -> Arc<Database> {
        self.db.clone()
    }

    pub fn files(&self) -> Option<Arc<FileStore>> {
        self.files.clone()
    }

    pub fn tasks(&self) -> Option<Arc<TaskStore>> {
        self.tasks.clone()
    }

    pub fn file_manager(&self) -> Option<Arc<FileManager>> {
        self.file_manager.clone()
    }

    pub async fn init_server(&mut self) -> Result<(), Error> {
        self.files = Some(Arc::new(FileStore::new(self.db())));
        self.tasks = Some(Arc::new(TaskStore::new(self.db())));
        self.file_manager = Some(Arc::new(FileManager::new(
            self.storage.clone().unwrap(),
            self.files.clone().unwrap(),
        )));
        Ok(())
    }

    pub async fn init_invoker(&mut self) -> Result<(), Error> {
        self.files = Some(Arc::new(FileStore::new(self.db())));
        self.tasks = Some(Arc::new(TaskStore::new(self.db())));
        self.file_manager = Some(Arc::new(FileManager::new(
            self.storage.clone().unwrap(),
            self.files.clone().unwrap(),
        )));
        Ok(())
    }
}

/// Awaits future from a blocking function.
pub fn blocking_await<F: Future>(future: F) -> F::Output {
    tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(future))
}

fn get_log_level(level: &str) -> slog::Level {
    match level {
        "debug" => slog::Level::Debug,
        "info" => slog::Level::Info,
        "warning" => slog::Level::Warning,
        "error" => slog::Level::Error,
        "critical" => slog::Level::Critical,
        _ => slog::Level::Info,
    }
}
