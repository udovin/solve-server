use std::future::Future;
use std::sync::Arc;

use slog::Drain;
use solve_db::Database;

use crate::config::Config;
use crate::db::new_database;
use crate::models::{FileStore, TaskStore};

pub type Error = Box<dyn std::error::Error + Send + Sync>;

pub struct Core {
    db: Arc<Database>,
    logger: slog::Logger,
    files: Option<FileStore>,
    tasks: Option<TaskStore>,
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
        Ok(Self {
            db,
            logger,
            files: None,
            tasks: None,
        })
    }

    pub fn db(&self) -> Arc<Database> {
        self.db.clone()
    }

    pub fn files(&self) -> Option<&FileStore> {
        self.files.as_ref()
    }

    pub fn tasks(&self) -> Option<&TaskStore> {
        self.tasks.as_ref()
    }

    pub fn logger(&self) -> &slog::Logger {
        &self.logger
    }

    pub async fn init_server(&mut self) -> Result<(), Error> {
        self.files = Some(FileStore::new(self.db()));
        self.tasks = Some(TaskStore::new(self.db()));
        Ok(())
    }

    pub async fn init_invoker(&mut self) -> Result<(), Error> {
        self.files = Some(FileStore::new(self.db()));
        self.tasks = Some(TaskStore::new(self.db()));
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
