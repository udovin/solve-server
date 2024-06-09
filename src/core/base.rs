use std::future::Future;
use std::sync::Arc;

use slog::Drain;
use solve_db::Database;

use crate::config::Config;
use crate::db::new_database;
use crate::managers::files::{new_storage, FileManager};
use crate::managers::tasks::TaskManager;
use crate::models::{FileStore, ProblemStore, SolutionStore, TaskStore};

pub type Error = Box<dyn std::error::Error + Send + Sync>;

pub struct Core {
    logger: slog::Logger,
    db: Arc<Database>,
    // Stores.
    task_store: Arc<TaskStore>,
    file_store: Arc<FileStore>,
    problem_store: Arc<ProblemStore>,
    solution_store: Arc<SolutionStore>,
    // Managers.
    task_manager: Option<Arc<TaskManager>>,
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
        let task_store = Arc::new(TaskStore::new(db.clone()));
        let file_store = Arc::new(FileStore::new(db.clone()));
        let problem_store = Arc::new(ProblemStore::new(db.clone()));
        let solution_store = Arc::new(SolutionStore::new(db.clone()));
        Ok(Self {
            logger,
            db,
            task_store,
            file_store,
            problem_store,
            solution_store,
            task_manager: None,
            file_manager: None,
        })
    }

    pub fn logger(&self) -> &slog::Logger {
        &self.logger
    }

    pub fn db(&self) -> &Database {
        &self.db
    }

    pub fn task_store(&self) -> &TaskStore {
        &self.task_store
    }

    pub fn file_store(&self) -> &FileStore {
        &self.file_store
    }

    pub fn problem_store(&self) -> &ProblemStore {
        &self.problem_store
    }

    pub fn solution_store(&self) -> &SolutionStore {
        &self.solution_store
    }

    pub fn task_manager(&self) -> &TaskManager {
        self.task_manager
            .as_ref()
            .expect("Task manager is not initialized")
    }

    pub fn file_manager(&self) -> &FileManager {
        self.file_manager
            .as_ref()
            .expect("File manager is not initialized")
    }

    pub async fn init_server(&mut self, _config: &Config) -> Result<(), Error> {
        Ok(())
    }

    pub async fn init_invoker(&mut self, config: &Config) -> Result<(), Error> {
        self.init_task_manager()?;
        self.init_file_manager(config)?;
        Ok(())
    }

    fn init_task_manager(&mut self) -> Result<(), Error> {
        self.task_manager = Some(Arc::new(TaskManager::new(self.task_store.clone())));
        Ok(())
    }

    fn init_file_manager(&mut self, config: &Config) -> Result<(), Error> {
        let config = config
            .storage
            .as_ref()
            .expect("Storage config is not provided");
        let file_manager = Arc::new(FileManager::new(
            new_storage(config)?,
            self.file_store.clone(),
        ));
        self.file_manager = Some(file_manager);
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
