pub use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::core::Error;
pub use crate::managers::tasks::Task;

#[async_trait::async_trait]
pub trait TaskProcess: Send + Sync {
    async fn run(
        self: Box<Self>,
        task: Task,
        logger: slog::Logger,
        shutdown: CancellationToken,
    ) -> Result<(), Error>;
}
