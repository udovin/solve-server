use std::sync::Arc;

use clap::Parser;
use solve::config::{parse_file, Config};
use solve::core::{Core, Error};
use solve::invoker::Invoker;
use tokio_util::sync::CancellationToken;

#[derive(clap::Args)]
struct ServerArgs {}

#[derive(clap::Args)]
struct InvokerArgs {}

#[derive(clap::Args)]
struct ClientArgs {}

#[derive(clap::Subcommand)]
enum Command {
    Server(ServerArgs),
    Invoker(InvokerArgs),
    Client(ClientArgs),
}

#[derive(clap::Parser)]
struct Cli {
    #[arg(long, default_value = "config.json")]
    config: std::path::PathBuf,
    #[command(subcommand)]
    command: Command,
}

async fn server_main(config: Config, _args: ServerArgs) -> Result<(), Error> {
    let _shutdown = CancellationToken::new();
    let mut core = Core::new(&config)?;
    core.init_server().await?;
    let _core = Arc::new(core);
    todo!()
}

async fn invoker_main(config: Config, _args: InvokerArgs) -> Result<(), Error> {
    let shutdown = CancellationToken::new();
    let mut core = Core::new(&config)?;
    core.init_invoker().await?;
    let core = Arc::new(core);
    let invoker_config = match &config.invoker {
        Some(v) => v,
        None => return Err("expected invoker section in config".into()),
    };
    let invoker = Invoker::new(core, invoker_config);
    tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            tokio::signal::ctrl_c()
                .await
                .expect("failed to listen for ctrl_c");
            shutdown.cancel();
        }
    });
    invoker.run(shutdown).await
}

async fn client_main(_config: Config, _args: ClientArgs) -> Result<(), Error> {
    todo!()
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let config = parse_file(cli.config).unwrap();
    match cli.command {
        Command::Server(args) => server_main(config, args).await.unwrap(),
        Command::Invoker(args) => invoker_main(config, args).await.unwrap(),
        Command::Client(args) => client_main(config, args).await.unwrap(),
    }
}
