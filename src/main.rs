use std::sync::Arc;

use axum::{routing, Router};
use clap::Parser;
use solve::config::{parse_file, Config};
use solve::core::{Core, Error};
use solve::invoker::Invoker;
use solve::server::Server;
use tokio::net::TcpListener;
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
    #[arg(long, default_value = "config.json", global = true)]
    config: std::path::PathBuf,
    #[command(subcommand)]
    command: Command,
}

async fn ping() -> &'static str {
    "pong"
}

async fn server_main(config: Config, _args: ServerArgs) -> Result<(), Error> {
    let shutdown = CancellationToken::new();
    let mut core = Core::new(&config)?;
    core.init_server(&config).await?;
    let core = Arc::new(core);
    let server_config = match &config.server {
        Some(v) => v,
        None => return Err("Expected server section in config".into()),
    };
    let server = Server::new(core, server_config)?;
    tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            tokio::signal::ctrl_c()
                .await
                .expect("Failed to listen for ctrl_c");
            shutdown.cancel();
        }
    });
    let router = Router::new().route("/ping", routing::get(ping));
    let addr = format!("{}:{}", server_config.host, server_config.port);
    let listener = TcpListener::bind(addr).await?;
    Ok(axum::serve(listener, router)
        .with_graceful_shutdown(shutdown.cancelled_owned())
        .await?)
}

async fn invoker_main(config: Config, _args: InvokerArgs) -> Result<(), Error> {
    let shutdown = CancellationToken::new();
    let mut core = Core::new(&config)?;
    core.init_invoker(&config).await?;
    let core = Arc::new(core);
    let invoker_config = match &config.invoker {
        Some(v) => v,
        None => return Err("Expected invoker section in config".into()),
    };
    let invoker = Invoker::new(core, invoker_config)?;
    tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            tokio::signal::ctrl_c()
                .await
                .expect("Failed to listen for ctrl_c");
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
