#![forbid(unsafe_code)]
#![deny(clippy::all, clippy::pedantic)]

use std::io::IsTerminal;
use std::net::TcpListener;
use std::path::PathBuf;
use std::str::FromStr;

use clap::{CommandFactory, Parser};
use hyper::server::Server;
use s3s::auth::SimpleAuth;
use s3s::service::S3ServiceBuilder;
use tendermint_rpc::Url;
use tracing::info;

use s3s_ipc::fendermint::{AccountKind, BroadcastMode, TransArgs};
use s3s_ipc::Ipc;
use s3s_ipc::Result;

#[derive(Debug, Parser)]
#[command(version)]
struct Opt {
    /// Host name to listen on.
    #[arg(long, default_value = "localhost")]
    host: String,

    /// Port number to listen on.
    #[arg(long, default_value = "8014")] // The original design was finished on 2020-08-14.
    port: u16,

    /// Private key used with Fendermint.
    #[arg(long)]
    private_key: PathBuf,

    /// Access key used for authentication.
    #[arg(long)]
    access_key: Option<String>,

    /// Secret key used for authentication.
    #[arg(long)]
    secret_key: Option<String>,

    /// Domain name used for virtual-hosted-style requests.
    #[arg(long)]
    domain_name: Option<String>,

    /// Root directory of stored data.
    root: PathBuf,
}

fn setup_tracing() {
    use tracing_subscriber::EnvFilter;

    let env_filter = EnvFilter::from_default_env();
    let enable_color = std::io::stdout().is_terminal();

    tracing_subscriber::fmt()
        .pretty()
        .with_env_filter(env_filter)
        .with_ansi(enable_color)
        .init();
}

fn check_cli_args(opt: &Opt) {
    use clap::error::ErrorKind;

    let mut cmd = Opt::command();

    // TODO: how to specify the requirements with clap derive API?
    if let (Some(_), None) | (None, Some(_)) = (&opt.access_key, &opt.secret_key) {
        let msg = "access key and secret key must be specified together";
        cmd.error(ErrorKind::MissingRequiredArgument, msg).exit();
    }

    if let Some(ref s) = opt.domain_name {
        if s.contains('/') {
            let msg = format!("expected domain name, found URL-like string: {s:?}");
            cmd.error(ErrorKind::InvalidValue, msg).exit();
        }
    }
}

fn main() -> Result {
    let opt = Opt::parse();
    check_cli_args(&opt);

    setup_tracing();

    run(opt)
}

#[tokio::main]
async fn run(opt: Opt) -> Result {
    // Setup S3 provider
    let url = Url::from_str("http://127.0.0.1:26657").unwrap();
    let args = TransArgs {
        chain_name: "test".to_string(),
        value: Default::default(),
        secret_key: opt.private_key,
        account_kind: AccountKind::Regular,
        sequence: 0,
        gas_limit: 10_000_000_000,
        gas_fee_cap: Default::default(),
        gas_premium: Default::default(),
        broadcast_mode: BroadcastMode::Commit,
    };
    let ipc = Ipc::new(opt.root, url, args)?;

    // Setup S3 service
    let service = {
        let mut b = S3ServiceBuilder::new(ipc);

        // Enable authentication
        if let (Some(ak), Some(sk)) = (opt.access_key, opt.secret_key) {
            b.set_auth(SimpleAuth::from_single(ak, sk));
            info!("authentication is enabled");
        }

        // Enable parsing virtual-hosted-style requests
        if let Some(domain_name) = opt.domain_name {
            b.set_base_domain(domain_name);
            info!("virtual-hosted-style requests are enabled");
        }

        b.build()
    };

    // Run server
    let listener = TcpListener::bind((opt.host.as_str(), opt.port))?;
    let local_addr = listener.local_addr()?;

    let server = Server::from_tcp(listener)?.serve(service.into_shared().into_make_service());

    info!("server is running at http://{local_addr}");
    server.with_graceful_shutdown(shutdown_signal()).await?;

    info!("server is stopped");
    Ok(())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
