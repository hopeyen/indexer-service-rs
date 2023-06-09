use std::collections::HashSet;
use autometrics::autometrics;
use serde::{Deserialize, Serialize};
use tracing::{debug, info};
use ethers::signers::WalletError;
use clap::{Args, Parser, CommandFactory, command, ValueEnum, error::ErrorKind};

use crate::{common::address::build_wallet, util::init_tracing, query_processor::QueryError};

#[derive(Clone, Debug, Parser, Serialize, Deserialize, Default)]
#[clap(
    name = "indexer-service",
    about = "Indexer service on top of graph node",
    author = "hopeyen"
)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    #[command(flatten)]
    pub ethereum: Ethereum,
    #[command(flatten)]
    pub indexer_infrastructure: IndexerInfrastructure,
    #[command(flatten)]
    pub postgres: Postgres,
    #[command(flatten)]
    pub network_subgraph: NetworkSubgraph,

    /// some regular input
    #[arg(group = "input")]
    input_file: Option<String>,

    /// some special input argument
    #[arg(long, group = "input")]
    spec_in: Option<String>,

    #[arg(
        short,
        requires = "input",
        value_name = "config",
        env = "CONFIG",
        help = "Indexer service configuration file (YAML format)"
    )]
    config: Option<String>,
}

#[derive(Clone, Debug, Args, Serialize, Deserialize, Default)]
#[group(required = true, multiple = true)]
pub struct Ethereum {
    #[clap(
        long,
        value_name = "ethereum-node-provider",
        env = "ETH_NODE",
        help = "Ethereum node or provider URL"
    )]
    pub ethereum: String,
    #[clap(
        long,
        value_name = "ethereum-polling-interval",
        env = "ETHEREUM_POLLING_INTERVAL",
        default_value_t = 4000,
        help = "Polling interval for the Ethereum provider (ms)"
    )]
    pub ethereum_polling_interval: usize,
    #[clap(
        long,
        value_name = "mnemonic",
        env = "MNEMONIC",
        help = "Mnemonic for the operator wallet"
    )]
    pub mnemonic: String,
    #[clap(
        long,
        value_name = "indexer-address",
        env = "INDEXER_ADDRESS",
        help = "Ethereum address of the indexer"
    )]
    pub indexer_address: String,
}

#[derive(Clone, Debug, Args, Serialize, Deserialize, Default)]
#[group(required = true, multiple = true)]
pub struct IndexerInfrastructure {
    #[clap(
        long,
        value_name = "port",
        env = "PORT",
        default_value_t = 7600,
        help = "Port to serve queries at"
    )]
    pub port: u32,
    #[clap(
        long,
        value_name = "metrics-port",
        env = "METRICS_PORT",
        default_value_t = 7300,
        help = "Port to serve Prometheus metrics at"
    )]
    pub metrics_port: u16,
    #[clap(
        long,
        value_name = "graph-node-query-endpoint",
        env = "GRAPH_NODE_QUERY_ENDPOINT",
        default_value_t = String::from("http://0.0.0.0:8000"),
        help = "Graph node GraphQL HTTP service endpoint"
    )]
    pub graph_node_query_endpoint: String,
    #[clap(
        long,
        value_name = "graph-node-status-endpoint",
        env = "GRAPH_NODE_STATUS_ENDPOINT",
        default_value_t = String::from("http://0.0.0.0:8030"),
        help = "Graph node endpoint for the index node server"
    )]
    pub graph_node_status_endpoint: String,
    #[clap(
        long,
        value_name = "log-level",
        env = "LOG_LEVEL",
        default_value_t = LogLevel::Debug,
        value_enum,
        help = "Log level"
    )]
    pub log_level: LogLevel,
    #[clap(
        long,
        value_name = "gcloud-profiling",
        env = "GCLOUD_PROFILING",
        default_value_t = false,
        help = "Whether to enable Google Cloud profiling"
    )]
    pub gcloud_profiling: bool,
    #[clap(
        long,
        value_name = "free-query-auth-token",
        env = "FREE_QUERY_AUTH_TOKEN",
        help = "Auth token that clients can use to query for free"
    )]
    pub free_query_auth_token: Option<String>,
}

#[derive(Clone, Debug, Args, Serialize, Deserialize, Default)]
#[group(required = true, multiple = true)]
pub struct Postgres {
    #[clap(
        long,
        value_name = "postgres-host",
        env = "POSTGRES_HOST",
        default_value_t = String::from("http://0.0.0.0/"),
        help = "Postgres host"
    )]
    pub postgres_host: String,
    #[clap(
        long,
        value_name = "postgres-port",
        env = "POSTGRES_PORT",
        default_value_t = 5432,
        help = "Postgres port"
    )]
    pub postgres_port: usize,
    #[clap(
        long,
        value_name = "postgres-database",
        env = "POSTGRES_DATABASE",
        help = "Postgres database name"
    )]
    pub postgres_database: String,
    #[clap(
        long,
        value_name = "postgres-username",
        env = "POSTGRES_USERNAME",
        default_value_t = String::from("postgres"),
        help = "Postgres username"
    )]
    pub postgres_username: String,
    #[clap(
        long,
        value_name = "postgres-password",
        env = "POSTGRES_PASSWORD",
        default_value_t = String::from(""),
        help = "Postgres password"
    )]
    pub postgres_password: String,
}

#[derive(Clone, Debug, Args, Serialize, Deserialize, Default)]
#[group(required = true, multiple = true)]
pub struct NetworkSubgraph {
    #[clap(
        long,
        value_name = "network-subgraph-deployment",
        env = "NETWORK_SUBGRAPH_DEPLOYMENT",
        help = "Network subgraph deployment"
    )]
    pub network_subgraph_deployment: Option<String>,
    #[clap(
        long,
        value_name = "network-subgraph-endpoint",
        env = "NETWORK_SUBGRAPH_ENDPOINT",
        default_value_t = String::from("https://api.thegraph.com/subgraphs/name/graphprotocol/graph-network-goerli"),
        help = "Endpoint to query the network subgraph from"
    )]
    pub network_subgraph_endpoint: String,
    #[clap(
        long,
        value_name = "network-subgraph-auth-token",
        env = "NETWORK_SUBGRAPH_AUTH_TOKEN",
        help = "Bearer token to require for /network queries"
    )]
    pub network_subgraph_auth_token: Option<String>,
    #[clap(
        long,
        value_name = "serve-network-subgraph",
        env = "SERVE_NETWORK_SUBGRAPH",
        default_value_t = false,
        help = "Whether to serve the network subgraph at /network"
    )]
    pub serve_network_subgraph: bool,
    #[clap(
        long,
        value_name = "allocation-syncing-interval",
        env = "ALLOCATION_SYNCING_INTERVAL",
        default_value_t = 120_000,
        help = "Interval (in ms) for syncing indexer allocations from the network"
    )]
    pub allocation_syncing_interval: u32,
    #[clap(
        long,
        value_name = "client-signer-address",
        env = "CLIENT_SIGNER_ADDRESS",
        help = "Address that signs query fee receipts from a known client"
    )]
    pub client_signer_address: Option<String>,
}

impl Cli {
    /// Parse config arguments
    pub fn args() -> Self {
        // TODO: load config file before parse
        let cli = Cli::parse();
        if let Some(path) = cli.input_file.clone(){
            let loaded_cli = confy::load_path::<Cli>(path);
            println!("loaded cli, not used, but may later be used by overwriting cli arguments: {:#?}", loaded_cli);
        };
        
        // Enables tracing under RUST_LOG variable
        // std::env::set_var("RUST_LOG", cli.log_level.clone());
        init_tracing(String::from("pretty")).expect("Could not set up global default subscriber for logger, check environmental variable `RUST_LOG` or the CLI input `log-level`");
        cli
    }

    pub fn parse_config_file(&self) {
        if let Some(config) = self.config.as_deref() {
            let input = self
                .input_file
                .as_deref()
                // 'or' is preferred to 'or_else' here since `Option::as_deref` is 'const'
                .or(self.spec_in.as_deref())
                .unwrap_or_else(|| {
                    let mut cmd = Cli::command();
                    cmd.error(
                        ErrorKind::MissingRequiredArgument,
                        "INPUT_FILE or --spec-in is required when using --config",
                    )
                    .exit()
                });
            println!("Using input {input} and config {config}");
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("Validate the input: {0}")]
    ValidateInput(String),
    #[error("Generate JSON representation of the config file: {0}")]
    GenerateJson(serde_json::Error),
    #[error("QueryError: {0}")]
    QueryError(QueryError),
    #[error("Toml file error: {0}")]
    ReadStr(std::io::Error),
    #[error("Unknown error: {0}")]
    Other(anyhow::Error),
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Serialize, Deserialize)]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
    Fatal,
}

impl Default for LogLevel {
    fn default() -> Self {
        LogLevel::Debug
    }
}
