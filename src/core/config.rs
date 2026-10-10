//! Configuration for the unified processor

use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub database: DatabaseConfig,
    pub pipeline: PipelineConfig,
    pub grpc: Option<GrpcConfig>,
    pub falkordb: FalkordbConfig,
    pub kafka: KafkaConfig,
    pub web: WebConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub workers: usize,
    pub grpc_host: String,
    pub grpc_port: u16,
    pub auth_middleware_url: String,
    pub auth_grpc_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrpcConfig {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    pub database_url: String,
    pub max_connections: u32,
}

impl DatabaseConfig {
    pub fn from_env() -> Result<Self, String> {
        Ok(Self {
            database_url: std::env::var("POSTGRES_URL")
                .or_else(|_| std::env::var("DATABASE_URL"))
                .map_err(|_| "POSTGRES_URL or DATABASE_URL must be set".to_string())?,
            max_connections: std::env::var("DB_MAX_CONNECTIONS")
                .unwrap_or_else(|_| "10".to_string())
                .parse()
                .unwrap_or(10),
        })
    }
}

/// Temporary storage configuration for repos and documents
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineConfig {
    pub max_file_size: usize,
    pub chunk_size: usize,
    pub max_batch_size: usize,
    pub timeout: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FalkordbConfig {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: Option<String>,
    pub use_tls: bool,
    /// Embedding dimension used when creating the vector index.
    pub embedding_dim: usize,
    /// Connection-level timeout in seconds.
    pub timeout_secs: u64,
}

impl FalkordbConfig {
    pub fn from_env() -> Result<Self, String> {
        Ok(Self {
            host: std::env::var("FALKORDB_HOST")
                .map_err(|_| "FALKORDB_HOST must be set".to_string())?,
            port: std::env::var("FALKORDB_PORT")
                .unwrap_or_else(|_| "50860".to_string())
                .parse()
                .unwrap_or(50860),
            username: std::env::var("FALKORDB_USERNAME")
                .unwrap_or_else(|_| "falkordb".to_string()),
            password: std::env::var("FALKORDB_PASSWORD").ok(),
            use_tls: std::env::var("FALKORDB_USE_TLS")
                .map(|v| v.to_lowercase() == "true" || v == "1")
                .unwrap_or(false), // Default to false for plaintext endpoints
            embedding_dim: std::env::var("FALKORDB_EMBEDDING_DIM")
                .unwrap_or_else(|_| "1536".to_string())
                .parse()
                .unwrap_or(1536),
            timeout_secs: std::env::var("FALKORDB_TIMEOUT_SECS")
                .unwrap_or_else(|_| "30".to_string())
                .parse()
                .unwrap_or(30),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebConfig {
    /// Enable/disable web scraping feature.
    pub enabled: bool,
    /// Default max pages per crawl.
    pub max_pages: usize,
    /// Default max depth per crawl.
    pub max_depth: usize,
    /// Default delay between requests (ms).
    pub crawl_delay_ms: u64,
    /// HTTP User-Agent string.
    pub user_agent: String,
    /// Per-request HTTP timeout (seconds).
    pub request_timeout_secs: u64,
    /// Maximum concurrent crawl jobs.
    pub max_concurrent_crawls: usize,
}

impl WebConfig {
    pub fn from_env() -> Result<Self, String> {
        Ok(Self {
            enabled: std::env::var("WEB_SCRAPING_ENABLED")
                .unwrap_or_else(|_| "true".to_string())
                .parse()
                .unwrap_or(true),
            max_pages: std::env::var("WEB_MAX_PAGES")
                .unwrap_or_else(|_| "50".to_string())
                .parse()
                .unwrap_or(50),
            max_depth: std::env::var("WEB_MAX_DEPTH")
                .unwrap_or_else(|_| "3".to_string())
                .parse()
                .unwrap_or(3),
            crawl_delay_ms: std::env::var("WEB_CRAWL_DELAY_MS")
                .unwrap_or_else(|_| "1000".to_string())
                .parse()
                .unwrap_or(1000),
            user_agent: std::env::var("WEB_USER_AGENT")
                .unwrap_or_else(|_| "UnifiedProcessorBot/1.0".to_string()),
            request_timeout_secs: std::env::var("WEB_REQUEST_TIMEOUT_SECS")
                .unwrap_or_else(|_| "30".to_string())
                .parse()
                .unwrap_or(30),
            max_concurrent_crawls: std::env::var("WEB_MAX_CONCURRENT_CRAWLS")
                .unwrap_or_else(|_| "5".to_string())
                .parse()
                .unwrap_or(5),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaConfig {
    pub bootstrap_servers: String,
    pub group_id: String,
    pub client_id: String,
    pub auto_offset_reset: String,
    pub enable_auto_commit: bool,
}

impl KafkaConfig {
    pub fn from_env() -> Result<Self, String> {
        Ok(Self {
            bootstrap_servers: std::env::var("KAFKA_BOOTSTRAP_SERVERS")
                .map_err(|_| "KAFKA_BOOTSTRAP_SERVERS must be set".to_string())?,
            group_id: std::env::var("DOC_UNI_PROC_KAFKA_GROUP_ID")
                .unwrap_or_else(|_| "doc-uni-proc-group".to_string()),
            client_id: std::env::var("KAFKA_CLIENT_ID")
                .unwrap_or_else(|_| "unified-processor".to_string()),
            auto_offset_reset: std::env::var("KAFKA_AUTO_OFFSET_RESET")
                .unwrap_or_else(|_| "earliest".to_string()),
            enable_auto_commit: std::env::var("KAFKA_ENABLE_AUTO_COMMIT")
                .unwrap_or_else(|_| "true".to_string())
                .parse()
                .unwrap_or(true),
        })
    }
}



impl Config {
    pub fn from_env() -> crate::core::Result<Self> {
        let host = std::env::var("HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
        let port = std::env::var("PORT")
            .or_else(|_| std::env::var("DOC_UNI_PROC_PORT"))
            .or_else(|_| std::env::var("HTTP_PORT"))
            .unwrap_or_else(|_| "8090".to_string())
            .parse()
            .unwrap_or(8090);
        
        let grpc_host = std::env::var("GRPC_HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
        let grpc_port = std::env::var("GRPC_PORT")
            .unwrap_or_else(|_| "50051".to_string())
            .parse()
            .unwrap_or(50051);
        
        let auth_middleware_url = std::env::var("AUTH_MIDDLEWARE_URL")
            .map_err(|_| crate::core::error::ProcessorError::ConfigError("AUTH_MIDDLEWARE_URL must be set".to_string()))?;
        let auth_grpc_url = std::env::var("AUTH_GRPC_URL")
            .unwrap_or_default();
        let workers = std::env::var("WORKERS")
            .unwrap_or_else(|_| "4".to_string())
            .parse()
            .unwrap_or(4);

        let grpc = Some(GrpcConfig {
            host: grpc_host.clone(),
            port: grpc_port,
        });

        // Pipeline Config
        let pipeline = PipelineConfig {
            max_file_size: std::env::var("PIPELINE_MAX_FILE_SIZE")
                .unwrap_or_else(|_| "10485760".to_string())
                .parse()
                .unwrap_or(10485760),
            chunk_size: std::env::var("PIPELINE_CHUNK_SIZE")
                .unwrap_or_else(|_| "1000".to_string())
                .parse()
                .unwrap_or(1000),
            max_batch_size: std::env::var("PIPELINE_MAX_BATCH_SIZE")
                .unwrap_or_else(|_| "100".to_string())
                .parse()
                .unwrap_or(100),
            timeout: Duration::from_secs(
                std::env::var("PIPELINE_TIMEOUT_SECS")
                    .unwrap_or_else(|_| "300".to_string())
                    .parse()
                    .unwrap_or(300)
            ),
        };

        let database = DatabaseConfig::from_env()
            .map_err(crate::core::error::ProcessorError::ConfigError)?;
        let falkordb = FalkordbConfig::from_env()
            .map_err(crate::core::error::ProcessorError::ConfigError)?;
        let kafka = KafkaConfig::from_env()
            .map_err(crate::core::error::ProcessorError::ConfigError)?;
        let web = WebConfig::from_env()
            .map_err(crate::core::error::ProcessorError::ConfigError)?;
        Ok(Self {
            server: ServerConfig {
                host,
                port,
                workers,
                grpc_host,
                grpc_port,
                auth_middleware_url,
                auth_grpc_url,
            },
            database,
            pipeline,
            grpc,
            falkordb,
            kafka,
            web,
        })
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            server: ServerConfig {
                host: "0.0.0.0".to_string(),
                port: std::env::var("PORT").unwrap_or_else(|_| "8080".to_string()).parse().unwrap_or(8080),
                workers: 4,
                grpc_host: "0.0.0.0".to_string(),
                grpc_port: 50052,
                auth_middleware_url: std::env::var("AUTH_MIDDLEWARE_URL").unwrap_or_else(|_| "http://auth-middleware:3010".to_string()),
                auth_grpc_url: "".to_string(),
            },
            database: DatabaseConfig {
                database_url: std::env::var("POSTGRES_URL").or_else(|_| std::env::var("DATABASE_URL")).unwrap_or_else(|_| "postgresql://postgres:postgres@localhost:5432/confuse".to_string()),
                max_connections: 5,
            },
            pipeline: PipelineConfig {
                max_file_size: 10485760,
                chunk_size: 1000,
                max_batch_size: 100,
                timeout: Duration::from_secs(300),
            },
            grpc: Some(GrpcConfig {
                host: "0.0.0.0".to_string(),
                port: 50052,
            }),
            falkordb: FalkordbConfig {
                host: std::env::var("FALKORDB_HOST").unwrap_or_else(|_| "localhost".to_string()),
                port: std::env::var("FALKORDB_PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(50860),
                username: std::env::var("FALKORDB_USERNAME").unwrap_or_else(|_| "falkordb".to_string()),
                password: std::env::var("FALKORDB_PASSWORD").ok().or_else(|| Some("adminconfuse".to_string())),
                use_tls: std::env::var("FALKORDB_USE_TLS").map(|v| v == "true" || v == "1").unwrap_or(false),
                embedding_dim: 1536,
                timeout_secs: 30,
            },
            kafka: KafkaConfig {
                bootstrap_servers: std::env::var("KAFKA_BOOTSTRAP_SERVERS").unwrap_or_else(|_| "localhost:9092".to_string()),
                group_id: "doc-uni-proc-group".to_string(),
                client_id: "unified-processor".to_string(),
                auto_offset_reset: "earliest".to_string(),
                enable_auto_commit: true,
            },
            web: WebConfig {
                enabled: true,
                max_pages: 50,
                max_depth: 3,
                crawl_delay_ms: 1000,
                user_agent: "UnifiedProcessorBot/1.0".to_string(),
                request_timeout_secs: 30,
                max_concurrent_crawls: 5,
            },
        }
    }
}

