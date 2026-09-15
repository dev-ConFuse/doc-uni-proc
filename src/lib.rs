//! Unified Processor - ConFuse Platform
//!
//! Ingests code and documents, chunks them, runs AST analysis,
//! and stores data for knowledge graph construction.

pub mod core;
pub mod processors;
pub mod infra;
pub mod graph;

use std::sync::Arc;
use tokio::sync::OnceCell;
use crate::core::config::Config;
use crate::core::orchestrator::UnifiedProcessor;
use crate::infra::storage::create_falkordb_storage;

/// Application state shared across handlers
pub struct AppState {
    pub processor: core::orchestrator::UnifiedProcessor,
    pub config: core::Config,
}

impl AppState {
    pub fn new(processor: core::orchestrator::UnifiedProcessor, config: core::Config) -> Arc<Self> {
        Arc::new(Self { processor, config })
    }
}

static PROCESSOR: OnceCell<Arc<UnifiedProcessor>> = OnceCell::const_new();
static CONFIG: OnceCell<Config> = OnceCell::const_new();

/// Initialize the processor (called once per cold start)
pub async fn init_processor() -> Result<Arc<UnifiedProcessor>, Box<dyn std::error::Error + Send + Sync>> {
    PROCESSOR.get_or_try_init(|| async {
        // Load environment variables
        dotenvy::from_filename_override(".map.env").ok();
        dotenvy::from_filename_override(".secret.env").ok();
        dotenvy::from_filename_override(".env.local").ok();
        dotenvy::dotenv().ok();

        // Load config
        let config = Config::from_env().unwrap_or_else(|e| {
            tracing::warn!("Config error, falling back to defaults: {}", e);
            Config::default()
        });
        CONFIG.set(config.clone()).ok();

        // Initialize FalkorDB storage
        let falkordb_storage = create_falkordb_storage(
            &config.falkordb.host,
            config.falkordb.port,
            "",
            &config.falkordb.username,
            config.falkordb.password.as_deref().unwrap_or(""),
            config.falkordb.use_tls,
            config.falkordb.embedding_dim,
        ).await.map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        // Initialize processor
        let processor = Arc::new(UnifiedProcessor::new(config, falkordb_storage).await.map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?);
        Ok(processor)
    }).await.map(|p| p.clone())
}

/// Get the initialized processor
pub async fn get_processor() -> Result<Arc<UnifiedProcessor>, Box<dyn std::error::Error + Send + Sync>> {
    init_processor().await
}

/// Get the config
pub fn get_config() -> Option<&'static Config> {
    CONFIG.get()
}
