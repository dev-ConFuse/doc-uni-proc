//! Main orchestration logic for the unified processor
//!
//! gRPC-based processing pipeline:
//! 1. Receive from: data-connector via gRPC calls
//! 2. Process: clone repo / download doc â†’ AST analysis â†’ intelligent chunking
//! 3. Store: chunks in local database
//! 4. Cleanup: delete temp content after processing

use crate::core::{Config, Result, ProcessorError};
use crate::processors::DocumentParser;

use crate::infra::storage::{PostgresStorage, GraphSync};
use crate::graph::extractors::SourceRelationshipRouter;
use crate::graph::SimplifiedChunk;
use crate::infra::events::producer::ChunkEventPublisher;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, LazyLock};
use uuid::Uuid;

// â”€â”€â”€ Data types â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessedData {
    pub id: Uuid,
    pub source_id: String,
    pub filename: String,
    pub content_type: ContentType,
    pub data: ProcessingResult,
    pub metadata: ProcessingMetadata,
    pub chunks: Vec<ChunkData>,
    pub processing_stage: String,
}

/// Individual chunk produced by the chunking pipeline
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkData {
    pub chunk_id: String,
    pub content: String,
    pub start_line: i32,
    pub end_line: i32,
    pub chunk_type: String,
    pub quality_score: f32,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ContentType {
    Document(DocumentData),
}



#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentData {
    pub text_content: String,
    pub sections: Vec<DocumentSection>,
    pub tables: Vec<DocumentTable>,
    pub figures: Vec<DocumentFigure>,
    pub processor: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CodeData {
    pub language: String,
    pub functions: Vec<CodeFunction>,
    pub classes: Vec<CodeClass>,
    pub imports: Vec<String>,
    pub metrics: CodeMetrics,
    pub ast_summary: AstSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessingResult {
    pub success: bool,
    pub processing_time_ms: u64,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessingMetadata {
    pub file_size: usize,
    pub line_count: usize,
    pub processed_at: String,
    pub embedding_model: Option<String>,
    pub embedding_generated: bool,
}

// Document-related types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentSection {
    pub title: String,
    pub content: String,
    pub level: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentTable {
    pub index: usize,
    pub content: String,
    pub caption: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentFigure {
    pub index: usize,
    pub caption: Option<String>,
    pub figure_type: String,
}

// Code-related types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CodeFunction {
    pub name: String,
    pub line_number: usize,
    pub parameters: Vec<String>,
    pub return_type: Option<String>,
    pub complexity: u32,
    pub docstring: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CodeClass {
    pub name: String,
    pub line_number: usize,
    pub methods: Vec<CodeFunction>,
    pub properties: Vec<String>,
    pub inheritance: Vec<String>,
    pub docstring: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CodeMetrics {
    pub lines_of_code: usize,
    pub lines_of_comments: usize,
    pub cyclomatic_complexity: u32,
    pub cognitive_complexity: u32,
    pub maintainability_index: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AstSummary {
    pub total_nodes: usize,
    pub max_depth: usize,
    pub node_types: std::collections::HashMap<String, usize>,
}

// â”€â”€â”€ Kafka message types â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€



/// Message consumed from `doc-processing.requests`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocProcessingRequest {
    pub request_id: String,
    pub user_id: String,
    pub document_id: String,
    pub filename: String,
    pub content: String, // base64-encoded document
    pub content_type: String, // MIME type
    pub metadata: HashMap<String, String>,
}

/// Message consumed from `repo-updates.requests`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepoUpdateRequest {
    pub update_id: String,
    pub repo_id: String,
    pub repo_url: String,
    pub branch: String,
    pub credentials: Option<String>,
    pub from_commit: String,
    pub to_commit: String,
    pub provider: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebProcessingRequest {
    pub request_id: String,
    pub user_id: String,
    pub url: String,
    pub crawl: bool,
    pub max_pages: Option<usize>,
    pub max_depth: Option<usize>,
    pub crawl_delay_ms: Option<u64>,
    pub include_css: Option<bool>,
    pub include_js: Option<bool>,
    pub metadata: HashMap<String, String>,
}



// â”€â”€â”€ Orchestrator â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€



#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebProcessingResult {
    pub request_id: String,
    pub url: String,
    pub pages_processed: usize,
    pub total_chunks: usize,
    pub processing_time_ms: u64,
}

pub struct UnifiedProcessor {
    config: Config,
    pub document_parser: DocumentParser,
    pub postgres_storage: Arc<PostgresStorage>,
    graph_sync: Arc<GraphSync>,
    chunker: crate::core::chunking::HybridChunker,
    /// Source-agnostic structural relationship router (replaces UnifiedRelationshipExtractor)
    relationship_router: SourceRelationshipRouter,


    pub falkordb_storage: Arc<crate::infra::storage::FalkordbStorage>,
    /// In-memory cache: chunk_id â†’ content, populated before Kafka publish,
    /// consumed by the embedding consumer to avoid PostgreSQL dependency.
    pub chunk_content_cache: Arc<Mutex<HashMap<String, String>>>,
}

impl UnifiedProcessor {

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub async fn new(
        config: Config,
        falkordb_storage: Arc<crate::infra::storage::FalkordbStorage>,
    ) -> Result<Self> {
        let document_parser = DocumentParser::new()?;
        
        let postgres_storage = Arc::new(
            PostgresStorage::new(&config.database.database_url).await?
        );
        
        let graph_sync = Arc::new(GraphSync::new(falkordb_storage.clone()));
        
        // Initialize advanced chunking system
        // Initialize advanced chunking system
        let chunker = crate::core::chunking::HybridChunker::new();
        

        let relationship_router = SourceRelationshipRouter::new();




        Ok(Self {
            config,
            document_parser,
            postgres_storage,
            graph_sync,
            chunker,

            relationship_router,

            falkordb_storage,
            chunk_content_cache: Arc::new(Mutex::new(HashMap::new())),
        })
    }
    



    // â”€â”€â”€ Legacy file processing (kept for gRPC health checks & direct calls) â”€â”€
    
    /// Process a single file (used by gRPC endpoint for backward compatibility)
    pub async fn process_file(&self, content: &str, is_base64: bool, filename: &str, source_id: &str, _repo_name: &str, user_id: &str) -> Result<ProcessedData> {
        let start_time = std::time::Instant::now();
        let file_id = Uuid::new_v4();
        
        let content_type = self.detect_content_type(filename);
        
        let (processing_result, mut chunks) = match content_type {
            ContentType::Document(_) => {
                let (res, doc_chunks) = self.process_document(content, is_base64, filename, source_id).await;
                tracing::info!("Finished process_document for {} with {} chunks", filename, doc_chunks.len());
                (res, doc_chunks)
            }
        };
        
        // --- NEW LOGIC: Diff against snapshot ---
        let mut chunks_to_delete_from_db = Vec::new();
        if let Ok(snapshots) = self.postgres_storage.get_chunk_snapshots(source_id, filename).await {
            let mut snapshot_map = std::collections::HashMap::new();
            for s in snapshots {
                snapshot_map.insert(s.chunk_key.clone(), s);
            }
            
            let mut new_keys = std::collections::HashSet::new();
            for chunk in chunks.iter_mut() {
                new_keys.insert(chunk.chunk_key.clone());
                if let Some(snapshot) = snapshot_map.get(&chunk.chunk_key) {
                    if chunk.chunk_hash == snapshot.chunk_hash {
                        chunk.is_dirty = false;
                        tracing::debug!("Chunk {} is clean (hash match)", chunk.chunk_key);
                    } else {
                        tracing::debug!("Chunk {} is dirty (hash mismatch)", chunk.chunk_key);
                    }
                } else {
                    tracing::debug!("Chunk {} is new", chunk.chunk_key);
                }
            }
            
            // Find deleted chunks
            for (old_key, _) in snapshot_map {
                if !new_keys.contains(&old_key) {
                    tracing::debug!("Chunk {} was deleted", old_key);
                    chunks_to_delete_from_db.push(format!("{}|{}", old_key, filename));
                    // Mark as tombstone in postgres
                    let _ = self.postgres_storage.mark_chunks_deleted(source_id, &[old_key]).await;
                }
            }
        }
        
        if !chunks_to_delete_from_db.is_empty() {
            tracing::info!("Deleting {} orphaned chunks from FalkorDB", chunks_to_delete_from_db.len());
            let user_graph = self.falkordb_storage.with_user_graph(user_id);
            let keys_str = chunks_to_delete_from_db.iter()
                .map(|k| format!("'{}'", k))
                .collect::<Vec<_>>()
                .join(",");
            let query = format!(
                "MATCH (c:Vector_Chunk) WHERE c.id IN [{}] DETACH DELETE c",
                keys_str
            );
            if let Err(e) = user_graph.execute_query(&query).await {
                tracing::error!("Failed to delete orphaned chunks: {}", e);
            }
        }
        // ----------------------------------------
        
        // Send chunks to falkordb & publish to embeddings-service
        // For documents, repo_name is explicitly set to None so that embeddings-service
        // generated events are routed back to doc-uni-proc instead of repo-uni-proc.
        if let Err(e) = self.store_and_publish_chunks(chunks.clone(), source_id, None, user_id).await {
            tracing::error!("Failed to store and publish chunks for {}: {}", filename, e);
        }

        // --- NEW LOGIC: Extract and store relationships for the single file ---
        if let Err(e) = self.extract_and_store_relationships(&chunks, source_id, user_id).await {
            tracing::error!("Failed to extract and store relationships for {}: {}", filename, e);
        }

        let chunk_data: Vec<ChunkData> = chunks.iter().enumerate().map(|(_idx, chunk)| {
            let (start_line, end_line) = chunk.metadata.line_range.unwrap_or((0, 0));
            let chunk_type_str = match &chunk.chunk_type {
                crate::core::chunking::ChunkType::Code { language: _, semantic_type } => format!("code:{:?}", semantic_type),
                crate::core::chunking::ChunkType::Document { format: _, semantic_type } => format!("doc:{:?}", semantic_type),
                crate::core::chunking::ChunkType::Web { url: _, semantic_type } => format!("web:{:?}", semantic_type),
                crate::core::chunking::ChunkType::Mixed => "mixed".to_string(),
            };
            ChunkData {
                chunk_id: chunk.chunk_key.clone(),
                content: chunk.content.clone(),
                start_line: start_line as i32,
                end_line: end_line as i32,
                chunk_type: chunk_type_str,
                quality_score: chunk.confidence,
                metadata: HashMap::new(),
            }
        }).collect();
        
        // Store chunks locally and publish to Kafka
        let processing_stage = "chunks_generated".to_string();
        let processing_time = start_time.elapsed().as_millis() as u64;
        
        let metadata = ProcessingMetadata {
            file_size: content.len(),
            line_count: content.lines().count(),
            processed_at: chrono::Utc::now().to_rfc3339(),
            embedding_model: None,
            embedding_generated: false,
        };
        
        let processed_data = ProcessedData {
            id: file_id,
            source_id: source_id.to_string(),
            filename: filename.to_string(),
            content_type: content_type.clone(),
            data: ProcessingResult {
                success: processing_result.success,
                processing_time_ms: processing_time,
                error: processing_result.error,
            },
            metadata,
            chunks: chunk_data,
            processing_stage,
        };
        
        // Store file metadata in PostgreSQL
        self.store_file_metadata(&processed_data, content, user_id).await?;
        
        Ok(processed_data)
    }
    
    
    // â”€â”€â”€ Internal helpers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    
    /// Generate chunks from file content
    pub(crate) async fn generate_chunks(
        &self,
        content: &str,
        filename: &str,
        source_id: &str,
    ) -> Result<Vec<crate::core::chunking::Chunk>> {
        use crate::core::chunking::ChunkingStrategy;
        
        let config = crate::core::chunking::ChunkingConfig::from_env();
        
        // Use the HybridChunker (which implements ChunkingStrategy)
        let result = self.chunker.process(content, filename, source_id, &config)
            .await
            .map_err(|e| ProcessorError::InfraError(format!("Chunking failed: {}", e)))?;
            
        Ok(result.chunks)
    }

    async fn process_document(&self, content: &str, is_base64: bool, filename: &str, source_id: &str) -> (ProcessingResult, Vec<crate::core::chunking::Chunk>) {
        // Enforce document size limits for container execution
        let max_size_mb: usize = std::env::var("MAX_DOCUMENT_SIZE_MB")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(50);
        if content.len() > max_size_mb * 1024 * 1024 {
            return (ProcessingResult {
                success: false,
                processing_time_ms: 0,
                error: Some(format!("Document exceeds maximum payload size limit of {} MB", max_size_mb)),
            }, vec![]);
        }

        // Write content to a temp file since process_document_file requires a file path
        let temp_dir = std::env::temp_dir();
        // Use a UUID to avoid path traversal or missing directory errors if filename contains slashes
        let ext = std::path::Path::new(filename).extension().and_then(|e| e.to_str()).unwrap_or("tmp");
        let safe_filename = format!("{}.{}", uuid::Uuid::new_v4(), ext);
        let file_path = temp_dir.join(safe_filename);
        
        let write_result = if is_base64 {
            use base64::{Engine as _, engine::general_purpose::STANDARD};
            match STANDARD.decode(content) {
                Ok(bytes) => {
                    let final_bytes = if bytes.len() >= 2 && bytes[0] == 0x1f && bytes[1] == 0x8b {
                        use std::io::Read;
                        let mut decoder = flate2::read::GzDecoder::new(&bytes[..]);
                        let mut decompressed = Vec::new();
                        if decoder.read_to_end(&mut decompressed).is_ok() {
                            tracing::info!("Decompressed gzip document payload: {} bytes -> {} bytes", bytes.len(), decompressed.len());
                            decompressed
                        } else {
                            bytes
                        }
                    } else {
                        bytes
                    };
                    std::fs::write(&file_path, final_bytes)
                }
                Err(e) => {
                    return (ProcessingResult {
                        success: false,
                        processing_time_ms: 0,
                        error: Some(format!("Failed to decode base64 content: {}", e)),
                    }, vec![]);
                }
            }
        } else {
            std::fs::write(&file_path, content)
        };
        
        if let Err(e) = write_result {
            return (ProcessingResult {
                success: false,
                processing_time_ms: 0,
                error: Some(format!("Failed to write temp file: {}", e)),
            }, vec![]);
        }

        let deployed_testing_enabled = self.postgres_storage.is_toggle_enabled("deployedTesting").await;
        if deployed_testing_enabled {
            tracing::info!(
                "Deployed Testing feature toggle is ENABLED: Disabling heavy OCR/Docling pipelines, using lightweight processing for {}",
                filename
            );
        }

        let result = match self.document_parser.process_document_file_with_options(file_path.to_str().unwrap(), deployed_testing_enabled).await {
            Ok(document_data) => {
                let chunks = crate::processors::parser::build_document_chunks(&document_data, filename, source_id);
                (ProcessingResult {
                    success: true,
                    processing_time_ms: 0,
                    error: None,
                }, chunks)
            }
            Err(e) => {
                tracing::error!("process_document_file failed: {}", e);
                (ProcessingResult {
                    success: false,
                    processing_time_ms: 0,
                    error: Some(e.to_string()),
                }, vec![])
            },
        };

        // Clean up temp file immediately after processing
        let _ = std::fs::remove_file(&file_path);

        result
    }



    async fn store_file_metadata(&self, data: &ProcessedData, _content: &str, user_id: &str) -> Result<()> {
        let file_type = match &data.content_type {
            ContentType::Document(_) => "document",
        };
        
        let language = match &data.content_type {
            ContentType::Document(_) => None,
        };
        
        let pg_metadata = crate::infra::storage::FileMetadata {
            id: data.id,
            source_id: data.source_id.clone(),
            filename: data.filename.clone(),
            file_type: file_type.to_string(),
            language,
            size_bytes: Some(data.metadata.file_size as i32),
            line_count: Some(data.metadata.line_count as i32),
            processed_at: chrono::DateTime::parse_from_rfc3339(&data.metadata.processed_at)
                .map_err(|e| ProcessorError::SerializationError(e.to_string()))?
                .with_timezone(&chrono::Utc),
            user_id: user_id.to_string(),
        };
        self.postgres_storage.store_file_metadata(pg_metadata, user_id).await?;
        
        Ok(())
    }

    /// Delete a file's chunks from FalkorDB when a deletion event is received.
    pub async fn delete_file(&self, file_path: &str, source_id: &str, user_id: &str) -> Result<()> {
        let start_time = std::time::Instant::now();
        tracing::info!(
            "Deleting file from FalkorDB: filename={}, source_id={}, user_graph=graph-{}",
            file_path, source_id, user_id
        );
        
        let user_graph = self.falkordb_storage.with_user_graph(user_id);
        let query = format!(
            "MATCH (c:Vector_Chunk) WHERE c.source_id = '{}' AND c.id ENDS WITH '|{}' DETACH DELETE c",
            source_id, file_path
        );
        
        match user_graph.execute_query(&query).await {
            Ok(_) => {
                tracing::info!(
                    "Successfully deleted file chunks in {}ms",
                    start_time.elapsed().as_millis()
                );
            },
            Err(e) => {
                tracing::error!("Failed to delete file chunks: {}", e);
                return Err(ProcessorError::DatabaseError(format!("Failed to delete chunks: {}", e)));
            }
        }
        
        // Also delete file metadata from PostgreSQL
        if let Err(e) = self.postgres_storage.delete_file_metadata(source_id, file_path).await {
            tracing::error!("Failed to delete file metadata: {}", e);
        }
        
        Ok(())
    }

    pub async fn trigger_graph_sync(&self, source_id: &str, user_id: &str) -> Result<()> {
        self.graph_sync.trigger_relationship_building(source_id, user_id).await?;
        Ok(())
    }

    fn detect_content_type(&self, filename: &str) -> ContentType {
        // DSA: O(1) HashSet lookup replaces O(n) linear array scan.
        // LazyLock ensures the set is built exactly once across all calls.
        static _CODE_EXTENSIONS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
            [
                "rs", "py", "js", "jsx", "ts", "tsx", "go", "java",
                "c", "cpp", "cxx", "cc", "h", "hpp", "cs", "rb",
                "kt", "swift", "scala", "lua", "dart", "r",
                "html", "htm", "xhtml", "css", "scss", "less", "vue", "svelte",
                "yaml", "yml", "json", "toml", "xml",
            ]
            .into_iter()
            .collect()
        });

        let extension = std::path::Path::new(filename)
            .extension()
            .and_then(|ext| ext.to_str())
            .unwrap_or("")
            .to_lowercase();

        static DOCUMENT_EXTENSIONS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
            [
                "pdf", "docx", "doc", "pptx", "ppt", "rtf", "epub", "md", "markdown", "txt"
            ]
            .into_iter()
            .collect()
        });

        if DOCUMENT_EXTENSIONS.contains(extension.as_str()) {
            return ContentType::Document(DocumentData {
                text_content: String::new(),
                sections: Vec::new(),
                tables: Vec::new(),
                figures: Vec::new(),
                processor: "unknown".to_string(),
            });
        }

        ContentType::Document(DocumentData {
            text_content: String::new(),
            sections: Vec::new(),
            tables: Vec::new(),
            figures: Vec::new(),
            processor: "unknown".to_string(),
        })
    }

    pub async fn get_processing_status(&self, source_id: &str, user_id: &str) -> Result<ProcessingStatus> {
        let files = self.postgres_storage.list_files_by_source(source_id, user_id).await?;
        let graph_status = self.graph_sync.get_graph_status(source_id).await.ok();
        
        Ok(ProcessingStatus {
            source_id: source_id.to_string(),
            total_files: files.len(),
            processed_files: files.len(),
            graph_built: graph_status.map(|s| s.processed).unwrap_or(false),
            last_updated: files.first()
                .map(|f| f.processed_at.to_rfc3339())
                .unwrap_or_else(|| chrono::Utc::now().to_rfc3339()),
        })
    }
    
    /// Get processor capabilities for health check
    pub fn get_capabilities(&self) -> ProcessorCapabilities {
        ProcessorCapabilities {
            tree_sitter_enabled: true,
            docling_enabled: false,
            nim_ocr_enabled: std::env::var("NVIDIA_NIM_ENDPOINT").is_ok(),
            kafka_connected: true, // Kafka enabled for embeddings pipeline
        }
    }

    /// Store chunk nodes in FalkorDB (without embeddings) and publish to Kafka for the embeddings-service.
    ///
    /// Pipeline:
    ///   1. Cache chunks in memory
    ///   2. Publish chunks to Kafka for embeddings-service processing
    ///   3. Store chunks directly in FalkorDB (embeddings callback will update them later)
    pub async fn store_and_publish_chunks(&self, chunks: Vec<crate::core::chunking::Chunk>, source_id: &str, _repo_name: Option<String>, user_id: &str) -> Result<()> {

        use std::collections::HashMap;

        if chunks.is_empty() {
            tracing::warn!("store_and_publish_chunks called with empty chunks array for source_id: {}", source_id);
            return Ok(());
        }

        // Resolve the per-user graph for all FalkorDB operations in this function
        let user_graph = self.falkordb_storage.with_user_graph(user_id);
        // Ensure the user's graph has indexes (lazy init)
        if let Err(e) = user_graph.ensure_user_graph().await {
            tracing::warn!("Failed to ensure user graph indexes for user {}: {}", user_id, e);
        }

        let chunk_count = chunks.len();
        tracing::info!(
            "store_and_publish_chunks starting: source_id={}, chunk_count={}, user_id={}, graph={}",
            source_id, chunk_count, user_id, user_graph.graph_name
        );
        
        let filename = &chunks[0].file_path;
        
        // --- Incremental Diffing Logic ---
        let existing_snapshots = self.postgres_storage.get_chunk_snapshots(source_id, filename).await.unwrap_or_default();
        
        let mut to_insert_chunks = Vec::new();
        let mut to_delete_uuids = Vec::new();
        let mut unchanged_count = 0;
        
        let mut existing_map: HashMap<String, crate::infra::storage::ChunkSnapshot> = existing_snapshots.into_iter()
            .filter(|s| !s.tombstone)
            .map(|s| (s.chunk_hash.clone(), s))
            .collect();
            
        let mut chunks_to_process = Vec::new();
        
        for chunk in &chunks {
            // Check if chunk with identical hash exists
            if existing_map.remove(&chunk.chunk_hash).is_some() {
                unchanged_count += 1;
            } else {
                to_insert_chunks.push(chunk.clone());
                chunks_to_process.push(chunk.clone());
            }
        }
        
        // Remaining chunks in existing_map are missing from the new parse, meaning they were deleted
        for (_, snapshot) in existing_map {
            to_delete_uuids.push(snapshot.id);
        }
        
        tracing::info!(
            "Diff results for {}: {} unchanged, {} new/modified, {} deleted",
            filename, unchanged_count, chunks_to_process.len(), to_delete_uuids.len()
        );
        
        if chunks_to_process.is_empty() && to_delete_uuids.is_empty() {
            tracing::info!("No changes detected for {}, skipping processing.", filename);
            return Ok(());
        }
        
        let mut snapshots_to_insert = Vec::new();
        for chunk in &chunks_to_process {
            let (start_line, end_line) = chunk.metadata.line_range.unwrap_or((0, 0));
            snapshots_to_insert.push(crate::infra::storage::ChunkSnapshot {
                id: chunk.id,
                source_id: source_id.to_string(),
                filename: chunk.file_path.clone(),
                start_byte: start_line as i32,
                end_byte: end_line as i32,
                chunk_key: chunk.chunk_key.clone(),
                chunk_hash: chunk.chunk_hash.clone(),
                commit_id: None,
                embedding_model: None,
                last_indexed_at: chrono::Utc::now(),
                tombstone: false,
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            });
        }
        
        if let Err(e) = self.postgres_storage.atomic_replace_file_chunks(source_id, filename, snapshots_to_insert, to_delete_uuids.clone()).await {
            tracing::warn!("Failed to atomic_replace_file_chunks: {}", e);
        }
        
        if !to_delete_uuids.is_empty() {
            if let Err(e) = user_graph.delete_chunks_by_ids(&to_delete_uuids).await {
                tracing::warn!("Failed to delete chunks from FalkorDB: {}", e);
            }
        }

        // Only process the new/modified chunks further down
        let chunks = chunks_to_process;
        let chunk_count = chunks.len();

        // â”€â”€ Step 1: Cache chunk content in-memory so the embedding consumer can
        //           look it up by chunk_id without needing a PostgreSQL `chunks` table.
        {
            let mut cache = self.chunk_content_cache.lock().unwrap_or_else(|p| p.into_inner());
            for c in chunks.iter() {
                let composite_id = format!("{}|{}", c.chunk_key, c.file_path);
                cache.insert(composite_id, c.content.clone());
            }
            tracing::info!("Cached {} new chunks in-memory for source: {}", chunk_count, source_id);
        }



        // â”€â”€ Step 2: Send chunks to embeddings-service via Kafka for embedding generation â”€â”€
        // NOTE: This is best-effort. If the embeddings-service is down, chunks are still
        // stored directly in FalkorDB (Step 2.5 below). When the embeddings callback
        // arrives later, it will MERGE-update the node with the actual embedding vector.
        {
            tracing::info!("Creating ChunkEventPublisher for Kafka publishing");
            let publisher = ChunkEventPublisher::new();

            // Convert internal Chunk models to SimplifiedChunk for Kafka
            tracing::info!("Converting {} chunks to SimplifiedChunk format", chunk_count);
            let simplified_chunks: Vec<SimplifiedChunk> = chunks.iter().filter(|c| c.is_dirty).enumerate().map(|(idx, c)| {
                let (start_line, end_line) = c.metadata.line_range.unwrap_or((0, 0));
                let composite_id = format!("{}|{}", c.chunk_key, c.file_path);

                tracing::debug!(
                    "Converting dirty chunk {}: id={}, type={:?}, lines={}-{}, confidence={}",
                    idx, composite_id, c.chunk_type, start_line, end_line, c.confidence
                );

                let content_len = c.content.len();
                if content_len > 100_000 {
                    tracing::warn!("Large chunk detected: id={}, size={} bytes, file={}", c.id, content_len, source_id);
                }

                let language = match &c.chunk_type {
                    crate::core::chunking::types::ChunkType::Code { language, .. } => Some(language.clone()),
                    _ => None,
                };

                SimplifiedChunk {
                    id: composite_id,
                    file_id: c.file_path.clone(),
                    chunk_type: format!("{:?}", c.chunk_type),
                    content: c.content.clone(),
                    language,
                    start_line: Some(start_line as u32),
                    end_line: Some(end_line as u32),
                    confidence: Some(c.confidence),
                    quality_score: c.quality.as_ref().map(|q| q.overall),
                }
            }).collect();

            tracing::info!("Attempting to publish {} simplified chunks to Kafka for embeddings", simplified_chunks.len());
            
            // Batch chunks to avoid Kafka MessageSizeTooLarge error
            for (batch_idx, batch) in simplified_chunks.chunks(1).enumerate() {
                let batch_vec = batch.to_vec();
                let batch_chunks_count = batch_vec.len();
                
                // Calculate approximate size
                let est_size = serde_json::to_string(&batch_vec).unwrap_or_default().len();
                
                tracing::debug!(
                    "Publishing batch {} ({} chunks, approx {} bytes) for source: {}",
                    batch_idx, batch_chunks_count, est_size, source_id
                );
                
                if est_size > 900_000 {
                    tracing::warn!("Chunk is very large ({} bytes), might exceed Kafka limit", est_size);
                }

                if let Err(e) = publisher.publish_chunks(source_id, _repo_name.clone(), batch_vec, Some(source_id.to_string()), user_id).await {
                    tracing::warn!(
                        "Kafka publish to embeddings-service failed for batch {} of source_id={}: {}. \
                         Chunks will still be stored directly in FalkorDB without embeddings.",
                        batch_idx, source_id, e
                    );
                    // Don't return error â€” continue to direct FalkorDB storage below
                    break;
                }
            }
            
            tracing::info!(
                "Kafka publish phase complete for {} chunks ({} dirty), source: {}",
                chunk_count, simplified_chunks.len(), source_id
            );
        }

        // â”€â”€ Step 1.5: Store chunks DIRECTLY in FalkorDB (without embeddings) â”€â”€
        // This ensures the knowledge graph is always populated, regardless of whether
        // the embeddings-service processes them. Uses MERGE so the embeddings callback
        // can update the node with the actual embedding vector later.
        {
            let mut chunks_stored = 0;
            tracing::info!(
                "[FalkorDB-Direct] Storing {} chunks directly in FalkorDB for source: {}",
                chunk_count, source_id
            );

            for c in chunks.iter() {
                let composite_id = format!("{}|{}", c.chunk_key, c.file_path);
                let repo_file_path = format!("{}/{}", source_id, c.file_path);
                let chunk_type_str = format!("{:?}", c.chunk_type);
                let metadata = serde_json::json!({
                    "confidence": c.confidence,
                    "level": format!("{:?}", c.level),
                    "user_id": user_id,
                });
                // Empty embedding â€” will be filled by the embeddings callback if it arrives
                let empty_embedding: Vec<f32> = vec![];
                let language_str = match &c.chunk_type {
                    crate::core::chunking::types::ChunkType::Code { language, .. } => language.clone(),
                    _ => "unknown".to_string(),
                };

                if let Err(e) = user_graph.store_chunk_with_embedding(
                    &composite_id,
                    source_id,
                    &c.content,
                    &empty_embedding,
                    &chunk_type_str,
                    &metadata,
                    "none",  // model = "none" until embeddings arrive
                    user_id,
                    &repo_file_path,
                    &language_str,
                ).await {
                    tracing::warn!(
                        "[FalkorDB-Direct] Failed to store chunk {} in FalkorDB: {}",
                        composite_id, e
                    );
                } else {
                    chunks_stored += 1;
                }
            }

            tracing::info!(
                "[FalkorDB-Direct] Stored {}/{} chunks directly in FalkorDB for source: {}",
                chunks_stored, chunk_count, source_id
            );
        }

        tracing::info!("store_and_publish_chunks completed successfully for source_id={}", source_id);
        Ok(())
    }

    /// Extract and store relationships across all chunks in a source.
    pub async fn extract_and_store_relationships(&self, chunks: &[crate::core::chunking::Chunk], source_id: &str, user_id: &str) -> Result<()> {
        let chunk_count = chunks.len();
        if chunk_count == 0 {
            return Ok(());
        }

        let user_graph = self.falkordb_storage.with_user_graph(user_id);
        
        // â”€â”€ Step 2: Structural relationship extraction (source-agnostic router) â”€â”€
        tracing::info!("Starting structural relationship extraction for {} chunks", chunk_count);
        let relationships = self.relationship_router.extract_all(chunks);





        let mut uuid_to_composite = std::collections::HashMap::new();
        for c in chunks {
            uuid_to_composite.insert(c.id, format!("{}|{}", c.chunk_key, c.file_path));
        }

        let mut rels_stored = 0;
        for rel in &relationships {
            if let (Some(source_composite), Some(target_composite)) = (
                uuid_to_composite.get(&rel.source_chunk_id),
                uuid_to_composite.get(&rel.target_chunk_id)
            ) {
                let metadata_val = serde_json::to_value(&rel.metadata).unwrap_or(serde_json::json!({}));
                if let Err(e) = user_graph.store_relationship(
                    source_composite,
                    target_composite,
                    rel.relationship_type.label(),
                    rel.confidence as f64,
                    &metadata_val
                ).await {
                    tracing::warn!("Failed to store relationship in FalkorDB: {}", e);
                } else {
                    rels_stored += 1;
                }
            } else {
                tracing::warn!("Could not find composite ID for relationship chunks");
            }
        }
        tracing::info!("Stored {} relationships in FalkorDB", rels_stored);

        tracing::info!("extract_and_store_relationships completed successfully for source_id={}", source_id);
        Ok(())
    }

    /// Get chunk content by chunk ID.
    /// First checks the in-memory cache, then falls back to FalkorDB.
    pub async fn get_chunk_content(&self, chunk_id: &str, user_id: &str) -> Result<String> {
        // Check in-memory cache first
        {
            let cache = self.chunk_content_cache.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(content) = cache.get(chunk_id) {
                return Ok(content.clone());
            }
        }
        
        tracing::warn!("Chunk not found in in-memory cache, falling back to FalkorDB: {}", chunk_id);
        let user_graph = self.falkordb_storage.with_user_graph(user_id);
        
        match user_graph.get_chunk_content(chunk_id).await {
            Ok(Some(content)) => Ok(content),
            Ok(None) => Err(ProcessorError::NotFound(format!("Chunk not found in FalkorDB: {}", chunk_id))),
            Err(e) => Err(ProcessorError::DatabaseError(format!("Failed to get chunk from FalkorDB: {}", e))),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessingStatus {
    pub source_id: String,
    pub total_files: usize,
    pub processed_files: usize,
    pub graph_built: bool,
    pub last_updated: String,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ProcessorCapabilities {
    pub tree_sitter_enabled: bool,
    pub docling_enabled: bool,
    pub nim_ocr_enabled: bool,
    pub kafka_connected: bool,
}



/// Helper: detect language from file path
/// DSA: O(1) static HashMap lookup, zero per-call allocation.
#[allow(dead_code)]
fn detect_language(file_path: &str) -> Option<String> {
    static EXT_TO_LANG: LazyLock<HashMap<&'static str, &'static str>> = LazyLock::new(|| {
        [
            ("py", "Python"),
            ("rs", "Rust"),
            ("js", "JavaScript"),
            ("mjs", "JavaScript"),
            ("cjs", "JavaScript"),
            ("ts", "TypeScript"),
            ("tsx", "TypeScript"),
            ("go", "Go"),
            ("java", "Java"),
            ("c", "C"),
            ("h", "C"),
            ("cpp", "C++"),
            ("cc", "C++"),
            ("cxx", "C++"),
            ("hpp", "C++"),
        ]
        .into_iter()
        .collect()
    });

    let ext = file_path.rsplit('.').next()?.to_lowercase();
    EXT_TO_LANG.get(ext.as_str()).map(|s| s.to_string())
}

