//! Chunking configuration

use serde::{Deserialize, Serialize};
use tracing::info;

/// Configuration for chunking system
#[derive(Debug, Clone, Serialize, Deserialize)]
#[derive(Default)]
pub struct ChunkingConfig {
    /// Size parameters
    pub size: SizeConfig,
    
    /// Quality thresholds
    pub quality: QualityConfig,
    
    /// Agent settings
    pub agents: AgentConfig,
    
    /// Performance settings
    pub performance: PerformanceConfig,
}


/// Size and overlap configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SizeConfig {
    /// Maximum chunk size in characters
    pub max_chunk_size: usize,
    
    /// Minimum chunk size in characters
    pub min_chunk_size: usize,
    
    /// Overlap size for semantic continuity
    pub overlap_size: usize,
    
    /// Target chunk size (soft limit)
    pub target_size: usize,
}

impl Default for SizeConfig {
    fn default() -> Self {
        Self {
            // Tuned for retrieval precision rather than raw throughput: a chunk
            // should carry roughly one idea so its embedding stays focused and a
            // narrow query can match it. ~500 chars is ~125 tokens — small enough
            // to rank precisely, large enough to stand alone alongside the overlap.
            max_chunk_size: 800,
            min_chunk_size: 150,
            overlap_size: 100,
            target_size: 500,
        }
    }
}

impl SizeConfig {
    /// Override from environment so chunk sizing is tunable without a rebuild.
    fn from_env() -> Self {
        let d = Self::default();
        let read = |key: &str, fallback: usize| -> usize {
            std::env::var(key)
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(fallback)
        };
        Self {
            max_chunk_size: read("CHUNK_MAX_SIZE", d.max_chunk_size),
            min_chunk_size: read("CHUNK_MIN_SIZE", d.min_chunk_size),
            overlap_size: read("CHUNK_OVERLAP_SIZE", d.overlap_size),
            target_size: read("CHUNK_TARGET_SIZE", d.target_size),
        }
    }
}

/// Quality threshold configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QualityConfig {
    /// Minimum confidence for tier 1 chunks (0.0-1.0)
    pub confidence_threshold: f32,
    
    /// Minimum quality for enhanced chunks (0.0-1.0)
    pub quality_threshold: f32,
    
    /// Complexity threshold for agent enhancement (1-10)
    pub complexity_threshold: u8,
    
    /// Minimum boundary clarity (0.0-1.0)
    pub boundary_threshold: f32,
}

impl Default for QualityConfig {
    fn default() -> Self {
        Self {
            confidence_threshold: 0.7,
            quality_threshold: 0.85,
            complexity_threshold: 7,
            boundary_threshold: 0.75,
        }
    }
}

/// AI agent configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentConfig {
    /// Enable agent enhancement (Tier 2)
    pub enabled: bool,
    
    /// Agent pool size for concurrent processing
    pub pool_size: usize,
    
    /// Timeout for agent operations (seconds)
    pub timeout_seconds: u64,

    
    /// Cache LLM responses (reduces API calls)
    pub enable_cache: bool,
    
    /// Cache TTL in seconds
    pub cache_ttl_seconds: u64,
    
    /// Maximum tokens per LLM request
    pub max_tokens: usize,
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            pool_size: 4,
            timeout_seconds: 300,

            enable_cache: true,
            cache_ttl_seconds: 3600,
            max_tokens: 4096,
        }
    }
}

/// Performance optimization settings
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceConfig {
    /// Enable parallel chunking
    pub enable_parallel: bool,
    
    /// Number of parallel workers
    pub num_workers: usize,
    
    /// Batch size for database operations
    pub batch_size: usize,
    
    /// Maximum memory usage in MB
    pub max_memory_mb: usize,
    
    /// Enable relationship extraction (Tier 3)
    pub enable_relationships: bool,
    
    /// Relationship extraction timeout (seconds)
    pub relationship_timeout_seconds: u64,
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            enable_parallel: true,
            num_workers: num_cpus::get(),
            batch_size: 100,
            max_memory_mb: 2048,
            enable_relationships: true,
            relationship_timeout_seconds: 180,
        }
    }
}

impl ChunkingConfig {
    /// Load from environment variables
    pub fn from_env() -> Self {
        let mut config = Self::default();

        // Chunk sizing is tunable per deployment (CHUNK_TARGET_SIZE,
        // CHUNK_MAX_SIZE, CHUNK_MIN_SIZE, CHUNK_OVERLAP_SIZE).
        config.size = SizeConfig::from_env();
        info!(
            "Chunk sizing: target={} max={} min={} overlap={}",
            config.size.target_size,
            config.size.max_chunk_size,
            config.size.min_chunk_size,
            config.size.overlap_size
        );

        // Load Agents Config
        // Agents are REQUIRED in fail-fast mode
        info!("Loading agent configuration (fail-fast mode)");


        config
    }
    
    /// Validate configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.size.max_chunk_size < self.size.min_chunk_size {
            return Err("max_chunk_size must be >= min_chunk_size".to_string());
        }
        
        if self.quality.confidence_threshold < 0.0 || self.quality.confidence_threshold > 1.0 {
            return Err("confidence_threshold must be between 0.0 and 1.0".to_string());
        }
        
        if self.agents.pool_size == 0 {
            return Err("pool_size must be > 0".to_string());
        }
        
        Ok(())
    }
}
/// Core chunking data models and types
use std::collections::HashMap;
use uuid::Uuid;
use chrono::{DateTime, Utc};
use crate::graph::models::{
    ASTData, DocumentData, WebData, ConversationData, SchemaData, TranscriptData
};

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------

/// An entity surface form pre-identified in a chunk by the agentic chunker.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityHint {
    /// The entity text as it appears in the chunk
    pub text: String,
    /// Entity type (organization, person, technology, concept, location, etc.)
    pub entity_type: String,
    /// Confidence score (0.0–1.0) that this is a real named entity
    pub confidence: f32,
    /// Byte offset where entity starts within the chunk content
    pub start_offset: usize,
    /// Byte offset where entity ends within the chunk content
    pub end_offset: usize,
}

/// A single chunk of processed content
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Chunk {
    /// Unique chunk identifier
    pub id: Uuid,
    
    /// Source identifier (repository, file, etc.)
    pub source_id: String,
    
    ///  File path within source
    pub file_path: String,
    
    /// Chunk content (text)
    pub content: String,
    
    /// Type of chunk
    pub chunk_type: ChunkType,
    
    /// Granularity level
    pub level: ChunkLevel,
    
    /// Processing tier applied
    pub tier: ProcessingTier,
    
    /// Confidence score (0.0-1.0)
    pub confidence: f32,
    
    /// Quality score (0.0-1.0, populated after enhancement)
    pub quality: Option<QualityScore>,
    
    /// Chunk metadata
    pub metadata: ChunkMetadata,
    
    /// Vector embeddings (populated by embeddings service)
    pub embeddings: Option<Vec<f32>>,
    
    /// Deterministic chunk key for incremental sync
    pub chunk_key: String,

    /// SHA256 hash of the content
    pub chunk_hash: String,
    
    /// Flag indicating whether the chunk needs re-embedding
    #[serde(default = "default_true")]
    pub is_dirty: bool,
    
    /// Creation timestamp
    pub created_at: DateTime<Utc>,
    
    /// Last enhanced timestamp
    pub enhanced_at: Option<DateTime<Utc>>,
}

fn default_true() -> bool {
    true
}

impl Chunk {
    /// Create new chunk with default tier (Structural)
    pub fn new(
        source_id: String,
        file_path: String,
        content: String,
        chunk_type: ChunkType,
        level: ChunkLevel,
    ) -> Self {
        use sha2::{Sha256, Digest};
        let mut hasher = Sha256::new();
        hasher.update(content.as_bytes());
        let hash_bytes = hasher.finalize();
        let chunk_hash = hash_bytes.iter().map(|b| format!("{:02x}", b)).collect::<String>();
        
        let id = Uuid::new_v4();
        let chunk_key = format!("{}|{}|{}", source_id, file_path, id);
        
        Self {
            id,
            source_id,
            file_path,
            content,
            chunk_type,
            level,
            tier: ProcessingTier::Structural,
            confidence: 0.0,
            quality: None,
            metadata: ChunkMetadata::default(),
            embeddings: None,
            chunk_key,
            chunk_hash,
            is_dirty: true,
            created_at: Utc::now(),
            enhanced_at: None,
        }
    }

    /// Create new chunk with deterministic ID, chunk_key and chunk_hash
    pub fn new_deterministic(
        source_id: String,
        file_path: String,
        content: String,
        chunk_type: ChunkType,
        level: ChunkLevel,
        start_line: usize,
        end_line: usize,
    ) -> Self {
        use sha2::{Sha256, Digest};
        
        // 1. Compute SHA256 hash of content
        let mut hasher = Sha256::new();
        hasher.update(content.as_bytes());
        let hash_bytes = hasher.finalize();
        let chunk_hash = hash_bytes.iter().map(|b| format!("{:02x}", b)).collect::<String>();
        
        // 2. Generate deterministic UUIDv5
        let namespace = Uuid::new_v5(&Uuid::NAMESPACE_URL, format!("{}|{}", source_id, file_path).as_bytes());
        let id = Uuid::new_v5(&namespace, format!("{}-{}", start_line, end_line).as_bytes());
        
        // 3. Generate chunk key
        let chunk_key = format!("{}|{}|{}|{}", source_id, file_path, start_line, end_line);
        
        let mut metadata = ChunkMetadata::default();
        metadata.line_range = Some((start_line, end_line));
        
        Self {
            id,
            source_id,
            file_path,
            content,
            chunk_type,
            level,
            tier: ProcessingTier::Structural,
            confidence: 0.0,
            quality: None,
            metadata,
            embeddings: None,
            chunk_key,
            chunk_hash,
            is_dirty: true,
            created_at: Utc::now(),
            enhanced_at: None,
        }
    }

    /// Create new chunk for stable AST-based chunking
    pub fn new_stable(
        source_id: String,
        file_path: String,
        content: String,
        chunk_type: ChunkType,
        level: ChunkLevel,
        signature: &str,
        chunk_hash: &str,
    ) -> Self {
        // Generate deterministic UUIDv5 using signature and chunk_hash
        let namespace = Uuid::new_v5(&Uuid::NAMESPACE_URL, format!("{}|{}", source_id, file_path).as_bytes());
        let id_input = format!("{}|{}", signature, chunk_hash);
        let id = Uuid::new_v5(&namespace, id_input.as_bytes());
        
        // Generate chunk key based on signature
        let chunk_key = format!("{}|{}|{}", source_id, file_path, signature);
        
        Self {
            id,
            source_id,
            file_path,
            content,
            chunk_type,
            level,
            tier: ProcessingTier::Structural,
            confidence: 0.0,
            quality: None,
            metadata: ChunkMetadata::default(),
            embeddings: None,
            chunk_key,
            chunk_hash: chunk_hash.to_string(),
            is_dirty: true,
            created_at: Utc::now(),
            enhanced_at: None,
        }
    }
    
    /// Set confidence score
    pub fn with_confidence(mut self, confidence: f32) -> Self {
        self.confidence = confidence.clamp(0.0, 1.0);
        self
    }
    
    /// Mark as enhanced (Tier 2)
    pub fn mark_enhanced(mut self, quality: QualityScore) -> Self {
        self.tier = ProcessingTier::Enhanced;
        self.quality = Some(quality);
        self.enhanced_at = Some(Utc::now());
        self
    }
    
    /// Mark with relationships (Tier 3)
    pub fn mark_with_relationships(mut self) -> Self {
        self.tier = ProcessingTier::Relationship;
        self
    }
    
    /// Check if chunk should be enhanced
    pub fn should_enhance(&self) -> bool {
        self.tier == ProcessingTier::Structural && 
        (self.confidence < 0.7 || self.metadata.complexity_score > 7)
    }
}

/// Type of content chunk
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ChunkType {
    /// Code chunks
    Code {
        language: String,
        semantic_type: CodeSemanticType,
    },
    
    /// Document chunks
    Document {
        format: String,
        semantic_type: DocumentSemanticType,
    },
    
    /// Web page chunks (scraped URL content)
    Web {
        url: String,
        semantic_type: WebSemanticType,
    },

    /// Mixed content
    Mixed,
}

/// Semantic type for code chunks
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CodeSemanticType {
    Repository,      // Level 1: Repo overview
    File,            // Level 2: File context
    Function,        // Level 3: Function/method
    Class,           // Level 3: Class/struct
    Module,          // Level 3: Module/package
    CodeBlock,       // Level 4: Micro-chunk
    Comment,         // Documentation
    Import,          // Dependencies
}

/// Semantic type for document chunks
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DocumentSemanticType {
    DocumentOverview,   // Level 1: Document classification
    Section,            // Level 2: Major section
    Subsection,         // Level 2: Subsection
    Paragraph,          // Level 3: Paragraph
    Table,              // Level 4: Table data
    Figure,             // Level 4: Image/diagram
    CodeExample,        // Level 4: Code block
    List,               // Level 3: List
}

/// Semantic type for web page chunks
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WebSemanticType {
    PageOverview,    // Level 1: Full page summary (title + meta + heading outline)
    Section,         // Level 2: Major content section (under an h2/h3)
    Paragraph,       // Level 3: Text paragraph / content block
    Navigation,      // Level 2: Nav structure, breadcrumbs, menu
    Table,           // Level 4: Tabular content
    CodeSnippet,     // Level 4: Code block found on the page
    StyleSheet,      // Level 3: CSS content (inline or linked)
    Script,          // Level 3: JS content (inline or external)
    Metadata,        // Level 1: Page metadata + JSON-LD structured data
    Form,            // Level 3: Form structure
    Media,           // Level 4: Image / video descriptions
}

/// Granularity level of chunk
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum ChunkLevel {
    /// Level 1: Highest level (repo/document overview)
    Overview = 1,
    
    /// Level 2: Structural level (file/section)
    Structural = 2,
    
    /// Level 3: Semantic units (functions/paragraphs)
    Semantic = 3,
    
    /// Level 4: Micro-chunks (blocks/tables)
    Micro = 4,
}

/// Processing tier
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProcessingTier {
    /// Tier 1: Fast structural chunking
    Structural,
    
    /// Tier 2: AI-agent enhanced
    Enhanced,
    
    /// Tier 3: Relationship intelligence applied
    Relationship,
}

/// Chunk metadata
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ChunkMetadata {
    /// Line range in source file
    pub line_range: Option<(usize, usize)>,

    /// Byte range in source file
    pub byte_range: Option<(usize, usize)>,

    /// Complexity score (1-10)
    pub complexity_score: u8,

    /// Token count (approximate)
    pub token_count: usize,

    /// Parent chunk ID (for hierarchical chunks)
    pub parent_id: Option<Uuid>,

    /// Child chunk IDs
    pub child_ids: Vec<Uuid>,

    /// Related chunk IDs (cross-references) — populated by SourceRelationshipRouter
    pub related_ids: Vec<Uuid>,

    /// Pre-identified entity hints from agentic chunker.
    #[serde(default)]
    pub entity_hints: Vec<EntityHint>,

    /// Relationship hints in "subject -> predicate -> object" notation,
    /// detected during LLM enhancement.
    #[serde(default)]
    pub relationship_context: Vec<String>,

    /// AST reference data for relationship extraction.
    /// Populated during chunk creation; used by SourceRelationshipRouter
    /// to map AST entities (function names, class names) to chunk UUIDs.
    #[serde(default)]
    pub ast_data: Option<ASTData>,

    /// Document-specific relationship data.
    #[serde(default)]
    pub document_data: Option<DocumentData>,

    /// Web page specific relationship data.
    #[serde(default)]
    pub web_data: Option<WebData>,

    /// Conversation-specific relationship data.
    #[serde(default)]
    pub conversation_data: Option<ConversationData>,

    /// Schema-specific relationship data.
    #[serde(default)]
    pub schema_data: Option<SchemaData>,

    /// Transcript-specific relationship data.
    #[serde(default)]
    pub transcript_data: Option<TranscriptData>,

    /// Custom key-value metadata
    pub custom: HashMap<String, serde_json::Value>,

    /// Content SHA-256 hash for deduplication (hex-encoded)
    pub content_hash: Option<String>,

    /// Source permissions / ACL — inherited from source system.
    /// Used to filter retrieval results based on requesting user's access.
    pub source_permissions: SourcePermissions,

    /// Provenance chain — list of transformations applied
    pub provenance: Vec<ProvenanceEntry>,
}

/// Access control information inherited from the source system.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SourcePermissions {
    /// Organization / tenant ID (multi-tenancy isolation)
    pub org_id: Option<String>,
    /// User IDs with explicit read access (empty = inherits channel/repo defaults)
    pub allowed_user_ids: Vec<String>,
    /// Role-based access (e.g., "engineering", "sales", "admin")
    pub allowed_roles: Vec<String>,
    /// Whether this content is publicly accessible within the org
    pub is_org_wide: bool,
    /// Source-specific visibility level (e.g., "public", "private", "workspace")
    pub visibility: Option<String>,
}

/// A single step in the provenance chain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProvenanceEntry {
    /// Stage name (e.g., "ingestion", "chunking", "embedding", "entity_extraction")
    pub stage: String,
    /// Service that performed this stage
    pub service: String,
    /// Timestamp of transformation
    pub timestamp: DateTime<Utc>,
    /// Optional details (version, model name, config hash, etc.)
    pub details: Option<String>,
}

/// Quality score from AI agents
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QualityScore {
    /// Overall quality (0.0-1.0)
    pub overall: f32,
    
    /// Boundary clarity (0.0-1.0)
    pub boundary_clarity: f32,
    
    /// Contextual completeness (0.0-1.0)
    pub contextual_completeness: f32,
    
    /// Semantic coherence (0.0-1.0)
    pub semantic_coherence: f32,
    
    /// Agent that scored this
    pub scored_by: String,
    
    /// Timestamp of scoring
    pub scored_at: DateTime<Utc>,
}

/// Result of chunking operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkingResult {
    /// Generated chunks
    pub chunks: Vec<Chunk>,
    
    /// Processing statistics
    pub stats: ChunkingStats,
    
    /// Errors encountered
    pub errors: Vec<String>,
}

impl ChunkingResult {
    pub fn new(chunks: Vec<Chunk>) -> Self {
        let stats = ChunkingStats {
            total_chunks: chunks.len(),
            avg_size: chunks.iter().map(|c| c.content.len()).sum::<usize>() / chunks.len().max(1),
            avg_confidence: chunks.iter().map(|c| c.confidence).sum::<f32>() / chunks.len() as f32,
            processing_time_ms: 0,
        };
        
        Self {
            chunks,
            stats,
            errors: Vec::new(),
        }
    }
    
    pub fn with_stats(mut self, stats: ChunkingStats) -> Self {
        self.stats = stats;
        self
    }
}

/// Chunking statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkingStats {
    pub total_chunks: usize,
    pub avg_size: usize,
    pub avg_confidence: f32,
    pub processing_time_ms: u64,
}
