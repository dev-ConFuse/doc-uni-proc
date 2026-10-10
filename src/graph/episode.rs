//! Graphify Episode — Canonical Data Contract (Rust)
//!
//! Defines the standardised episode schema that all ConFuse Rust services use
//! when emitting processed data to Graphify. This mirrors the Python model in
//! `confuse_common.events.episode` for cross-language consistency.
//!
//! Episode = chunk + metadata + embedding (optional at emission time).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// ── Source type taxonomy ─────────────────────────────────────────────────────

/// Content source categories for Graphify episodes.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum EpisodeSourceType {
    Codebase,
    Document,
    Web,
    Video,
    Audio,
    Image,
    Chat,
    Message,
    Unknown,
}

impl std::fmt::Display for EpisodeSourceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Codebase => write!(f, "codebase"),
            Self::Document => write!(f, "document"),
            Self::Web => write!(f, "web"),
            Self::Video => write!(f, "video"),
            Self::Audio => write!(f, "audio"),
            Self::Image => write!(f, "image"),
            Self::Chat => write!(f, "chat"),
            Self::Message => write!(f, "message"),
            Self::Unknown => write!(f, "unknown"),
        }
    }
}

/// Granularity / semantic type of the chunk.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum EpisodeChunkType {
    // Codebase types
    Function,
    Class,
    Module,
    Import,
    CodeBlock,
    Docstring,
    // Document types
    Section,
    Paragraph,
    Sentence,
    Table,
    Citation,
    // Web types
    PageOverview,
    WebSection,
    WebMetadata,
    // Media types
    VideoSegment,
    AudioSegment,
    Keyframe,
    Transcript,
    // Chat types
    Thread,
    MessageTurn,
    // Generic
    Mixed,
    #[default]
    Raw,
}


// ── Sub-models ───────────────────────────────────────────────────────────────

/// Content-type-specific metadata attached to every episode.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct EpisodeMetadata {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub author: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,
    // Document-specific
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page_number: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub heading: Option<String>,
    // Code-specific
    #[serde(skip_serializing_if = "Option::is_none")]
    pub symbol_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub line_start: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub line_end: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub complexity: Option<u32>,
    // Media-specific
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub width: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub height: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exif: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transcript: Option<String>,
    // Chat-specific
    #[serde(skip_serializing_if = "Option::is_none")]
    pub participant: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    // Entity hints for downstream graph construction
    #[serde(default)]
    pub entity_hints: Vec<serde_json::Value>,
    // Relationship hints for downstream graph construction
    #[serde(default)]
    pub relationship_hints: Vec<serde_json::Value>,
    // Arbitrary key-value pairs for extensibility
    #[serde(default)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

/// Provenance tracking — which service produced this episode, when, and how.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EpisodeProvenance {
    /// Service that created this episode (e.g. "data-connector", "unified-processor").
    pub origin_service: String,
    /// Schema version for forward/backward compatibility.
    #[serde(default = "default_version")]
    pub version: String,
    /// ID of the pipeline run that created this episode.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pipeline_run_id: Option<String>,
    /// Timestamp when this episode was created.
    #[serde(default = "Utc::now")]
    pub created_at: DateTime<Utc>,
    /// ID of the parent episode if this is a derived/split episode.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_episode_id: Option<String>,
}

fn default_version() -> String {
    "1.0.0".to_string()
}

impl Default for EpisodeProvenance {
    fn default() -> Self {
        Self {
            origin_service: "unknown".to_string(),
            version: default_version(),
            pipeline_run_id: None,
            created_at: Utc::now(),
            parent_episode_id: None,
        }
    }
}

// ── Canonical Episode ────────────────────────────────────────────────────────

/// Canonical episode contract for Graphify ingestion.
///
/// This is the standardised handoff unit used across all ConFuse services:
/// - data-connector → Kafka → Graphify
/// - embeddings-service → Kafka → Graphify
/// - unified-processor → Graphify (direct or Kafka)
///
/// Fields are designed to be content-type-agnostic while supporting
/// rich metadata for every supported source type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphifyEpisode {
    // ── Identity ─────────────────────────────────────────────────────────
    /// Globally unique episode identifier.
    pub id: String,
    /// Content source category.
    pub source_type: EpisodeSourceType,
    /// Identifier of the source (repo ID, document ID, URL, etc.).
    pub source_id: String,

    // ── Content ──────────────────────────────────────────────────────────
    /// The actual text content of this episode chunk.
    pub chunk_text: String,
    /// Semantic type / granularity of this chunk.
    #[serde(default)]
    pub chunk_type: EpisodeChunkType,

    // ── Embedding (populated by embeddings-service) ──────────────────────
    /// Float vector embedding — populated after embedding generation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub embedding: Option<Vec<f32>>,
    /// Name of the model used to generate the embedding.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub embedding_model: Option<String>,
    /// Dimensionality of the embedding vector.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub embedding_dimension: Option<u32>,

    // ── Context ──────────────────────────────────────────────────────────
    /// Programming or natural language of the content.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,
    /// File path, URL, or location within the source.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_path: Option<String>,
    /// Logical grouping / tenant ID for graph partitioning.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group_id: Option<String>,

    // ── Timestamps ───────────────────────────────────────────────────────
    /// Start time for temporal content (video/audio segments).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_timestamp: Option<DateTime<Utc>>,
    /// End time for temporal content (video/audio segments).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_timestamp: Option<DateTime<Utc>>,

    // ── Rich metadata ────────────────────────────────────────────────────
    /// Content-type-specific metadata.
    #[serde(default)]
    pub metadata: EpisodeMetadata,

    // ── Provenance ───────────────────────────────────────────────────────
    /// Origin and version tracking.
    pub provenance: EpisodeProvenance,

    // ── Security / Governance ────────────────────────────────────────────
    /// Whether PII was detected in this episode's content.
    #[serde(default)]
    pub pii_detected: bool,
    /// Access control tags for graph query authorization.
    #[serde(default)]
    pub acl_tags: Vec<String>,
}

impl GraphifyEpisode {
    /// Create a new episode with sensible defaults.
    pub fn new(
        source_type: EpisodeSourceType,
        source_id: String,
        chunk_text: String,
        origin_service: &str,
    ) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            source_type,
            source_id,
            chunk_text,
            chunk_type: EpisodeChunkType::default(),
            embedding: None,
            embedding_model: None,
            embedding_dimension: None,
            language: None,
            content_path: None,
            group_id: None,
            start_timestamp: None,
            end_timestamp: None,
            metadata: EpisodeMetadata::default(),
            provenance: EpisodeProvenance {
                origin_service: origin_service.to_string(),
                version: default_version(),
                pipeline_run_id: None,
                created_at: Utc::now(),
                parent_episode_id: None,
            },
            pii_detected: false,
            acl_tags: vec![],
        }
    }

    /// Builder method: set chunk type.
    pub fn with_chunk_type(mut self, chunk_type: EpisodeChunkType) -> Self {
        self.chunk_type = chunk_type;
        self
    }

    /// Builder method: set language.
    pub fn with_language(mut self, language: impl Into<String>) -> Self {
        self.language = Some(language.into());
        self
    }

    /// Builder method: set content path.
    pub fn with_content_path(mut self, path: impl Into<String>) -> Self {
        self.content_path = Some(path.into());
        self
    }

    /// Builder method: set group ID.
    pub fn with_group_id(mut self, group_id: impl Into<String>) -> Self {
        self.group_id = Some(group_id.into());
        self
    }

    /// Builder method: attach embedding data.
    pub fn with_embedding(mut self, embedding: Vec<f32>, model: &str) -> Self {
        self.embedding_dimension = Some(embedding.len() as u32);
        self.embedding = Some(embedding);
        self.embedding_model = Some(model.to_string());
        self
    }

    /// Serialize to a JSON string payload.
    pub fn to_json_payload(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    /// Deserialize from a JSON string payload.
    pub fn from_json_payload(payload: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(payload)
    }
}


