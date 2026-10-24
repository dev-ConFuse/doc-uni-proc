//! Advanced document parser (invokes python pipeline)

use crate::core::Result;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::path::Path;


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineOutput {
    pub title: String,
    #[serde(default)]
    pub sections: Vec<PipelineSection>,
    #[serde(default)]
    pub tables: Vec<PipelineTable>,
    #[serde(default)]
    pub metadata: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineSection {
    #[serde(default)]
    pub heading: String,
    #[serde(default = "default_level")]
    pub level: u8,
    #[serde(default)]
    pub content: String,
}

fn default_level() -> u8 { 1 }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineTable {
    #[serde(default)]
    pub caption: String,
    #[serde(default)]
    pub markdown: String,
}

pub struct DocumentParser {
    nim_ocr: Option<crate::processors::preprocessors::ocr::NvidiaNimOcr>,
}

impl DocumentParser {
    pub fn new() -> Result<Self> {
        let nim_ocr = if let Some(nim_config) = crate::processors::preprocessors::ocr::NimOcrConfig::from_env() {
            tracing::info!("Initializing NVIDIA NIM OCR service endpoint: {}", nim_config.endpoint);
            match crate::processors::preprocessors::ocr::NvidiaNimOcr::new(nim_config) {
                Ok(ocr) => Some(ocr),
                Err(e) => {
                    tracing::error!("Failed to initialize NVIDIA NIM OCR client: {}", e);
                    None
                }
            }
        } else {
            tracing::info!("NVIDIA NIM OCR is not configured (NVIDIA_NIM_ENDPOINT not set)");
            None
        };

        Ok(Self { nim_ocr })
    }

    pub async fn process_document_file(&self, file_path: &str) -> Result<PipelineOutput> {
        self.process_document_file_with_options(file_path, false).await
    }

    pub async fn process_document_file_with_options(&self, file_path: &str, force_lightweight: bool) -> Result<PipelineOutput> {
        let extension = self.detect_document_type(file_path);
        
        // Priority 1: NVIDIA NIM Vision-Language Model OCR Offloader (Fast, Low Power)
        if !force_lightweight {
            if let Some(ref nim_ocr) = self.nim_ocr {
                tracing::info!("Offloading document OCR/parsing to NVIDIA NIM for {}", file_path);
                match nim_ocr.process_document(file_path).await {
                    Ok(output) => {
                        if self.is_output_sufficient(&output) {
                            tracing::info!("NVIDIA NIM OCR output verified sufficient for {}", file_path);
                            return Ok(output);
                        }
                        tracing::info!("NIM OCR output insufficient for {}, falling back to native Rust extractor", file_path);
                    }
                    Err(e) => {
                        tracing::warn!("NVIDIA NIM OCR parsing failed for {}: {}, falling back to native Rust extractor", file_path, e);
                    }
                }
            }
        }
        
        // Priority 2: Native Rust extraction (Fast & zero-overhead fallback)
        let content = if extension == "pdf" {
            let path = file_path.to_string();
            tokio::task::spawn_blocking(move || {
                match pdf_extract::extract_text(&path) {
                    Ok(text) => text,
                    Err(e) => {
                        tracing::warn!("Failed to extract PDF text from {}: {}", path, e);
                        "Binary or unreadable file".to_string()
                    }
                }
            }).await.unwrap_or_else(|_| "Binary or unreadable file".to_string())
        } else {
            tokio::fs::read_to_string(file_path).await.unwrap_or_else(|_| "Binary or unreadable file".to_string())
        };

        let title = std::path::Path::new(file_path).file_stem().and_then(|s| s.to_str()).unwrap_or("Document").to_string();

        let parsed = PipelineOutput {
            title,
            sections: vec![PipelineSection {
                heading: "Content".to_string(),
                level: 1,
                content,
            }],
            tables: vec![],
            metadata: std::collections::HashMap::new(),
        };

        Ok(parsed)
    }

    /// Assess if the OCR/parsed output quality is sufficient
    pub fn is_output_sufficient(&self, output: &PipelineOutput) -> bool {
        let total_content_len: usize = output.sections.iter().map(|s| s.content.len()).sum();
        
        // If content is completely missing or negligible, quality is insufficient
        if total_content_len < 50 && output.tables.is_empty() {
            return false;
        }

        // If document appears to have tables but no tables were extracted, check heuristic
        let combined_text: String = output.sections.iter().map(|s| s.content.as_str()).collect::<Vec<_>>().join(" ");
        if output.tables.is_empty() && self.document_likely_has_tables(&combined_text) {
            return false;
        }

        true
    }

    /// Heuristic to detect if text likely contains markdown or formatted tables
    pub fn document_likely_has_tables(&self, content: &str) -> bool {
        let has_pipes = content.lines().filter(|l| l.split('|').count() > 3).count() > 3;
        let has_table_keyword = content.contains("TABLE ") || content.contains("Table ") || content.contains("table:");
        has_pipes || (has_table_keyword && content.lines().filter(|l| l.split_whitespace().count() >= 4).count() > 5)
    }

    /// Detect document type by extension
    pub fn detect_document_type(&self, filename: &str) -> String {
        let extension = Path::new(filename)
            .extension()
            .and_then(|ext| ext.to_str())
            .unwrap_or("")
            .to_lowercase();

        match extension.as_str() {
            "pdf" => "pdf".to_string(),
            "docx" => "docx".to_string(),
            "doc" => "doc".to_string(),
            "html" | "htm" => "html".to_string(),
            "md" | "markdown" => "markdown".to_string(),
            "txt" => "text".to_string(),
            "rtf" => "rtf".to_string(),
            _ => "unknown".to_string(),
        }
    }
}

/// Maximum characters of body text per document chunk.
///
/// Tuned for retrieval precision rather than throughput: a chunk should carry
/// roughly one idea so its embedding stays focused and a narrow query can match
/// it at high similarity. The previous 3000-char setting packed ~750 tokens
/// into one node, which blurs the embedding and makes a chunk match many
/// different queries only moderately well. Overridable via DOC_CHUNK_MAX_CHARS.
fn max_section_chunk_chars() -> usize {
    std::env::var("DOC_CHUNK_MAX_CHARS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(600)
}

/// Cap for the whole-document fallback node.
///
/// This node re-embeds document body text, so a generous limit recreated the
/// entire document as one giant Vector_Chunk. That blob matched almost any
/// query at medium similarity and crowded the precise section chunks out of the
/// top-k results. Overridable via DOC_OVERVIEW_MAX_CHARS.
fn max_full_doc_chars() -> usize {
    std::env::var("DOC_OVERVIEW_MAX_CHARS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1500)
}

pub fn build_document_chunks(
    parsed: &PipelineOutput,
    filename: &str,
    source_id: &str,
) -> Vec<crate::core::chunking::Chunk> {
    use crate::core::chunking::{Chunk, ChunkType, ChunkLevel, DocumentSemanticType};

    let ext = std::path::Path::new(filename)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_lowercase();

    let mut chunks: Vec<Chunk> = Vec::new();

    // 1. Document overview chunk
    let heading_outline: Vec<String> = parsed.sections.iter()
        .filter(|s| !s.heading.is_empty())
        .map(|s| format!("{} {}", "#".repeat(s.level as usize), s.heading))
        .collect();

    let _page_count = parsed.metadata.get("page_count")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let _word_count = parsed.metadata.get("word_count")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);

    let overview_text = format!(
        "# {}\n\n## Document Structure\n{}\n\n---\n\n",
        parsed.title,
        heading_outline.join("\n"),
    );

    // Track whether we've prepended the overview metadata to the first chunk
    let mut overview_prepended = false;

    // 1. Section chunks
    for section in &parsed.sections {
        if section.content.trim().is_empty() {
            continue;
        }

        let mut section_header = if section.heading.is_empty() {
            String::new()
        } else {
            format!("{} {}\n\n", "#".repeat(section.level as usize), section.heading)
        };
        
        if !overview_prepended {
            section_header = format!("{}{}", overview_text, section_header);
            overview_prepended = true;
        }

        // If the section is small enough, keep it as one chunk
        if section.content.len() <= max_section_chunk_chars() {
            let content = format!("{}{}", section_header, section.content);
            let chunk = Chunk::new(
                source_id.to_string(),
                filename.to_string(),
                content,
                ChunkType::Document {
                    format: ext.clone(),
                    semantic_type: DocumentSemanticType::Section,
                },
                ChunkLevel::Structural,
            ).with_confidence(0.95);
            chunks.push(chunk);
        } else {
            // Split long sections with an algorithm that balances chunk sizes
            fn chunk_text(text: &str, max_len: usize) -> Vec<String> {
                let text_len = text.len();
                if text_len <= max_len {
                    return vec![text.to_string()];
                }
                
                // Calculate ideal chunk size to ensure chunks are evenly sized
                let num_chunks = std::cmp::max(1, (text_len + max_len - 1) / max_len);
                let target_len = text_len / num_chunks;
                
                // Add a small tolerance so we don't split unnaturally
                let split_threshold = std::cmp::min(target_len + (target_len as f64 * 0.15) as usize, max_len);
                
                let mut results = Vec::new();
                let mut current = String::new();
                
                for p in text.split("\n\n") {
                    if p.len() <= max_len {
                        let expected_len = current.len() + p.len() + 2;
                        if expected_len > split_threshold && !current.is_empty() {
                            results.push(current.clone());
                            current = p.to_string();
                        } else {
                            if !current.is_empty() { current.push_str("\n\n"); }
                            current.push_str(p);
                        }
                    } else {
                        // For paragraphs larger than max_len, recursively split by '\n' and then chars
                        for l in p.split('\n') {
                            if l.len() <= max_len {
                                let expected_len = current.len() + l.len() + 1;
                                if expected_len > split_threshold && !current.is_empty() {
                                    results.push(current.clone());
                                    current = l.to_string();
                                } else {
                                    if !current.is_empty() { current.push('\n'); }
                                    current.push_str(l);
                                }
                            } else {
                                let mut chars = l.chars().peekable();
                                while chars.peek().is_some() {
                                    let chunk_part: String = chars.by_ref().take(max_len).collect();
                                    if current.len() + chunk_part.len() > split_threshold && !current.is_empty() {
                                        results.push(current.clone());
                                        current = chunk_part;
                                    } else {
                                        current.push_str(&chunk_part);
                                    }
                                }
                            }
                        }
                    }
                }
                if !current.is_empty() {
                    results.push(current);
                }
                results
            }

            let text_chunks = chunk_text(&section.content, max_section_chunk_chars());
            
            for (i, t_chunk) in text_chunks.iter().enumerate() {
                let final_text = if i == 0 {
                    format!("{}{}", section_header, t_chunk)
                } else if !section.heading.is_empty() {
                    format!("[cont. {}]\n\n{}", section.heading, t_chunk)
                } else {
                    t_chunk.to_string()
                };
                
                let chunk = Chunk::new(
                    source_id.to_string(),
                    filename.to_string(),
                    final_text.trim().to_string(),
                    ChunkType::Document {
                        format: ext.clone(),
                        semantic_type: if i == 0 {
                            DocumentSemanticType::Section
                        } else {
                            DocumentSemanticType::Paragraph
                        },
                    },
                    ChunkLevel::Structural,
                ).with_confidence(0.90);
                chunks.push(chunk);
            }
        }
    }

    // 3. Table chunks
    for table in &parsed.tables {
        if table.markdown.trim().is_empty() {
            continue;
        }

        let content = if !table.caption.is_empty() {
            format!("Table: {}\n\n{}", table.caption, table.markdown)
        } else {
            table.markdown.clone()
        };

        let chunk = Chunk::new(
            source_id.to_string(),
            filename.to_string(),
            content,
            ChunkType::Document {
                format: ext.clone(),
                semantic_type: DocumentSemanticType::Table,
            },
            ChunkLevel::Micro,
        ).with_confidence(0.95);
        
        chunks.push(chunk);
    }

    // 4. Full-document chunk (store as large-context fallback)
    // Build a large contiguous representation of the document to provide
    // richer retrieval context. Truncate to `max_full_doc_chars()` to avoid
    // creating excessively large nodes.
    let mut full_text = String::new();
    for section in &parsed.sections {
        if !section.heading.is_empty() {
            full_text.push_str(&format!("{} {}\n\n", "#".repeat(section.level as usize), section.heading));
        }
        if !section.content.trim().is_empty() {
            full_text.push_str(&section.content);
            full_text.push_str("\n\n");
        }
    }

    // Append tables as text as well
    for table in &parsed.tables {
        if !table.markdown.trim().is_empty() {
            if !table.caption.is_empty() {
                full_text.push_str(&format!("Table: {}\n\n", table.caption));
            }
            full_text.push_str(&table.markdown);
            full_text.push_str("\n\n");
        }
    }

    if !full_text.trim().is_empty() {
        let limit = max_full_doc_chars();
        let truncated = if full_text.len() > limit {
            // Truncate at a paragraph boundary near the limit. Slicing with
            // `full_text[..limit]` panics when the limit lands mid-UTF-8
            // character (this document contains multi-byte bullets), so back up
            // to a valid char boundary first.
            let mut end = limit;
            while end > 0 && !full_text.is_char_boundary(end) {
                end -= 1;
            }
            let mut t = full_text[..end].to_string();
            if let Some(idx) = t.rfind("\n\n") {
                t.truncate(idx);
            }
            t
        } else {
            full_text
        };

        let mut full_chunk = Chunk::new(
            source_id.to_string(),
            filename.to_string(),
            truncated,
            ChunkType::Document {
                format: ext.clone(),
                semantic_type: DocumentSemanticType::DocumentOverview,
            },
            ChunkLevel::Overview,
        ).with_confidence(0.85);

        // Mark this chunk as the full document fallback
        full_chunk.metadata.custom.insert("full_document".to_string(), json!(true));
        chunks.push(full_chunk);
    }

    chunks
}
