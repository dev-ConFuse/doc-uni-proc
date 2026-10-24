//! Unified, density-aware chunking strategy inspired by Graphiti.
//!
//! Splitting content dynamically at boundaries (paragraphs, sentences, JSON objects) 
//! while strictly adhering to token budgets.

use crate::core::chunking::{
    Chunk, ChunkingConfig, ChunkingResult, ChunkType, ChunkLevel, DocumentSemanticType, CodeSemanticType, WebSemanticType
};
use crate::core::chunking::ChunkingStrategy;
use async_trait::async_trait;
use tracing::{info};

/// A robust token and density aware chunker that cleanly replaces the 13 legacy chunking strategies.
pub struct HybridChunker;

impl Default for HybridChunker {
    fn default() -> Self {
        Self::new()
    }
}

impl HybridChunker {
    pub fn new() -> Self {
        Self
    }
    
    fn determine_chunk_type(&self, filename: &str) -> ChunkType {
        let lower = filename.to_lowercase();
        let ext = std::path::Path::new(&lower).extension().and_then(|e| e.to_str()).unwrap_or("");

        // Map file extensions to the language string that tree-sitter and CodeExtractor expect.
        let language = match ext {
            "rs" => Some("rust"),
            "py" => Some("python"),
            "js" | "jsx" | "mjs" | "cjs" => Some("javascript"),
            "ts" | "tsx" | "mts" | "cts" => Some("typescript"),
            "java" => Some("java"),
            "go" => Some("go"),
            "cs" => Some("c_sharp"),
            "c" => Some("c"),
            "h" | "hpp" | "hxx" => Some("cpp"),   // headers treated as C++ for tree-sitter
            "cpp" | "cc" | "cxx" => Some("cpp"),
            "rb" => Some("ruby"),
            "php" => Some("php"),
            "swift" => Some("swift"),
            "kt" | "kts" => Some("kotlin"),
            "scala" | "sc" => Some("scala"),
            "sh" | "bash" | "zsh" => Some("bash"),
            "html" | "htm" => Some("html"),
            "css" | "scss" | "less" => Some("css"),
            "md" | "markdown" => Some("markdown"),
            _ => None,
        };

        if let Some(lang) = language {
            ChunkType::Code {
                language: lang.to_string(),
                semantic_type: CodeSemanticType::File,
            }
        } else if lower.starts_with("http") {
            ChunkType::Web {
                url: filename.to_string(),
                semantic_type: WebSemanticType::PageOverview,
            }
        } else {
            ChunkType::Document {
                format: ext.to_string(),
                semantic_type: DocumentSemanticType::DocumentOverview,
            }
        }
    }
    
    fn chunk_content(&self, content: &str, config: &ChunkingConfig) -> Vec<String> {
        let max_chars = config.size.max_chunk_size;
        let overlap_chars = config.size.overlap_size;
        
        if content.len() <= max_chars {
            return vec![content.to_string()];
        }
        
        // Simplistic JSON check
        if content.trim_start().starts_with('{') || content.trim_start().starts_with('[') {
            if let Ok(val) = serde_json::from_str::<serde_json::Value>(content) {
                return self.chunk_json(&val, max_chars, overlap_chars);
            }
        }
        
        // Pack to the soft target so chunks stay small and retrieval-focused;
        // the hard maximum is only used to break up oversized atomic units.
        self.chunk_text(content, config.size.target_size, max_chars, overlap_chars)
    }

    fn chunk_json(&self, val: &serde_json::Value, max_chars: usize, _overlap_chars: usize) -> Vec<String> {
        let mut chunks = Vec::new();
        match val {
            serde_json::Value::Array(arr) => {
                let mut current_chunk = Vec::new();
                let mut current_size = 2; // "[]"
                for item in arr {
                    let item_str = serde_json::to_string(item).unwrap_or_default();
                    let item_size = item_str.len() + 2;
                    if !current_chunk.is_empty() && current_size + item_size > max_chars {
                        chunks.push(serde_json::to_string(&current_chunk).unwrap_or_default());
                        current_chunk.clear();
                        current_size = 2;
                    }
                    current_chunk.push(item);
                    current_size += item_size;
                }
                if !current_chunk.is_empty() {
                    chunks.push(serde_json::to_string(&current_chunk).unwrap_or_default());
                }
            },
            serde_json::Value::Object(obj) => {
                let mut current_chunk = serde_json::Map::new();
                let mut current_size = 2; // "{}"
                for (k, v) in obj {
                    let entry_str = serde_json::to_string(&serde_json::json!({k: v})).unwrap_or_default();
                    let entry_size = entry_str.len();
                    if !current_chunk.is_empty() && current_size + entry_size > max_chars {
                        chunks.push(serde_json::to_string(&current_chunk).unwrap_or_default());
                        current_chunk.clear();
                        current_size = 2;
                    }
                    current_chunk.insert(k.clone(), v.clone());
                    current_size += entry_size;
                }
                if !current_chunk.is_empty() {
                    chunks.push(serde_json::to_string(&current_chunk).unwrap_or_default());
                }
            },
            _ => chunks.push(serde_json::to_string(val).unwrap_or_default()),
        }
        
        if chunks.is_empty() {
            vec!["{}".to_string()]
        } else {
            chunks
        }
    }
    
    fn chunk_text(&self, content: &str, pack_chars: usize, max_chars: usize, overlap_chars: usize) -> Vec<String> {
        let mut chunks = Vec::new();
        let mut current_chunk = String::new();

        // Split on line boundaries rather than blank lines. Extracted document
        // text (PDF, HTML) rarely contains blank lines, so splitting on "\n\n"
        // collapses a whole document into one oversized "paragraph" that is then
        // hard-cut at max_chars — producing few, oversized, multi-topic chunks.
        // Lines keep table rows, list items and code lines intact.
        let paragraphs: Vec<&str> = content.split('\n').collect();

        for paragraph in paragraphs {
            let p = paragraph.trim();
            if p.is_empty() {
                continue;
            }

            // Fallback to sentences if the line is incredibly long
            let pieces = if p.len() > max_chars {
                p.split(". ").collect::<Vec<_>>()
            } else {
                vec![p]
            };

            for (i, piece) in pieces.iter().enumerate() {
                let mut text = piece.to_string();
                if pieces.len() > 1 && i < pieces.len() - 1 {
                    text.push_str(". ");
                } else if pieces.len() == 1 {
                    text.push('\n');
                }

                if current_chunk.len() + text.len() > pack_chars && !current_chunk.is_empty() {
                    chunks.push(current_chunk.trim().to_string());
                    
                    // Add overlap
                    let mut overlap_text = String::new();
                    if overlap_chars > 0 && current_chunk.len() > overlap_chars {
                        let words: Vec<&str> = current_chunk.split_whitespace().collect();
                        let mut overlap_len = 0;
                        for w in words.iter().rev() {
                            if overlap_len + w.len() + 1 > overlap_chars { break; }
                            overlap_text.insert_str(0, &format!("{} ", w));
                            overlap_len += w.len() + 1;
                        }
                    }
                    current_chunk = overlap_text;
                }
                
                // If single piece is STILL larger than max_chars, split by words
                if text.len() > max_chars {
                    let words: Vec<&str> = text.split_whitespace().collect();
                    for word in words {
                        if word.len() > max_chars {
                            if !current_chunk.is_empty() {
                                chunks.push(current_chunk.trim().to_string());
                                current_chunk.clear();
                            }
                            let chars: Vec<char> = word.chars().collect();
                            for chunk_slice in chars.chunks(max_chars) {
                                let block: String = chunk_slice.iter().collect();
                                chunks.push(block);
                            }
                        } else {
                            if current_chunk.len() + word.len() + 1 > pack_chars && !current_chunk.is_empty() {
                                chunks.push(current_chunk.trim().to_string());
                                // Add overlap
                                let mut overlap_text = String::new();
                                if overlap_chars > 0 && current_chunk.len() > overlap_chars {
                                    let w_list: Vec<&str> = current_chunk.split_whitespace().collect();
                                    let mut overlap_len = 0;
                                    for w in w_list.iter().rev() {
                                        if overlap_len + w.len() + 1 > overlap_chars { break; }
                                        overlap_text.insert_str(0, &format!("{} ", w));
                                        overlap_len += w.len() + 1;
                                    }
                                }
                                current_chunk = overlap_text;
                            }
                            current_chunk.push_str(word);
                            current_chunk.push(' ');
                        }
                    }
                } else {
                    current_chunk.push_str(&text);
                }
            }
        }
        
        if !current_chunk.trim().is_empty() {
            chunks.push(current_chunk.trim().to_string());
        }
        
        if chunks.is_empty() && !content.trim().is_empty() {
            chunks.push(content.trim().to_string());
        }
        
        chunks
    }
}

#[async_trait]
impl ChunkingStrategy for HybridChunker {
    async fn process(
        &self,
        content: &str,
        filename: &str,
        source_id: &str,
        config: &ChunkingConfig,
    ) -> anyhow::Result<ChunkingResult> {
        let start = std::time::Instant::now();
        let chunk_type = self.determine_chunk_type(filename);
        
        let mut final_chunks = Vec::new();

        
        let raw_chunks = self.chunk_content(content, config);
        let _total_chunks = raw_chunks.len();
        
        let _type_desc = match &chunk_type {
            ChunkType::Code { language, .. } => format!("Language: {}", language),
            ChunkType::Document { format, .. } => format!("Format: {}", format),
            ChunkType::Web { url, .. } => format!("URL: {}", url),
            _ => "Type: Unknown".to_string(),
        };
        
        for (_i, c_text) in raw_chunks.into_iter().enumerate() {
            let enriched_content = c_text.clone();

            let start_byte = content.find(&c_text).unwrap_or(0);
            let end_byte = start_byte + c_text.len();

            let chunk = Chunk::new_deterministic(
                source_id.to_string(),
                filename.to_string(),
                enriched_content,
                chunk_type.clone(),
                ChunkLevel::Structural,
                start_byte,
                end_byte,
            ).with_confidence(0.9);
            
            final_chunks.push(chunk);
        }
        
        let elapsed = start.elapsed();
        let stats = crate::core::chunking::ChunkingStats {
            total_chunks: final_chunks.len(),
            avg_size: if final_chunks.is_empty() { 0 } else { final_chunks.iter().map(|c| c.content.len()).sum::<usize>() / final_chunks.len() },
            avg_confidence: 0.9,
            processing_time_ms: elapsed.as_millis() as u64,
        };

        info!(
            "Hybrid chunking complete: {} chunks extracted for {} in {:?}",
            final_chunks.len(),
            filename,
            elapsed
        );

        Ok(ChunkingResult {
            chunks: final_chunks,
            stats,
            errors: Vec::new(),
        })
    }

    fn name(&self) -> &str {
        "hybrid"
    }

    fn supports(&self, _filename: &str) -> bool {
        true
    }
}
