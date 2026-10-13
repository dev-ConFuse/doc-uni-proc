//! NVIDIA NIM Vision-Language Model service for document OCR and structure extraction.
//!
//! Offloads OCR and heavy document processing to an NVIDIA NIM endpoint
//! serving a Vision-Language Model (VLM). The client reads document files,
//! encodes them as base64 data URIs, sends them to the NIM
//! `/v1/chat/completions` endpoint with a structured extraction prompt,
//! and parses the model's Markdown response back into `PipelineOutput`.
//!
//! The model identifier is configured with `NVIDIA_NIM_MODEL`.

use crate::core::Result;
use crate::processors::parser::{PipelineOutput, PipelineSection, PipelineTable};
use base64::Engine;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::path::Path;

// ───────────────────────────── Configuration ─────────────────────────────

/// Configuration for the NVIDIA NIM endpoint, populated from environment
/// variables at startup.
#[derive(Debug, Clone)]
pub struct NimOcrConfig {
    /// Base URL of the NIM inference endpoint, e.g. `http://localhost:8000/v1`
    /// or `https://integrate.api.nvidia.com/v1`.
    pub endpoint: String,
    /// API key / bearer token (required for cloud catalog, optional self-hosted).
    pub api_key: Option<String>,
    /// Model identifier configured for the NIM endpoint.
    pub model: String,
    /// Maximum concurrent page-batch requests.
    pub batch_size: usize,
    /// Per-request timeout in seconds.
    pub timeout_secs: u64,
    /// Maximum tokens the model may generate per page request.
    pub max_tokens: u32,
}

impl NimOcrConfig {
    /// Build config from environment variables. Returns `None` when the
    /// feature is not configured (i.e. `NVIDIA_NIM_ENDPOINT` is unset).
    /// When enabled, all companion vars are required (no in-code defaults —
    /// see `.cline/rules/env-policy.md`).
    pub fn from_env() -> Option<Self> {
        let endpoint = std::env::var("NVIDIA_NIM_ENDPOINT").ok()?;
        let endpoint = endpoint.trim_end_matches('/').to_string();

        fn required(name: &str) -> String {
            std::env::var(name)
                .unwrap_or_else(|_| panic!("Missing required environment variable: {name}"))
        }
        fn required_parse<T: std::str::FromStr>(name: &str) -> T {
            required(name)
                .parse()
                .unwrap_or_else(|_| panic!("Invalid value for environment variable: {name}"))
        }

        Some(Self {
            endpoint,
            api_key: std::env::var("NVIDIA_NIM_API_KEY").ok(),
            model: required("NVIDIA_NIM_MODEL"),
            batch_size: required_parse("NIM_BATCH_SIZE"),
            timeout_secs: required_parse("NIM_TIMEOUT_SECS"),
            max_tokens: required_parse("NIM_MAX_TOKENS"),
        })
    }
}

// ───────────────────── OpenAI-compatible request/response ────────────────

#[derive(Serialize)]
struct ChatRequest {
    model: String,
    messages: Vec<ChatMessage>,
    max_tokens: u32,
    temperature: f32,
}

#[derive(Serialize)]
struct ChatMessage {
    role: String,
    content: Vec<ContentPart>,
}

#[derive(Serialize)]
#[serde(tag = "type")]
enum ContentPart {
    #[serde(rename = "text")]
    Text { text: String },
    #[serde(rename = "image_url")]
    ImageUrl { image_url: ImageUrl },
}

#[derive(Serialize)]
struct ImageUrl {
    url: String,
}

#[derive(Deserialize)]
struct ChatResponse {
    choices: Vec<ChatChoice>,
}

#[derive(Deserialize)]
struct ChatChoice {
    message: ChatChoiceMessage,
}

#[derive(Deserialize)]
struct ChatChoiceMessage {
    content: String,
}

// ────────────────────────── NVIDIA NIM OCR Service ────────────────────────

/// Async service that sends documents to an NVIDIA NIM VLM endpoint for
/// OCR and structural extraction, replacing local Docling/Tesseract.
pub struct NvidiaNimOcr {
    config: NimOcrConfig,
    http: Client,
}

impl NvidiaNimOcr {
    pub fn new(config: NimOcrConfig) -> Result<Self> {
        let http = Client::builder()
            .timeout(std::time::Duration::from_secs(config.timeout_secs))
            .build()
            .map_err(|e| crate::core::ProcessorError::DocumentError(
                format!("Failed to build NVIDIA NIM HTTP client: {}", e),
            ))?;

        Ok(Self { config, http })
    }

    /// Process a document file by reading it as binary, encoding it as a
    /// base64 data-URI, and sending it to the NIM VLM endpoint.
    ///
    /// Returns a fully-populated `PipelineOutput`.
    pub async fn process_document(&self, file_path: &str) -> Result<PipelineOutput> {
        let path = Path::new(file_path);
        let extension = path
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_lowercase();

        // Read the file bytes
        let file_bytes = tokio::fs::read(file_path).await.map_err(|e| {
            crate::core::ProcessorError::DocumentError(format!(
                "Failed to read file {}: {}",
                file_path, e
            ))
        })?;

        // Determine MIME type for the data URI
        let mime = match extension.as_str() {
            "pdf" => "application/pdf",
            "png" => "image/png",
            "jpg" | "jpeg" => "image/jpeg",
            "tiff" | "tif" => "image/tiff",
            "webp" => "image/webp",
            "bmp" => "image/bmp",
            "gif" => "image/gif",
            _ => "application/octet-stream",
        };

        let b64 = base64::engine::general_purpose::STANDARD.encode(&file_bytes);
        let data_uri = format!("data:{};base64,{}", mime, b64);

        // Send to the NIM endpoint
        let markdown_output = self.call_nim_vision(&data_uri, &extension).await?;

        // Parse the model's Markdown output into PipelineOutput
        let title = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("Document")
            .to_string();

        let output = Self::parse_markdown_to_pipeline_output(&title, &markdown_output, &extension);
        Ok(output)
    }

    /// Send a single vision request to the NIM endpoint.
    async fn call_nim_vision(&self, data_uri: &str, extension: &str) -> Result<String> {
        let system_prompt = Self::build_extraction_prompt(extension);

        let request = ChatRequest {
            model: self.config.model.clone(),
            messages: vec![ChatMessage {
                role: "user".to_string(),
                content: vec![
                    ContentPart::Text {
                        text: system_prompt,
                    },
                    ContentPart::ImageUrl {
                        image_url: ImageUrl {
                            url: data_uri.to_string(),
                        },
                    },
                ],
            }],
            max_tokens: self.config.max_tokens,
            temperature: 0.1, // Low temperature for deterministic extraction
        };

        let url = format!("{}/chat/completions", self.config.endpoint);

        let mut req_builder = self.http.post(&url).json(&request);

        if let Some(ref api_key) = self.config.api_key {
            req_builder = req_builder.bearer_auth(api_key);
        }

        let response = req_builder.send().await.map_err(|e| {
            crate::core::ProcessorError::DocumentError(format!(
                "NVIDIA NIM OCR request failed: {}",
                e
            ))
        })?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(crate::core::ProcessorError::DocumentError(format!(
                "NVIDIA NIM returned HTTP {}: {}",
                status, body
            )));
        }

        let chat_response: ChatResponse = response.json().await.map_err(|e| {
            crate::core::ProcessorError::DocumentError(format!(
                "Failed to parse NVIDIA NIM response: {}",
                e
            ))
        })?;

        chat_response
            .choices
            .into_iter()
            .next()
            .map(|c| c.message.content)
            .ok_or_else(|| {
                crate::core::ProcessorError::DocumentError(
                    "NVIDIA NIM response contained no choices".to_string(),
                )
            })
    }

    /// Build the extraction prompt that instructs the VLM to output
    /// structured Markdown with clearly delimited sections and tables.
    fn build_extraction_prompt(extension: &str) -> String {
        let doc_type_hint = match extension {
            "pdf" => "PDF document",
            "png" | "jpg" | "jpeg" | "tiff" | "tif" | "bmp" | "webp" => "scanned document image",
            "docx" | "doc" => "Word document",
            "pptx" | "ppt" => "presentation",
            _ => "document",
        };

        format!(
            r#"You are a document OCR and structure extraction system. You are processing a {doc_type_hint}.

Extract ALL text content from this document with the following rules:

1. **Headings**: Use Markdown heading syntax (`# `, `## `, `### `, etc.) to mark section titles exactly as they appear in the document. Preserve the hierarchical nesting level.

2. **Body text**: Reproduce all paragraph text faithfully. Do NOT summarize or skip content. Include every sentence.

3. **Tables**: Format tables using Markdown table syntax with `|` delimiters and `---` header separators. Preserve all rows and columns. Before each table, add a line `<!-- TABLE -->` and optionally a caption line `Caption: <caption text>`.

4. **Lists**: Reproduce numbered and bulleted lists using Markdown list syntax.

5. **Code blocks**: Wrap code snippets in fenced code blocks (```language).

6. **Reading order**: Follow the natural reading order of the document (left-to-right, top-to-bottom, across columns).

7. **No commentary**: Do NOT add explanatory text, summaries, or notes of your own. Output ONLY the extracted document content.

Output the complete extracted content in Markdown now:"#
        )
    }

    /// Parse raw Markdown output from the VLM into a `PipelineOutput`.
    fn parse_markdown_to_pipeline_output(
        title: &str,
        markdown: &str,
        extension: &str,
    ) -> PipelineOutput {
        let mut sections: Vec<PipelineSection> = Vec::new();
        let mut tables: Vec<PipelineTable> = Vec::new();

        let mut current_heading = String::new();
        let mut current_level: u8 = 1;
        let mut current_content: Vec<String> = Vec::new();

        // Track whether we're inside a table block
        let mut in_table = false;
        let mut table_lines: Vec<String> = Vec::new();
        let mut table_caption = String::new();

        for line in markdown.lines() {
            let trimmed = line.trim();

            // Detect table marker
            if trimmed == "<!-- TABLE -->" {
                // Flush any current content as a section
                if !current_content.is_empty() {
                    let content_text = current_content.join("\n\n").trim().to_string();
                    if !content_text.is_empty() {
                        sections.push(PipelineSection {
                            heading: current_heading.clone(),
                            level: current_level,
                            content: content_text,
                        });
                    }
                    current_content.clear();
                }
                in_table = true;
                table_lines.clear();
                table_caption.clear();
                continue;
            }

            // Capture table caption
            if in_table && trimmed.starts_with("Caption:") {
                table_caption = trimmed.trim_start_matches("Caption:").trim().to_string();
                continue;
            }

            // Collect table rows (lines starting with `|`)
            if in_table {
                if trimmed.starts_with('|') || trimmed.starts_with("---") || trimmed.starts_with("|---") {
                    table_lines.push(line.to_string());
                    continue;
                } else {
                    // End of table block
                    if !table_lines.is_empty() {
                        tables.push(PipelineTable {
                            caption: table_caption.clone(),
                            markdown: table_lines.join("\n"),
                        });
                    }
                    in_table = false;
                    table_lines.clear();
                    table_caption.clear();
                    // Fall through to process current line normally
                }
            }

            // Detect headings
            if trimmed.starts_with('#') {
                // Flush current section
                if !current_content.is_empty() {
                    let content_text = current_content.join("\n\n").trim().to_string();
                    if !content_text.is_empty() {
                        sections.push(PipelineSection {
                            heading: current_heading.clone(),
                            level: current_level,
                            content: content_text,
                        });
                    }
                    current_content.clear();
                }

                // Parse heading level
                let hashes = trimmed.chars().take_while(|c| *c == '#').count();
                current_level = hashes.min(6).max(1) as u8;
                current_heading = trimmed
                    .trim_start_matches('#')
                    .trim()
                    .to_string();
                continue;
            }

            // Regular content lines
            if !trimmed.is_empty() {
                current_content.push(trimmed.to_string());
            } else if !current_content.is_empty() {
                // Blank line — keep paragraph break
                current_content.push(String::new());
            }
        }

        // Flush any remaining table
        if in_table && !table_lines.is_empty() {
            tables.push(PipelineTable {
                caption: table_caption,
                markdown: table_lines.join("\n"),
            });
        }

        // Flush any remaining section content
        if !current_content.is_empty() {
            let content_text = current_content.join("\n\n").trim().to_string();
            if !content_text.is_empty() {
                sections.push(PipelineSection {
                    heading: current_heading,
                    level: current_level,
                    content: content_text,
                });
            }
        }

        // If no sections were parsed at all, create one from raw markdown
        if sections.is_empty() && !markdown.trim().is_empty() {
            sections.push(PipelineSection {
                heading: String::new(),
                level: 1,
                content: markdown.trim().to_string(),
            });
        }

        let word_count = markdown.split_whitespace().count();

        let mut metadata = std::collections::HashMap::new();
        metadata.insert(
            "parser".to_string(),
            serde_json::Value::String("nvidia_nim".to_string()),
        );
        metadata.insert(
            "format".to_string(),
            serde_json::Value::String(extension.to_string()),
        );
        metadata.insert(
            "word_count".to_string(),
            serde_json::json!(word_count),
        );

        PipelineOutput {
            title: if !title.is_empty() {
                title.to_string()
            } else {
                sections
                    .first()
                    .filter(|s| !s.heading.is_empty())
                    .map(|s| s.heading.clone())
                    .unwrap_or_else(|| "Document".to_string())
            },
            sections,
            tables,
            metadata,
        }
    }
}
