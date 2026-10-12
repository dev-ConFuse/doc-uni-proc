//! Local shared-volume directory scanner
//!
//! Provides `process_local_directory` â€” called by the `POST /api/v1/process/local` endpoint
//! after `data-connector` downloads a source (git repo or document set) to the shared storage.
//!
//! # AKS Portability
//! The shared volume is mounted at `DOWNLOADS_BASE_PATH` (default `/shared/downloads`):
//! - **Local dev**: Docker named volume `confuse-downloads`
//! - **AKS**:       Azure Files PVC `confuse-downloads-pvc`
//! The mount path is identical in both environments â€” no code change on deployment.

use crate::core::orchestrator::UnifiedProcessor;
use crate::core::error::ProcessorError;

use std::path::Component;

pub mod security {
    //! Security and anomaly detection.
    //!
    //! Ported from `graphify/security.py` to identify potential secrets or
    //! insecure patterns in chunks before they are fully ingested or embedded.

    use crate::core::chunking::Chunk;
    use regex::Regex;
    use std::sync::OnceLock;

    /// Provides basic security and anomaly detection.
    /// Designed to flag chunks that might contain hardcoded secrets or PII.
    pub struct SecurityScanner;

    static AWS_KEY_REGEX: OnceLock<Regex> = OnceLock::new();
    static PRIVATE_KEY_REGEX: OnceLock<Regex> = OnceLock::new();

    impl SecurityScanner {
        fn aws_regex() -> &'static Regex {
            AWS_KEY_REGEX.get_or_init(|| Regex::new(r"(?i)AKIA[0-9A-Z]{16}").unwrap())
        }

        fn private_key_regex() -> &'static Regex {
            PRIVATE_KEY_REGEX.get_or_init(|| Regex::new(r"-----BEGIN [A-Z ]+ PRIVATE KEY-----").unwrap())
        }

        /// Scan a chunk's content for potential secrets and append metadata if found.
        pub fn scan_chunk(chunk: &mut Chunk) {
            let content = &chunk.content;
            let mut anomalies = Vec::new();

            if Self::aws_regex().is_match(content) {
                anomalies.push("Detected potential AWS Access Key");
            }

            if Self::private_key_regex().is_match(content) {
                anomalies.push("Detected potential Private Key");
            }

            if !anomalies.is_empty() {
                let anomalies_value = serde_json::to_value(&anomalies).unwrap_or(serde_json::Value::Null);
                chunk.metadata.custom.insert("security_anomalies".to_string(), anomalies_value);
            }
        }
    }
}

type Result<T> = std::result::Result<T, ProcessorError>;

const MAX_FILE_BYTES: u64 = 5 * 1024 * 1024; // 5 MiB per-file limit
/// Maximum chars per chunk for document sections. Longer sections are
/// further split by paragraph boundaries.



impl UnifiedProcessor {
    /// Scan a directory on the shared volume and process every file through the chunking pipeline.
    ///
    /// # Security
    /// Canonicalises the path and verifies it is a strict sub-path of `DOWNLOADS_BASE_PATH`
    /// to prevent path-traversal attacks before any I/O is performed.
    pub async fn process_local_directory(
        &self,
        source_id: &str,
        directory_path: &str,
        user_id: &str,
    ) -> Result<()> {
        let start = std::time::Instant::now();

        // â”€â”€ Path-traversal guard (cross-platform) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        let base_env = std::env::var("DOWNLOADS_BASE_PATH")
            .expect("Missing required environment variable: DOWNLOADS_BASE_PATH");
        let base_path = std::path::PathBuf::from(&base_env);
        
        tracing::info!(
            source_id = %source_id,
            target_path = %directory_path,
            base_path = %base_env,
            user_id = %user_id,
            "Starting cross-platform path validation"
        );
        
        // First, check if the target directory actually exists
        let target_path_obj = std::path::Path::new(directory_path);
        if !target_path_obj.exists() {
            tracing::error!(
                source_id = %source_id,
                target_path = %directory_path,
                "Directory does not exist - cannot process non-existent directory"
            );
            return Err(ProcessorError::InfraError(format!(
                "Directory inaccessible: {directory_path} â€” The system cannot find the file specified"
            )));
        }
        
        if !target_path_obj.is_dir() {
            tracing::error!(
                source_id = %source_id,
                target_path = %directory_path,
                "Path exists but is not a directory"
            );
            return Err(ProcessorError::InfraError(format!(
                "Directory inaccessible: {directory_path} â€” Path is not a directory"
            )));
        }
        
        tracing::info!(
            source_id = %source_id,
            target_path = %directory_path,
            "Directory exists and is accessible"
        );
        
        // Cross-platform path validation
        match self.is_path_within_allowed_directory(directory_path, &base_path) {
            Ok(true) => {
                tracing::info!(
                    source_id = %source_id,
                    target_path = %directory_path,
                    "Path validation passed - directory is within allowed base path"
                );
            }
            Ok(false) => {
                tracing::error!(
                    source_id = %source_id,
                    target_path = %directory_path,
                    base_path = %base_env,
                    "Path validation failed - directory is outside allowed base path"
                );
                return Err(ProcessorError::InfraError(format!(
                    "Security: '{directory_path}' is outside DOWNLOADS_BASE_PATH '{}'", 
                    base_env
                )));
            }
            Err(e) => {
                tracing::error!(
                    source_id = %source_id,
                    target_path = %directory_path,
                    base_path = %base_env,
                    error = %e,
                    "Path validation error - unable to validate directory path"
                );
                return Err(e);
            }
        }

        tracing::info!(
            source_id = %source_id,
            directory = %directory_path,
            "Starting shared-volume directory scan"
        );

        // â”€â”€ Collect file paths (blocking walk offloaded to a thread-pool thread) â”€â”€â”€
        let dir_path = std::path::PathBuf::from(directory_path);
        let file_paths = tokio::task::spawn_blocking(move || {
            let mut collected: Vec<std::path::PathBuf> = Vec::new();
            collect_files_recursive(&dir_path, &mut collected);
            collected
        })
        .await
        .map_err(|e| ProcessorError::InfraError(format!("Directory walk panicked: {e}")))?;

        let total_files = file_paths.len();
        tracing::info!(source_id = %source_id, total_files, "Files discovered in shared volume directory");

        let mut processed = 0usize;
        let mut skipped = 0usize;
        let mut total_chunks = 0usize;
        let mut all_source_chunks = Vec::new();

        for file_path in &file_paths {
            // Skip oversized files
            let file_meta = match std::fs::metadata(file_path) {
                Ok(m) => m,
                Err(_) => { skipped += 1; continue; }
            };
            if file_meta.len() > MAX_FILE_BYTES {
                tracing::debug!(path = %file_path.display(), "Skipping oversized file");
                skipped += 1;
                continue;
            }

            // Relative path used as the "filename" for chunk metadata
            let base_dir = std::path::Path::new(directory_path);
            let relative_path = file_path
                .strip_prefix(base_dir)
                .unwrap_or(file_path)
                .to_string_lossy()
                .to_string();

            // Check if it's a document type that Docling should handle
            let is_document = file_path.extension()
                .and_then(|e| e.to_str())
                .map(|e| {
                    let lower = e.to_lowercase();
                    matches!(lower.as_str(), "pdf" | "docx" | "pptx" | "doc" | "ppt" | "rtf" | "epub" | "md" | "markdown")
                })
                .unwrap_or(false);

            if is_document {
                // â”€â”€ Pipeline-based structured document parsing â”€â”€
                tracing::info!(path = %relative_path, "Parsing document with pipeline_parser");

                let path_str = file_path.to_string_lossy().to_string();
                
                match self.document_parser.process_document_file(&path_str).await {
                    Ok(parsed) => {
                        let chunks = crate::processors::parser::build_document_chunks(&parsed, &relative_path, source_id);
                        total_chunks += chunks.len();
                        processed += 1;

                        tracing::info!(
                            source_id = %source_id,
                            file = %relative_path,
                            sections = parsed.sections.len(),
                            tables = parsed.tables.len(),
                            chunks = chunks.len(),
                            parser = %parsed.metadata.get("parser").and_then(|v| v.as_str()).unwrap_or("unknown"),
                            "Document parsed and chunked via pipeline"
                        );

                        // Run security scan
                        let mut chunks = chunks;
                        for chunk in &mut chunks {
                            crate::core::scanner::security::SecurityScanner::scan_chunk(chunk);
                        }

                        all_source_chunks.extend(chunks.clone());

                        if let Err(e) = self.store_and_publish_chunks(chunks, source_id, None, user_id).await {
                            tracing::error!(
                                source_id = %source_id,
                                file = %relative_path,
                                error = %e,
                                "Failed to embed and store document chunks"
                            );
                        }
                    }
                    Err(e) => {
                        tracing::warn!(path = %relative_path, error = %e, "Pipeline script failed");
                        skipped += 1;
                    }
                }
            } else {
                // â”€â”€ Non-document files: existing HybridChunker path â”€â”€
                let content = match tokio::fs::read_to_string(file_path).await {
                    Ok(c) => c,
                    Err(_) => {
                        tracing::debug!(path = %relative_path, "Skipping binary/unreadable file");
                        skipped += 1;
                        continue;
                    }
                };

                match self.generate_chunks(&content, &relative_path, source_id).await {
                    Ok(mut chunks) => {
                        total_chunks += chunks.len();
                        processed += 1;
                        tracing::debug!(
                            source_id = %source_id,
                            file = %relative_path,
                            chunks = chunks.len(),
                            "File chunked"
                        );

                        for chunk in &mut chunks {
                            crate::core::scanner::security::SecurityScanner::scan_chunk(chunk);
                        }

                        all_source_chunks.extend(chunks.clone());

                        if let Err(e) = self.store_and_publish_chunks(chunks, source_id, None, user_id).await {
                            tracing::error!(
                                source_id = %source_id,
                                file = %relative_path,
                                error = %e,
                                "Failed to embed and store chunks"
                            );
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            source_id = %source_id,
                            file = %relative_path,
                            error = %e,
                            "Chunking failed, skipping file"
                        );
                        skipped += 1;
                    }
                }
            }
        }

        if let Err(e) = self.extract_and_store_relationships(&all_source_chunks, source_id, user_id).await {
            tracing::error!(
                source_id = %source_id,
                error = %e,
                "Failed to extract and store cross-file relationships"
            );
        }

        tracing::info!(
            source_id = %source_id,
            total_files,
            processed,
            skipped,
            total_chunks,
            elapsed_secs = start.elapsed().as_secs_f64(),
            "Shared-volume directory scan complete"
        );

        Ok(())
    }

    /// Cross-platform path validation to check if a path is within allowed directory
    /// Works on Windows, Linux, and macOS without canonicalization issues
    fn is_path_within_allowed_directory(&self, target_path: &str, base_path: &std::path::Path) -> Result<bool> {
        
        let target = std::path::PathBuf::from(target_path);
        
        tracing::debug!(
            target_path = %target_path,
            target_is_absolute = target.is_absolute(),
            base_path = %base_path.display(),
            base_is_absolute = base_path.is_absolute(),
            "Path validation: initial check"
        );
        
        // First, try to resolve both paths to absolute form
        let target_abs = if target.is_absolute() {
            target.clone()
        } else {
            match std::env::current_dir() {
                Ok(curr_dir) => {
                    let resolved = curr_dir.join(&target);
                    tracing::debug!(
                        current_dir = %curr_dir.display(),
                        resolved_target = %resolved.display(),
                        "Resolved relative path to absolute"
                    );
                    resolved
                }
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        "Cannot get current directory"
                    );
                    return Err(ProcessorError::InfraError(format!("Cannot get current directory: {e}")));
                }
            }
        };
        
        let base_abs = if base_path.is_absolute() {
            base_path.to_path_buf()
        } else {
            match std::env::current_dir() {
                Ok(curr_dir) => {
                    let resolved = curr_dir.join(base_path);
                    tracing::debug!(
                        current_dir = %curr_dir.display(),
                        resolved_base = %resolved.display(),
                        "Resolved relative base path to absolute"
                    );
                    resolved
                }
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        "Cannot get current directory for base path"
                    );
                    return Err(ProcessorError::InfraError(format!("Cannot get current directory: {e}")));
                }
            }
        };
        
        // Normalize both paths by resolving '.' and '..' components
        let target_normalized = self.normalize_path(&target_abs);
        let base_normalized = self.normalize_path(&base_abs);
        
        tracing::debug!(
            target_normalized = %target_normalized.display(),
            base_normalized = %base_normalized.display(),
            "Path validation: normalized paths"
        );
        
        // Check if target starts with base path
        let target_str = target_normalized.to_string_lossy().to_lowercase().replace("/", "\\");
        let base_str = base_normalized.to_string_lossy().to_lowercase().replace("/", "\\");
        let is_within = target_str.starts_with(&base_str);
        
        tracing::debug!(
            is_within = %is_within,
            "Path validation: final result"
        );
        
        Ok(is_within)
    }
    
    /// Normalize path by resolving '.' and '..' components
    fn normalize_path(&self, path: &std::path::Path) -> std::path::PathBuf {
        let mut components = path.components().peekable();
        let mut new_path = std::path::PathBuf::new();
        
        tracing::trace!(
            original_path = %path.display(),
            "Path normalization: starting"
        );
        
        while let Some(component) = components.next() {
            match component {
                Component::Prefix(..) => {
                    // On Windows, keep the prefix (e.g., "C:")
                    new_path.push(component);
                    tracing::trace!(component = ?component, "Path normalization: added prefix");
                }
                Component::RootDir => {
                    // Keep root directory
                    new_path.push(component);
                    tracing::trace!(component = ?component, "Path normalization: added root dir");
                }
                Component::CurDir => {
                    // Skip '.' (current directory)
                    tracing::trace!("Path normalization: skipped current directory");
                }
                Component::ParentDir => {
                    // Handle '..' by removing the last component if possible
                    if new_path.pop() && new_path.as_os_str().is_empty() {
                        // If we popped everything and it's empty, add root dir back
                        if let Some(Component::RootDir) = components.peek() {
                            new_path.push(Component::RootDir);
                        }
                    }
                    tracing::trace!(component = ?component, current_path = %new_path.display(), "Path normalization: handled parent dir");
                }
                Component::Normal(_) => {
                    new_path.push(component);
                    tracing::trace!(component = ?component, "Path normalization: added normal component");
                }
            }
        }
        
        tracing::trace!(
            normalized_path = %new_path.display(),
            "Path normalization: completed"
        );
        
        new_path
    }

}

use walkdir::WalkDir;

/// Collect all file paths under `dir` iteratively using optimized WalkDir, skipping:
/// - Hidden directories (`.git`, `.svn`, `.idea`, etc.)
/// - `node_modules`, `__pycache__`, `target`, `dist`
fn collect_files_recursive(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
    let walker = WalkDir::new(dir).into_iter();
    for entry in walker.filter_entry(|e| {
        let name_str = e.file_name().to_string_lossy();
        
        // Skip hidden dirs and well-known noise folders
        !(name_str.starts_with('.')
            || name_str == "node_modules"
            || name_str == "__pycache__"
            || name_str == "target"
            || name_str == "dist"
            || name_str == "build")
    }) {
        if let Ok(entry) = entry {
            if entry.file_type().is_file() {
                out.push(entry.into_path());
            }
        }
    }
}
