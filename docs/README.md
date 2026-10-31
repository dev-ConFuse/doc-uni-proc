# Document Unified Processor (doc-uni-proc)

**Port**: 8090 · **Role**: Document processing, chunking and same-source
knowledge-graph construction

> This is the canonical README for the service; **all documentation lives in
> [`docs/`](./)** (this folder). See [index.md](index.md) for the docs index.

## Overview

`doc-uni-proc` processes **documents** for the ConFuse platform under a
**Strict Same-Source Policy**: it processes one source at a time and only
extracts relationships within that single source.

- **Document Processing**: Extract text from documents and chunk appropriately
- **Language / Format Detection**: Automatically detect document types
- **Intelligent Chunking**: Context-aware chunking for optimal embeddings
- **Metadata Extraction**: Extract rich metadata from processed files
- **Kafka Integration**: Publish chunk events to `embeddings-service`
- **Graph Storage**: Write nodes and structural edges directly to FalkorDB

*Note: Cross-source relationship discovery is handled asynchronously by the
`fast-fetcher` microservice.*

> The repository flavour lives in `repo-uni-proc` (code analysis + chunking).

## Key API surface

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/api/v1/process` | POST | Process a document payload (auth required) |
| `/api/v1/process/local` | POST | Process a shared-volume directory (internal, no auth) |
| `/api/v1/status/{source_id}` | GET | Processing status |
| `/api/v1/graph/sync` | POST | Trigger graph sync (auth required) |

Full schemas: [api-reference.md](api-reference.md).

## How to run the microservice

### Prerequisites

- Rust 1.70+ · Python 3.11+ (ML models) · FalkorDB (Redis protocol) · Kafka

### Installation

```bash
cargo build --release

# Python dependencies for the ML models
pip install -r requirements.txt
```

### Configuration

```bash
export DATABASE_URL="postgresql://localhost/unified_processor"
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
export FALKORDB_HOST="localhost"
export FALKORDB_PORT="6379"
export FALKORDB_PASSWORD="your-password"
export PORT=8090
```

## Testing

Tests for this service live in the central test module (this service contains
no tests):

```bash
cd ../ConFuse-test-module
pip install -r requirements.txt
pytest unified_processor -v
```

## Documentation

| Document | Contents |
|----------|----------|
| [index.md](index.md) | Overview & docs index |
| [api-reference.md](api-reference.md) | Full endpoint reference |
| [architecture.md](architecture.md) | SourceRelationshipRouter & graph design |
| [setup.md](setup.md) | Local development setup |
