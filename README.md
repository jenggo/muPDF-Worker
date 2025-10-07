# MuPDF Document Parsing Worker

Standalone C service for high-performance document parsing using MuPDF. Provides real-time processing with WebSocket communication and Redis queue integration.

## Features

- **WebSocket Communication** - Real-time bidirectional messaging with Airag server
- **Redis Queue Integration** - Async job processing with persistent queue
- **Live Progress Updates** - Track parsing progress in real-time
- **Spatial Document Extraction**:
  - Character-level text with precise coordinates
  - Paragraph detection with bounding boxes
  - Embedded image extraction with metadata
- **Security Hardened** - CVE-free build with OpenJPEG disabled
- **Multi-threaded** - Concurrent document processing
- **Production Ready** - Static binary, scratch container, resource limits

## Architecture

The worker operates in multiple modes:

- **WebSocket Mode** - Connects to Airag server for real-time job coordination
- **Redis Queue Mode** - Pulls jobs from Redis queue for processing
- **HTTP API Mode** - Legacy REST API for document parsing (POST /parse, GET /health)

Jobs are processed concurrently with configurable limits. Results include text blocks with spatial coordinates, paragraph boundaries, and embedded images.

## Quick Start

### Docker (Recommended)
```bash
docker build -t airag-worker .
docker run -e WEBSOCKET_URL=ws://airag:1807/ws/worker airag-worker
```

### Local Build
```bash
make deps-ubuntu  # or make deps-alpine
make              # build binary
make test         # test with example PDF
```

## Configuration

Environment variables:

```bash
# Communication
WEBSOCKET_URL=ws://airag:1807/ws/worker    # WebSocket server
REDIS_HOST=localhost                        # Redis queue host
REDIS_PORT=6379                            # Redis queue port

# Processing
WORKER_MAX_CONCURRENT=8                    # Max parallel jobs (default: 8)
WORKER_VECTOR_EXTRACTION_ENABLED=false     # Extract vector graphics
UPLOADS_PATH=/tmp/airag                    # Document storage path
```

## Integration

The worker connects to the Airag server and processes documents from:
1. **WebSocket messages** - Real-time job requests
2. **Redis queue** - Persistent job queue (`airag:worker:jobs`)

Results are sent back via WebSocket or callback URL with extracted text blocks, paragraphs, and images with spatial metadata.

## Security

**CVE-Free Build:**
- Custom MuPDF compilation with OpenJPEG disabled (eliminates CVE-2025-54874)
- Static binary with zero runtime dependencies
- Scratch-based container with no OS libraries or shell

**Container Hardening:**
- Non-root user execution
- Multi-stage builds compiled from source
- Minimal attack surface

**Memory Scaling Guide:**

| Concurrent | Typical Docs | Complex PDFs | Vector-Heavy |
|------------|--------------|--------------|--------------|
| 1          | 100MB        | 200MB        | 300MB        |
| 2          | 200MB        | 400MB        | 600MB        |
| 4          | 400MB        | 800MB        | 1.2GB        |
| 8          | 800MB        | 1.6GB        | 2.4GB        |
| 40         | 4GB          | 8GB          | 12GB         |

Recommendation: `WORKER_MAX_CONCURRENT = Available_RAM_GB / 0.5`

Example: 4GB RAM → max 8 concurrent (safer: 4-6)


## License

This worker service is licensed under the GNU Affero General Public License v3 (AGPL-3.0) due to its use of MuPDF, which is also AGPL licensed. See the LICENSE file in this directory for the complete license terms.

### AGPL Compliance Requirements

Since this worker service uses MuPDF (which is AGPL licensed), the entire worker service must also be distributed under AGPL v3. This means:

- **Source Code Availability**: If you deploy this worker service on a network server (which is the typical use case), you must provide access to the complete source code to anyone who uses the service.

- **Network Service Provision**: Section 13 of the AGPL requires that users interacting with the service over a network must be offered access to the source code.

- **Derivative Works**: Any modifications to this worker service must also be licensed under AGPL v3.

- **Commercial Use**: AGPL allows commercial use, but requires that you provide source code access even for network services.

### MuPDF Licensing

This worker uses MuPDF for document processing. MuPDF is dual-licensed:
- **AGPL v3** (free/open source option) - Used by this worker
- **Commercial License** (proprietary option) - Available from Artifex Software

The worker is configured to build MuPDF from source with security hardening (OpenJPEG disabled) while maintaining AGPL compliance.

### Source Code Access

The complete source code for this worker service is available at the project repository. Users of the deployed service have the right to request and receive the complete source code under AGPL v3 terms.
