# MuPDF Document Parsing Service

A standalone C service for document parsing using MuPDF, designed to work as a microservice for the RAG system.

## Features

- **HTTP API** - Lightweight REST API for document parsing requests
- **Asynchronous Processing** - Non-blocking document processing with callback responses
- **Comprehensive Extraction**:
  - Text with precise spatial coordinates (character-level)
  - Paragraph-level bounding boxes with intelligent detection
  - Embedded images with metadata and coordinates
- **Multi-format Support** - PDF and other MuPDF-supported formats
- **Concurrent Processing** - Multi-threaded worker architecture
- **Container Ready** - Minimal Alpine-based Docker image
- **Production Ready** - Health checks, non-root user, proper error handling

## API Endpoints

### POST /parse
Parse a document and send results via callback.

**Request:**
```json
{
  "file_path": "/path/to/document.pdf",
  "job_id": "unique-job-identifier",
  "callback_url": "http://rag-service/callback",
  "organization_id": 1,
  "extract_vector_images": false
}
```

**Fields:**
- `file_path` (required): Path to the document file
- `job_id` (required): Unique identifier for the job
- `callback_url` (required): URL to send results to
- `organization_id` (required): Organization identifier for multi-tenant isolation
- `extract_vector_images` (optional): Enable vector image extraction (default: false, can be overridden by `WORKER_VECTOR_EXTRACTION_ENABLED` environment variable)

**Response:**
```json
{
  "status": "accepted",
  "job_id": "unique-job-identifier"
}
```

**Callback Response:**
```json
{
  "job_id": "unique-job-identifier",
  "text_blocks": [...],
  "paragraphs": [...],
  "images": [...],
  "metadata": {
    "page_count": 35,
    "format": "mupdf",
    "status": "success"
  }
}
```

### GET /health
Health check endpoint.

**Response:** `OK`

## Building

### Local Build
```bash
# Install dependencies (Ubuntu/Debian)
make deps-ubuntu

# Or for Alpine
make deps-alpine

# Build the service
make

# Test with example PDF
make test

# Build static binary
make static
```

### Docker Build
```bash
# Build container image
docker build -t mupdf-parser .

# Run with docker-compose
docker-compose up

# Test with docker-compose
docker-compose --profile test up mupdf-test
```

## JSON Output Format

### Text Blocks
```json
{
  "page": 1,
  "type": "text",
  "bbox": {
    "x0": 22.477, "y0": 355.206,
    "x1": 152.673, "y1": 366.206,
    "width": 130.196, "height": 11.0
  },
  "lines": [
    {
      "bbox": {...},
      "wmode": 0,
      "characters": [
        {
          "c": "B",
          "origin": {"x": 22.477, "y": 364.006},
          "size": 11.0,
          "quad": {
            "ul": {"x": 22.477, "y": 355.206},
            "ur": {"x": 30.419, "y": 355.206},
            "ll": {"x": 22.477, "y": 366.206},
            "lr": {"x": 30.419, "y": 366.206}
          }
        }
      ],
      "text": "Basic Instruction Manual"
    }
  ],
  "text": "Basic Instruction Manual\n"
}
```

### Paragraphs
```json
{
  "page": 1,
  "type": "paragraph",
  "bbox": {...},
  "lines": [...],
  "text": "Combined paragraph text",
  "line_count": 3
}
```

### Images
```json
{
  "page": 1,
  "image_id": 0,
  "type": "image",
  "bbox": {...},
  "transform": {
    "a": 1.0, "b": 0.0, "c": 0.0,
    "d": 1.0, "e": 100.0, "f": 200.0
  },
  "width": 800,
  "height": 600,
  "bpc": 8,
  "n": 3,
  "format": "jpeg"
}
```

## Configuration

The service can be configured via environment variables:

- `PORT` - HTTP port (default: 8080)
- `MAX_WORKERS` - Maximum concurrent workers (default: 8)

## Integration with RAG System

The service is designed to integrate with the main RAG system:

1. RAG system sends document parsing requests to `/parse`
2. Service processes document asynchronously
3. Results are sent back via HTTP callback to RAG system
4. RAG system can then index the extracted content with spatial metadata

## Performance

- **Memory Efficient** - Uses MuPDF's streaming API
- **Concurrent** - Handles multiple documents simultaneously
- **Fast** - Optimized C implementation with minimal overhead
- **Small Footprint** - ~15MB Alpine-based container image

## Supported Formats

All formats supported by MuPDF:
- PDF
- XPS
- OpenXPS
- CBZ (Comic Book Archive)
- EPUB
- And more

## Development

### Build Targets
- `make all` - Standard build
- `make static` - Static binary
- `make debug` - Debug build
- `make test` - Run tests
- `make clean` - Clean build artifacts
- `make info` - Show build information

### Testing
```bash
# Test with eos.pdf
./test_simple ../eos.pdf

# Test full service
./test_service.sh
```

## Security

### Production-Ready Security Hardening

The worker service implements comprehensive security measures:

- **Runs as non-root user in container** - Reduces attack surface
- **Input validation on all endpoints** - Prevents malicious requests
- **No file system writes** - Read-only document processing only
- **Configurable resource limits** - Prevents resource exhaustion
- **CVE-Eliminated Build** - Custom MuPDF compilation removes critical vulnerabilities

### Security Architecture

**Static Binary Deployment:**
- Single static binary with zero runtime dependencies
- Eliminates entire classes of CVEs by removing dependency libraries
- Runs in scratch containers with minimal attack surface
- No package managers or system libraries exposed

**OpenJPEG Vulnerability Elimination:**
- Built from MuPDF source with OpenJPEG completely disabled
- Removes CVE-2025-54874 and related JPEG 2000 vulnerabilities
- Maintains 99%+ image processing capability (standard JPEG, PNG, JBIG2 supported)
- Production-tested alternative that prioritizes security over rare format support

**Container Security:**
- Multi-stage Docker builds with minimal final image
- All dependencies compiled from source with security-hardened configurations
- Scratch-based runtime eliminates OS-level vulnerabilities
- No shell, package managers, or unnecessary tools in production container

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
