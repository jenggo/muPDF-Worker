# MuPDF Document Parsing Service Makefile
# Builds a standalone C service for document parsing with MuPDF

CC=gcc
CFLAGS=-std=c99 -Wall -Wextra -O2 -g
LDFLAGS=
TARGET=mupdf-parser
SRCDIR=.
BUILDDIR=build

# Source files
SOURCES=main.c extraction.c redis_queue.c monitoring.c ws_client.c
OBJECTS=$(SOURCES:%.c=$(BUILDDIR)/%.o)

# Required libraries
LIBS=-lmupdf -lmupdf-third -ljson-c -lcurl -lpthread -lm -lhiredis -lwslay

# Package config for library paths
MUPDF_CFLAGS=$(shell pkg-config --cflags mupdf 2>/dev/null || echo "-I/usr/local/include -I/usr/local/include/mupdf")
MUPDF_LIBS=$(shell pkg-config --libs mupdf 2>/dev/null || echo "-L/usr/local/lib -lmupdf -lmupdf-third")
JSON_CFLAGS=$(shell pkg-config --cflags json-c 2>/dev/null || echo "-I/usr/include/json-c")
JSON_LIBS=$(shell pkg-config --libs json-c 2>/dev/null || echo "-ljson-c")
CURL_CFLAGS=$(shell pkg-config --cflags libcurl 2>/dev/null || echo "")
CURL_LIBS=$(shell pkg-config --libs libcurl 2>/dev/null || echo "-lcurl")
REDIS_CFLAGS=$(shell pkg-config --cflags hiredis 2>/dev/null || echo "")
REDIS_LIBS=$(shell pkg-config --libs hiredis 2>/dev/null || echo "-lhiredis")

# CivetWeb - embedded HTTP server
CIVETWEB_DIR=civetweb
CIVETWEB_SRC=$(CIVETWEB_DIR)/src/civetweb.c
CIVETWEB_OBJ=$(BUILDDIR)/civetweb.o
CIVETWEB_CFLAGS=-DNO_SSL -DUSE_IPV6 -DNDEBUG

# Combined flags
ALL_CFLAGS=$(CFLAGS) $(MUPDF_CFLAGS) $(JSON_CFLAGS) $(CURL_CFLAGS) $(REDIS_CFLAGS) $(CIVETWEB_CFLAGS) -I$(CIVETWEB_DIR)/include
ALL_LIBS=$(MUPDF_LIBS) $(JSON_LIBS) $(CURL_LIBS) $(REDIS_LIBS) -lpthread -lm

# Default target
all: $(TARGET)

# Create build directory
$(BUILDDIR):
	mkdir -p $(BUILDDIR)

# Download CivetWeb if not present
$(CIVETWEB_DIR):
	@echo "Downloading CivetWeb..."
	wget -O civetweb.tar.gz https://github.com/civetweb/civetweb/archive/v1.16.tar.gz
	tar xzf civetweb.tar.gz
	mv civetweb-1.16 civetweb
	rm civetweb.tar.gz

# Build CivetWeb object
$(CIVETWEB_OBJ): $(CIVETWEB_DIR) | $(BUILDDIR)
	$(CC) $(ALL_CFLAGS) -c $(CIVETWEB_SRC) -o $@

# Build object files
$(BUILDDIR)/%.o: %.c | $(BUILDDIR)
	$(CC) $(ALL_CFLAGS) -c $< -o $@

# Link final executable
$(TARGET): $(OBJECTS) $(CIVETWEB_OBJ)
	$(CC) $(LDFLAGS) $^ $(ALL_LIBS) -o $@

# Static build (for container deployment)
static: LDFLAGS += -static -static-libgcc
static: CFLAGS += -DSTATIC_BUILD
static: ALL_LIBS += -static
static: $(TARGET)

# Fully static build for minimal containers (chainguard/static)
static-full: LDFLAGS += -static -static-libgcc -s -L/usr/local/lib
static-full: CFLAGS += -DSTATIC_BUILD -static -Os
static-full: MUPDF_CFLAGS=-I/usr/local/include -I/usr/local/include/mupdf
static-full: JSON_CFLAGS=-I/usr/include/json-c
static-full: CURL_CFLAGS=-I/usr/include/curl
static-full: ALL_LIBS := -lmupdf -lmupdf-third -ljson-c -lcurl -lnghttp2 -lssl -lcrypto -lz -lbrotlidec -lbrotlicommon -ljpeg -lpng -lfreetype -lharfbuzz -lidn2 -lunistring -lpsl -lpthread -lm -ldl
static-full: $(TARGET)

# Install dependencies (Ubuntu/Debian)
deps-ubuntu:
	sudo apt-get update
	sudo apt-get install -y \
		build-essential \
		libmupdf-dev \
		libmupdf-third-dev \
		libjson-c-dev \
		libcurl4-openssl-dev \
		libhiredis-dev \
		pkg-config \
		wget

# Install dependencies (Alpine)
deps-alpine:
	apk add --no-cache \
		build-base \
		mupdf-dev \
		mupdf-static \
		json-c-dev \
		json-c-static \
		curl-dev \
		curl-static \
		hiredis-dev \
		hiredis-static \
		openssl-libs-static \
		zlib-static \
		musl-dev \
		pkgconfig \
		wget

# Install dependencies for fully static build (Alpine)
deps-alpine-static:
	apk add --no-cache \
		build-base \
		musl-dev \
		mupdf-dev \
		mupdf-static \
		json-c-dev \
		json-c-static \
		curl-dev \
		curl-static \
		openssl-libs-static \
		openssl-dev \
		zlib-static \
		zlib-dev \
		jpeg-static \
		freetype-static \
		harfbuzz-static \
		pkgconfig \
		wget

# Test with example PDF
test: $(TARGET)
	@echo "Testing MuPDF parser service..."
	@if [ ! -f "../eos.pdf" ]; then \
		echo "Error: eos.pdf not found in parent directory"; \
		exit 1; \
	fi
	@echo "Starting service in background..."
	./$(TARGET) &
	@SERVICE_PID=$$!; \
	sleep 2; \
	echo "Sending test request..."; \
	curl -X POST http://localhost:8080/parse \
		-H "Content-Type: application/json" \
		-d '{"file_path": "../eos.pdf", "job_id": "test-123", "callback_url": "http://localhost:8080/callback"}' \
		|| true; \
	sleep 3; \
	echo "Stopping service..."; \
	kill $$SERVICE_PID 2>/dev/null || true

# Check for required libraries
check-deps:
	@echo "Checking dependencies..."
	@pkg-config --exists mupdf || echo "WARNING: MuPDF not found via pkg-config"
	@pkg-config --exists json-c || echo "WARNING: json-c not found via pkg-config"
	@pkg-config --exists libcurl || echo "WARNING: libcurl not found via pkg-config"
	@echo "Dependency check complete."

# Clean build artifacts
clean:
	rm -rf $(BUILDDIR)
	rm -f $(TARGET)

# Clean everything including downloaded dependencies
distclean: clean
	rm -rf $(CIVETWEB_DIR)
	rm -f civetweb.tar.gz

# Development build with debug symbols
debug: CFLAGS += -DDEBUG -g -O0
debug: $(TARGET)

# Show build information
info:
	@echo "MuPDF Document Parsing Service Build Information"
	@echo "================================================"
	@echo "Target: $(TARGET)"
	@echo "Compiler: $(CC)"
	@echo "CFLAGS: $(ALL_CFLAGS)"
	@echo "LIBS: $(ALL_LIBS)"
	@echo "Sources: $(SOURCES)"
	@echo ""
	@echo "Available targets:"
	@echo "  all       - Build the service (default)"
	@echo "  static    - Build statically linked binary"
	@echo "  static-full - Build fully static binary for minimal containers"
	@echo "  debug     - Build with debug symbols"
	@echo "  test      - Run basic functionality test"
	@echo "  lint      - Run clang-tidy and scan-build analysis"
	@echo "  lint-tidy - Run clang-tidy checks only"
	@echo "  lint-scan - Run clang static analyzer only"
	@echo "  lint-cppcheck - Install and run cppcheck"
	@echo "  lint-all  - Run all available linting tools"
	@echo "  valgrind  - Run valgrind memory debugging"
	@echo "  deps-*    - Install dependencies for specific OS"
	@echo "  check-deps- Check if required libraries are installed"
	@echo "  clean     - Remove build artifacts"
	@echo "  distclean - Remove all generated files"

# Linting targets
lint: lint-tidy lint-scan

# Run clang-tidy checks
lint-tidy:
	@echo "Running clang-tidy analysis..."
	@clang-tidy $(SOURCES) -- $(ALL_CFLAGS) || true

# Run clang static analyzer
lint-scan: clean
	@echo "Running clang static analyzer..."
	@scan-build -o build/scan-results --use-cc=gcc make all

# Install cppcheck and run additional checks
lint-cppcheck:
	@echo "Installing and running cppcheck..."
	@which cppcheck >/dev/null 2>&1 || (echo "Installing cppcheck..." && sudo apt-get update && sudo apt-get install -y cppcheck)
	@cppcheck --enable=all --std=c99 --suppress=missingIncludeSystem $(SOURCES)

# Run all available linting tools
lint-all: lint-tidy lint-scan lint-cppcheck

# Memory debugging with valgrind
valgrind: debug
	@echo "Running valgrind memory check..."
	@which valgrind >/dev/null 2>&1 || (echo "Installing valgrind..." && sudo apt-get update && sudo apt-get install -y valgrind)
	@valgrind --leak-check=full --show-leak-kinds=all --track-origins=yes ./$(TARGET) --version 2>/dev/null || echo "Valgrind check complete"

# Help target
help: info

.PHONY: all static debug test deps-ubuntu deps-alpine check-deps clean distclean info help lint lint-tidy lint-scan lint-cppcheck lint-all valgrind
