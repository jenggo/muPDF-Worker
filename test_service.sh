#!/bin/bash

# Test script for MuPDF parsing service

echo "Starting MuPDF parsing service test..."

# Start the service in background
./mupdf-parser &
SERVICE_PID=$!

# Wait for service to start
sleep 3

echo "Testing health endpoint..."
curl -s http://localhost:8080/health
echo ""

echo "Testing document parsing with eos.pdf..."
curl -X POST http://localhost:8080/parse \
  -H "Content-Type: application/json" \
  -d '{"file_path": "../eos.pdf", "job_id": "test-123", "callback_url": "http://httpbin.org/post"}' \
  --silent | python3 -m json.tool

echo ""

# Wait a bit for processing
sleep 5

echo "Stopping service..."
kill $SERVICE_PID 2>/dev/null

wait $SERVICE_PID 2>/dev/null

echo "Test completed."