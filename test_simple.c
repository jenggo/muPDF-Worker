#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <json-c/json.h>
#include <mupdf/fitz.h>

// Test program to verify MuPDF parsing works
extern json_object* extract_text_blocks(fz_context *ctx, fz_document *doc);
extern json_object* extract_paragraphs(fz_context *ctx, fz_document *doc);
extern json_object* extract_images(fz_context *ctx, fz_document *doc, const char *image_directory_path, int extract_vector_images);

int main(int argc, char **argv) {
    if (argc != 2) {
        printf("Usage: %s <pdf_file>\n", argv[0]);
        return 1;
    }

    // Initialize MuPDF
    fz_context *ctx = fz_new_context(NULL, NULL, FZ_STORE_DEFAULT);
    if (!ctx) {
        fprintf(stderr, "Failed to initialize MuPDF context\n");
        return 1;
    }

    fz_register_document_handlers(ctx);

    fz_document *doc = NULL;
    json_object *response = json_object_new_object();

    fz_try(ctx) {
        doc = fz_open_document(ctx, argv[1]);

        printf("Processing document: %s\n", argv[1]);
        printf("Page count: %d\n", fz_count_pages(ctx, doc));

        // Extract content
        json_object *text_blocks = extract_text_blocks(ctx, doc);
        json_object *paragraphs = extract_paragraphs(ctx, doc);
        json_object *images = extract_images(ctx, doc, "/tmp/test_images", 1); // Test with test directory, vector extraction enabled

        // Add to response
        json_object_object_add(response, "text_blocks", text_blocks);
        json_object_object_add(response, "paragraphs", paragraphs);
        json_object_object_add(response, "images", images);

        // Print JSON response
        printf("\n=== JSON OUTPUT ===\n");
        printf("%s\n", json_object_to_json_string_ext(response, JSON_C_TO_STRING_PRETTY));
    }
    fz_catch(ctx) {
        fprintf(stderr, "Error: %s\n", fz_caught_message(ctx));
    }

    if (doc) fz_drop_document(ctx, doc);
    json_object_put(response);
    fz_drop_context(ctx);

    return 0;
}