/*
 * MuPDF Document Content Extraction
 *
 * Implements detailed text, paragraph, and image extraction
 * with spatial coordinate information for the parsing service.
 */

#include <json-c/json.h>
#include <mupdf/fitz.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>

// External function declarations (from main.c)
extern void get_org_images_path(char *buffer, size_t buffer_size, const char *image_directory_path);

// Internal function declarations
// Object-level vector extraction structures
typedef struct {
    fz_rect bbox;           // Bounding box of the graphic object
    int path_count;         // Number of vector paths in this object
    int text_overlap;       // Whether object overlaps with text
    float density;          // Drawing density (complexity measure)
    int is_decorative;      // Whether this appears to be decorative (lines, borders)
    int is_instructional;   // Whether this appears to be instructional content
} vector_object_t;

typedef struct {
    vector_object_t *objects;
    int count;
    int capacity;
} vector_object_list_t;

static json_object* extract_text_blocks_from_page(fz_context *ctx, fz_stext_page *stext_page, int page_num);
static json_object* extract_paragraphs_from_page(fz_context *ctx, fz_stext_page *stext_page, int page_num);
static json_object* extract_images_from_page_filtered(fz_context *ctx, fz_page *page, int page_num, const char *image_directory_path, int no_filter);
static json_object* extract_vector_objects_from_page(fz_context *ctx, fz_page *page, int page_num, const char *image_directory_path, int *vector_object_count);
static vector_object_list_t* analyze_page_vector_objects(fz_context *ctx, fz_page *page);
static int is_meaningful_vector_object(vector_object_t *obj, float page_width, float page_height);
static int calculate_text_density_in_region(fz_context *ctx, fz_page *page, fz_rect region);
static int objects_overlap(vector_object_t *obj1, vector_object_t *obj2);
static void merge_overlapping_objects(vector_object_list_t *objects);
static void classify_vector_objects(fz_context *ctx, fz_page *page, vector_object_list_t *objects);
static fz_rect expand_bbox_for_context(fz_rect bbox, float expansion_factor);
static json_object* create_bounding_box_json(fz_rect bbox);
static json_object* create_quad_json(fz_quad quad);
static int is_paragraph_break(fz_stext_line *current_line, fz_stext_line *next_line);
static int is_meaningful_image(fz_context *ctx, fz_image *image, fz_rect bbox, int no_filter);

/*
 * Extract text blocks with character-level coordinates
 */
json_object* extract_text_blocks(fz_context *ctx, fz_document *doc) {
    json_object *all_blocks = json_object_new_array();
    int page_count = fz_count_pages(ctx, doc);

    for (int page_num = 0; page_num < page_count; page_num++) {
        fz_page *page = NULL;
        fz_stext_page *stext_page = NULL;

        fz_try(ctx) {
            page = fz_load_page(ctx, doc, page_num);

            // Create text extraction options
            fz_stext_options opts = { 0 };
            opts.flags = FZ_STEXT_PRESERVE_WHITESPACE | FZ_STEXT_PRESERVE_IMAGES;

            stext_page = fz_new_stext_page_from_page(ctx, page, &opts);

            json_object *page_blocks = extract_text_blocks_from_page(ctx, stext_page, page_num + 1);

            // Add each block to the overall array
            int block_count = (int)json_object_array_length(page_blocks);
            for (int i = 0; i < block_count; i++) {
                json_object *block = json_object_array_get_idx(page_blocks, i);
                json_object_array_add(all_blocks, json_object_get(block));
            }

            json_object_put(page_blocks);
        }
        fz_always(ctx) {
            if (stext_page) {
                fz_drop_stext_page(ctx, stext_page);
            }
            if (page) {
                fz_drop_page(ctx, page);
            }
        }
        fz_catch(ctx) {
            (void)fprintf(stderr, "Error processing page %d for text blocks\n", page_num + 1);
            continue;
        }
    }

    return all_blocks;
}

/*
 * Extract paragraph-level bounding boxes
 */
json_object* extract_paragraphs(fz_context *ctx, fz_document *doc) {
    json_object *all_paragraphs = json_object_new_array();
    int page_count = fz_count_pages(ctx, doc);

    for (int page_num = 0; page_num < page_count; page_num++) {
        fz_page *page = NULL;
        fz_stext_page *stext_page = NULL;

        fz_try(ctx) {
            page = fz_load_page(ctx, doc, page_num);

            fz_stext_options opts = { 0 };
            opts.flags = FZ_STEXT_PRESERVE_WHITESPACE;

            stext_page = fz_new_stext_page_from_page(ctx, page, &opts);

            json_object *page_paragraphs = extract_paragraphs_from_page(ctx, stext_page, page_num + 1);

            // Add each paragraph to the overall array
            int para_count = (int)json_object_array_length(page_paragraphs);
            for (int i = 0; i < para_count; i++) {
                json_object *paragraph = json_object_array_get_idx(page_paragraphs, i);
                json_object_array_add(all_paragraphs, json_object_get(paragraph));
            }

            json_object_put(page_paragraphs);
        }
        fz_always(ctx) {
            if (stext_page) {
                fz_drop_stext_page(ctx, stext_page);
            }
            if (page) {
                fz_drop_page(ctx, page);
            }
        }
        fz_catch(ctx) {
            (void)fprintf(stderr, "Error processing page %d for paragraphs\n", page_num + 1);
            continue;
        }
    }

    return all_paragraphs;
}

/*
 * Extract embedded images with spatial coordinates
 */
json_object* extract_images_filtered(fz_context *ctx, fz_document *doc, const char *image_directory_path, int extract_vector_images, int no_filter) {
    json_object *all_images = json_object_new_array();
    int page_count = fz_count_pages(ctx, doc);

    for (int page_num = 0; page_num < page_count; page_num++) {
        fz_page *page = NULL;

        fz_try(ctx) {
            page = fz_load_page(ctx, doc, page_num);

            printf("[IMAGE_DEBUG] Processing page %d for image extraction\n", page_num + 1);

            // Extract embedded raster images
            json_object *page_images = extract_images_from_page_filtered(ctx, page, page_num + 1, image_directory_path, no_filter);
            int raster_count = (int)json_object_array_length(page_images);

            // Extract vector images by rendering (only if enabled)
            int vector_count = 0;
            json_object *vector_images = NULL;
            if (extract_vector_images) {
                vector_images = extract_vector_objects_from_page(ctx, page, page_num + 1, image_directory_path, &vector_count);
                printf("[IMAGE_DEBUG] Found %d raster + %d vector objects on page %d\n",
                       raster_count, vector_count, page_num + 1);
            } else {
                vector_images = json_object_new_array();
                printf("[IMAGE_DEBUG] Found %d raster images on page %d (vector extraction disabled)\n",
                       raster_count, page_num + 1);
            }

            // Add raster images to overall array
            for (int i = 0; i < raster_count; i++) {
                json_object *image = json_object_array_get_idx(page_images, i);
                json_object_array_add(all_images, json_object_get(image));
            }

            // Add vector images to overall array
            for (int i = 0; i < vector_count; i++) {
                json_object *image = json_object_array_get_idx(vector_images, i);
                json_object_array_add(all_images, json_object_get(image));
            }

            json_object_put(page_images);
            json_object_put(vector_images);
        }
        fz_always(ctx) {
            if (page) {
                fz_drop_page(ctx, page);
            }
        }
        fz_catch(ctx) {
            (void)fprintf(stderr, "Error processing page %d for images\n", page_num + 1);
            continue;
        }
    }

    return all_images;
}

/*
 * Extract text blocks from a single page with detailed coordinates
 */
static json_object* extract_text_blocks_from_page(fz_context *ctx __attribute__((unused)), fz_stext_page *stext_page, int page_num) {
    json_object *blocks_array = json_object_new_array();

    for (fz_stext_block *block = stext_page->first_block; block; block = block->next) {
        if (block->type != FZ_STEXT_BLOCK_TEXT) {
            continue;
        }

        json_object *block_obj = json_object_new_object();
        json_object *lines_array = json_object_new_array();

        // Add block metadata
        json_object_object_add(block_obj, "page", json_object_new_int(page_num));
        json_object_object_add(block_obj, "type", json_object_new_string("text"));
        json_object_object_add(block_obj, "bbox", create_bounding_box_json(block->bbox));

        // Extract text content and build full block text
        char *block_text = malloc(4096);
        block_text[0] = '\0';
        size_t block_text_len = 0;

        for (fz_stext_line *line = block->u.t.first_line; line; line = line->next) {
            json_object *line_obj = json_object_new_object();
            json_object *chars_array = json_object_new_array();

            // Add line metadata
            json_object_object_add(line_obj, "bbox", create_bounding_box_json(line->bbox));
            json_object_object_add(line_obj, "wmode", json_object_new_int(line->wmode));

            // Extract characters with coordinates
            char *line_text = malloc(1024);
            line_text[0] = '\0';
            size_t line_text_len = 0;

            for (fz_stext_char *ch = line->first_char; ch; ch = ch->next) {
                json_object *char_obj = json_object_new_object();

                // Character data
                char char_str[5] = {0};  // UTF-8 character (up to 4 bytes + null)
                int char_len = fz_runetochar(char_str, ch->c);
                char_str[char_len] = '\0';

                json_object_object_add(char_obj, "c", json_object_new_string(char_str));
                json_object_object_add(char_obj, "origin", json_object_new_object());
                json_object_object_add(json_object_object_get(char_obj, "origin"), "x", json_object_new_double(ch->origin.x));
                json_object_object_add(json_object_object_get(char_obj, "origin"), "y", json_object_new_double(ch->origin.y));
                json_object_object_add(char_obj, "size", json_object_new_double(ch->size));
                json_object_object_add(char_obj, "quad", create_quad_json(ch->quad));

                json_object_array_add(chars_array, char_obj);

                // Build line text
                if (line_text_len + char_len < 1023) {
                    strncat(line_text, char_str, 1024 - line_text_len - 1);
                    line_text_len += char_len;
                }
            }

            json_object_object_add(line_obj, "characters", chars_array);
            json_object_object_add(line_obj, "text", json_object_new_string(line_text));
            json_object_array_add(lines_array, line_obj);

            // Build block text
            if (block_text_len + line_text_len + 1 < 4095) {
                strncat(block_text, line_text, 4096 - block_text_len - 1);
                strncat(block_text, "\n", 4096 - strlen(block_text) - 1);
                block_text_len += line_text_len + 1;
            }

            free(line_text);
        }

        json_object_object_add(block_obj, "lines", lines_array);
        json_object_object_add(block_obj, "text", json_object_new_string(block_text));
        json_object_array_add(blocks_array, block_obj);

        free(block_text);
    }

    return blocks_array;
}

/*
 * Extract paragraphs by analyzing line spacing and indentation
 */
static json_object* extract_paragraphs_from_page(fz_context *ctx __attribute__((unused)), fz_stext_page *stext_page, int page_num) {
    json_object *paragraphs_array = json_object_new_array();

    for (fz_stext_block *block = stext_page->first_block; block; block = block->next) {
        if (block->type != FZ_STEXT_BLOCK_TEXT) {
            continue;
        }

        // Current paragraph data
        json_object *current_para = NULL;
        json_object *para_lines = NULL;
        char *para_text = NULL;
        size_t para_text_len = 0;
        fz_rect para_bbox = {0, 0, 0, 0};
        int para_line_count = 0;

        for (fz_stext_line *line = block->u.t.first_line; line; line = line->next) {
            int is_new_paragraph = 0;

            // Determine if this starts a new paragraph
            if (!current_para) {
                is_new_paragraph = 1;  // First line
            } else {
                is_new_paragraph = is_paragraph_break(line->prev, line);
            }

            if (is_new_paragraph) {
                // Finish previous paragraph
                if (current_para) {
                    json_object_object_add(current_para, "lines", para_lines);
                    json_object_object_add(current_para, "text", json_object_new_string(para_text ? para_text : ""));
                    json_object_object_add(current_para, "bbox", create_bounding_box_json(para_bbox));
                    json_object_object_add(current_para, "line_count", json_object_new_int(para_line_count));
                    json_object_array_add(paragraphs_array, current_para);

                    if (para_text) {
                        free(para_text);
                    }
                }

                // Start new paragraph
                current_para = json_object_new_object();
                para_lines = json_object_new_array();
                para_text = malloc(2048);
                para_text[0] = '\0';
                para_text_len = 0;
                para_bbox = line->bbox;
                para_line_count = 0;

                json_object_object_add(current_para, "page", json_object_new_int(page_num));
                json_object_object_add(current_para, "type", json_object_new_string("paragraph"));
            }

            // Add line to current paragraph
            json_object *line_obj = json_object_new_object();
            json_object_object_add(line_obj, "bbox", create_bounding_box_json(line->bbox));

            // Extract line text
            char *line_text = malloc(512);
            line_text[0] = '\0';
            for (fz_stext_char *ch = line->first_char; ch; ch = ch->next) {
                char char_str[5] = {0};
                int char_len = fz_runetochar(char_str, ch->c);
                char_str[char_len] = '\0';
                if (strlen(line_text) + char_len < 511) {
                    strncat(line_text, char_str, 512 - strlen(line_text) - 1);
                }
            }

            json_object_object_add(line_obj, "text", json_object_new_string(line_text));
            json_object_array_add(para_lines, line_obj);

            // Update paragraph text and bounding box
            if (para_text_len + strlen(line_text) + 2 < 2047) {
                strncat(para_text, line_text, 2048 - para_text_len - 1);
                strncat(para_text, " ", 2048 - strlen(para_text) - 1);
                para_text_len += strlen(line_text) + 1;
            }

            para_bbox = fz_union_rect(para_bbox, line->bbox);
            para_line_count++;

            free(line_text);
        }

        // Finish final paragraph
        if (current_para) {
            json_object_object_add(current_para, "lines", para_lines);
            json_object_object_add(current_para, "text", json_object_new_string(para_text ? para_text : ""));
            json_object_object_add(current_para, "bbox", create_bounding_box_json(para_bbox));
            json_object_object_add(current_para, "line_count", json_object_new_int(para_line_count));
            json_object_array_add(paragraphs_array, current_para);

            if (para_text) {
                free(para_text);
            }
        }
    }

    return paragraphs_array;
}

/*
 * Extract images from page using stext (for embedded images)
 */
static json_object* extract_images_from_page_filtered(fz_context *ctx, fz_page *page, int page_num, const char *image_directory_path, int no_filter) {
    json_object *images_array = json_object_new_array();

    fz_stext_page *stext_page = NULL;

    fz_try(ctx) {
        fz_stext_options opts = { 0 };
        opts.flags = FZ_STEXT_PRESERVE_IMAGES;

        stext_page = fz_new_stext_page_from_page(ctx, page, &opts);

        int image_id = 0;
        for (fz_stext_block *block = stext_page->first_block; block; block = block->next) {
            if (block->type != FZ_STEXT_BLOCK_IMAGE) {
                continue;
            }

            json_object *image_obj = json_object_new_object();

            // Image metadata
            json_object_object_add(image_obj, "page", json_object_new_int(page_num));
            json_object_object_add(image_obj, "image_id", json_object_new_int(image_id++));
            json_object_object_add(image_obj, "type", json_object_new_string("image"));
            json_object_object_add(image_obj, "bbox", create_bounding_box_json(block->bbox));

            // Transform matrix
            json_object *transform_obj = json_object_new_object();
            json_object_object_add(transform_obj, "a", json_object_new_double(block->u.i.transform.a));
            json_object_object_add(transform_obj, "b", json_object_new_double(block->u.i.transform.b));
            json_object_object_add(transform_obj, "c", json_object_new_double(block->u.i.transform.c));
            json_object_object_add(transform_obj, "d", json_object_new_double(block->u.i.transform.d));
            json_object_object_add(transform_obj, "e", json_object_new_double(block->u.i.transform.e));
            json_object_object_add(transform_obj, "f", json_object_new_double(block->u.i.transform.f));
            json_object_object_add(image_obj, "transform", transform_obj);

            // Image properties
            if (block->u.i.image) {
                fz_image *img = block->u.i.image;

                // Debug: Log all images found for analysis
                printf("[IMAGE_DEBUG] Found image on page %d: %dx%d, bpc=%d, n=%d, bbox=%.1f,%.1f,%.1f,%.1f\n",
                       page_num, img->w, img->h, img->bpc, img->n,
                       block->bbox.x0, block->bbox.y0, block->bbox.x1, block->bbox.y1);

                // Apply intelligent filtering to remove gradients, patterns, and decorative elements
                if (!is_meaningful_image(ctx, img, block->bbox, no_filter)) {
                    printf("[IMAGE_DEBUG] Image filtered out as non-meaningful\n");
                    json_object_put(image_obj); // Free the unused object
                    image_id--; // Don't increment ID for filtered images
                    continue;
                }
                printf("[IMAGE_DEBUG] Image passed filters, including in results\n");

                json_object_object_add(image_obj, "width", json_object_new_int(img->w));
                json_object_object_add(image_obj, "height", json_object_new_int(img->h));
                json_object_object_add(image_obj, "bpc", json_object_new_int(img->bpc));
                json_object_object_add(image_obj, "n", json_object_new_int(img->n));

                // Extract and save image to shared volume
                const char *format = "png"; // Always save as PNG for consistency
                char image_path[512] = {0};

                fz_try(ctx) {
                    fz_buffer *buffer = fz_new_buffer_from_image_as_png(ctx, img, fz_default_color_params);
                    if (buffer) {
                        unsigned char *data = NULL;
                        size_t len = fz_buffer_storage(ctx, buffer, &data);

                        // Create organizational directory if it doesn't exist
                        char org_dir[256];
                        get_org_images_path(org_dir, sizeof(org_dir), image_directory_path);

                        // Generate organizational filename: org_id/documentID_page_imageId_timestamp.png
                        // Note: We don't have documentID in this context, so we'll use a simple pattern for now
                        (void)snprintf(image_path, sizeof(image_path),
                                "%s/page_%d_img_%d_%ld.png",
                                org_dir, page_num, image_id, (long)time(NULL));

                        // Save image data to file
                        FILE *img_file = fopen(image_path, "wb");
                        if (img_file) {
                            (void)fwrite(data, 1, len, img_file);
                            (void)fclose(img_file);
                            printf("[IMAGE_DEBUG] Saved image to: %s (%zu bytes)\n", image_path, len);
                        } else {
                            printf("[IMAGE_DEBUG] Failed to save image: %s\n", image_path);
                            image_path[0] = '\0'; // Clear path on failure
                        }

                        fz_drop_buffer(ctx, buffer);
                    }
                }
                fz_catch(ctx) {
                    printf("[IMAGE_DEBUG] Failed to extract image data\n");
                    image_path[0] = '\0'; // Clear path on failure
                }

                json_object_object_add(image_obj, "format", json_object_new_string(format));

                // Add image path if successfully saved
                if (image_path[0] != '\0') {
                    json_object_object_add(image_obj, "image_path", json_object_new_string(image_path));
                }
            }

            json_object_array_add(images_array, image_obj);
        }
    }
    fz_always(ctx) {
        if (stext_page) {
            fz_drop_stext_page(ctx, stext_page);
        }
    }
    fz_catch(ctx) {
        (void)fprintf(stderr, "Error extracting images from page %d\n", page_num);
    }

    return images_array;
}

/*
 * Enhanced image filtering to remove gradients, patterns, and decorative elements
 * Returns 1 if image is meaningful, 0 if it should be filtered out
 */
static int is_meaningful_image(fz_context *ctx, fz_image *image, fz_rect bbox, int no_filter) {
    if (!image) {
        return 0;
    }

    // If no_filter is enabled, bypass all filtering and accept all images
    if (no_filter) {
        printf("[IMAGE_FILTER] Filtering bypassed - accepting all images (no_filter=true)\n");
        return 1;
    }

    // Filter by minimum size (avoid tiny decorative elements)
    float width = bbox.x1 - bbox.x0;
    float height = bbox.y1 - bbox.y0;
    float min_dimension = 40.0F; // Increased minimum to 40 points (more selective)

    if (width < min_dimension || height < min_dimension) {
        printf("[IMAGE_FILTER] Image filtered out: too small (%.1fx%.1f)\n", width, height);
        return 0;
    }

    // Filter by aspect ratio (avoid very thin lines/borders/gradients)
    float aspect_ratio = width / height;
    if (aspect_ratio > 15.0F || aspect_ratio < 0.067F) { // Tightened from 20.0F and 0.05F
        printf("[IMAGE_FILTER] Image filtered out: extreme aspect ratio (%.2f)\n", aspect_ratio);
        return 0;
    }

    // Filter by image dimensions (more selective)
    if (image->w < 32 || image->h < 32) { // Increased from 16x16
        printf("[IMAGE_FILTER] Image filtered out: low resolution (%dx%d)\n", image->w, image->h);
        return 0;
    }

    // Filter by color depth - single color channel images are often decorative
    if (image->n == 1 && image->bpc <= 2) { // Include 2-bit images as likely decorative
        printf("[IMAGE_FILTER] Image filtered out: low bit depth (%d channels, %d bpc)\n", image->n, image->bpc);
        return 0;
    }

    // Filter very large images that are likely full-page backgrounds
    float area = width * height;
    if (area > 500000.0F) { // > ~700x700 points area
        printf("[IMAGE_FILTER] Image filtered out: too large, likely background (%.0f points²)\n", area);
        return 0;
    }

    // Enhanced pattern/gradient detection
    fz_buffer *buffer = NULL;
    int is_meaningful = 1;

    fz_try(ctx) {
        buffer = fz_new_buffer_from_image_as_png(ctx, image, fz_default_color_params);

        if (buffer) {
            unsigned char *data = NULL;
            size_t len = fz_buffer_storage(ctx, buffer, &data);

            // Calculate compression efficiency
            float compression_ratio = (float)len / (float)(image->w * image->h * image->n);

            // Gradient/pattern detection: very efficient compression suggests repetitive content
            if (compression_ratio < 0.05F) { // Tightened threshold
                printf("[IMAGE_FILTER] Image filtered out: likely gradient/pattern (compression ratio: %.3f)\n", compression_ratio);
                is_meaningful = 0;
            }

            // Size-based filtering for small compressed images (likely icons/decorations)
            else if (len < 2048 && (image->w * image->h) > 1600) { // Small file but large dimensions
                printf("[IMAGE_FILTER] Image filtered out: small file with large dimensions (likely simple pattern)\n");
                is_meaningful = 0;
            }

            // Very small files are usually simple graphics
            else if (len < 512) {
                printf("[IMAGE_FILTER] Image filtered out: very small file size (%zu bytes)\n", len);
                is_meaningful = 0;
            }
        }
    }
    fz_always(ctx) {
        if (buffer) {
            fz_drop_buffer(ctx, buffer);
        }
    }
    fz_catch(ctx) {
        // If analysis fails, err on the side of including the image
        printf("[IMAGE_FILTER] Analysis failed, including image by default\n");
        is_meaningful = 1;
    }

    if (is_meaningful) {
        printf("[IMAGE_FILTER] Image passed all filters: %.1fx%.1f, %dx%d, %d channels\n",
               width, height, image->w, image->h, image->n);
    }

    return is_meaningful;
}

/*
 * Utility functions for JSON coordinate objects
 */
static json_object* create_bounding_box_json(fz_rect bbox) {
    json_object *bbox_obj = json_object_new_object();
    json_object_object_add(bbox_obj, "x0", json_object_new_double(bbox.x0));
    json_object_object_add(bbox_obj, "y0", json_object_new_double(bbox.y0));
    json_object_object_add(bbox_obj, "x1", json_object_new_double(bbox.x1));
    json_object_object_add(bbox_obj, "y1", json_object_new_double(bbox.y1));
    json_object_object_add(bbox_obj, "width", json_object_new_double(bbox.x1 - bbox.x0));
    json_object_object_add(bbox_obj, "height", json_object_new_double(bbox.y1 - bbox.y0));
    return bbox_obj;
}

static json_object* create_quad_json(fz_quad quad) {
    json_object *quad_obj = json_object_new_object();

    json_object *upper_left = json_object_new_object();
    json_object_object_add(upper_left, "x", json_object_new_double(quad.ul.x));
    json_object_object_add(upper_left, "y", json_object_new_double(quad.ul.y));

    json_object *upper_right = json_object_new_object();
    json_object_object_add(upper_right, "x", json_object_new_double(quad.ur.x));
    json_object_object_add(upper_right, "y", json_object_new_double(quad.ur.y));

    json_object *lower_left = json_object_new_object();
    json_object_object_add(lower_left, "x", json_object_new_double(quad.ll.x));
    json_object_object_add(lower_left, "y", json_object_new_double(quad.ll.y));

    json_object *lower_right = json_object_new_object();
    json_object_object_add(lower_right, "x", json_object_new_double(quad.lr.x));
    json_object_object_add(lower_right, "y", json_object_new_double(quad.lr.y));

    json_object_object_add(quad_obj, "ul", upper_left);
    json_object_object_add(quad_obj, "ur", upper_right);
    json_object_object_add(quad_obj, "ll", lower_left);
    json_object_object_add(quad_obj, "lr", lower_right);

    return quad_obj;
}

/*
 * Determine if there's a paragraph break between two lines
 * Based on vertical spacing and indentation patterns
 */
static int is_paragraph_break(fz_stext_line *current_line, fz_stext_line *next_line) {
    if (!current_line || !next_line) {
        return 1;
    }

    // Calculate vertical spacing
    float line_height = current_line->bbox.y1 - current_line->bbox.y0;
    float spacing = next_line->bbox.y0 - current_line->bbox.y1;

    // Paragraph break if spacing is more than 1.5x line height
    if (spacing > line_height * 1.5) {
        return 1;
    }

    // Check for indentation changes (simple heuristic)
    float current_indent = current_line->bbox.x0;
    float next_indent = next_line->bbox.x0;

    // Paragraph break if next line is significantly indented
    if (fabsf(next_indent - current_indent) > 20.0F) {
        return 1;
    }

    return 0;  // Same paragraph
}

/*
 * Check if a vector area is meaningful for extraction
 */
/*
 * Extract vector objects from a single page using sophisticated object detection
 */
static json_object* extract_vector_objects_from_page(fz_context *ctx, fz_page *page, 
                                                   int page_num, const char *image_directory_path,
                                                   int *vector_object_count) {
    json_object *vector_objects_array = json_object_new_array();
    *vector_object_count = 0;

    fz_rect page_bounds = fz_bound_page(ctx, page);
    float page_width = page_bounds.x1 - page_bounds.x0;
    float page_height = page_bounds.y1 - page_bounds.y0;

    printf("[VECTOR_OBJ_DEBUG] Page %d bounds: %.1f,%.1f,%.1f,%.1f\n",
           page_num, page_bounds.x0, page_bounds.y0, page_bounds.x1, page_bounds.y1);

    // Analyze page to detect vector objects
    vector_object_list_t *objects = analyze_page_vector_objects(ctx, page);
    if (!objects || objects->count == 0) {
        printf("[VECTOR_OBJ_DEBUG] No vector objects detected on page %d\n", page_num);
        if (objects) {
            free(objects->objects);
            free(objects);
        }
        return vector_objects_array;
    }

    // Merge overlapping objects that likely belong together
    merge_overlapping_objects(objects);
    
    // Classify objects to identify instructional vs decorative content
    classify_vector_objects(ctx, page, objects);

    printf("[VECTOR_OBJ_DEBUG] Found %d potential vector objects on page %d\n", objects->count, page_num);

    // Process each meaningful vector object
    int object_id = 0;
    for (int i = 0; i < objects->count; i++) {
        vector_object_t *obj = &objects->objects[i];

        // Filter out non-meaningful objects
        if (!is_meaningful_vector_object(obj, page_width, page_height)) {
            printf("[VECTOR_OBJ_DEBUG] Object %d filtered out as non-meaningful\n", i);
            continue;
        }

        // Calculate text density in this region
        int text_density = calculate_text_density_in_region(ctx, page, obj->bbox);

        // Skip regions that are mostly text (>70% text coverage)
        if (text_density > 70) {
            printf("[VECTOR_OBJ_DEBUG] Object %d filtered out: %d%% text density (threshold: 70%%)\n",
                   i, text_density);
            continue;
        }

        printf("[VECTOR_OBJ_DEBUG] Object %d passed text filter: %d%% text density\n",
               i, text_density);

        // Expand bounding box slightly to capture complete context
        fz_rect render_bbox = expand_bbox_for_context(obj->bbox, 1.1f);
        
        // Ensure render bbox stays within page bounds
        render_bbox.x0 = fmaxf(render_bbox.x0, page_bounds.x0);
        render_bbox.y0 = fmaxf(render_bbox.y0, page_bounds.y0);
        render_bbox.x1 = fminf(render_bbox.x1, page_bounds.x1);
        render_bbox.y1 = fminf(render_bbox.y1, page_bounds.y1);

        // Render this specific object region
        fz_pixmap *pix = NULL;
        fz_device *draw_device = NULL;

        fz_try(ctx) {
            // Create high-resolution pixmap for this object
            fz_matrix transform = fz_scale(3.0f, 3.0f); // 3x resolution for quality
            fz_rect render_bounds = fz_transform_rect(render_bbox, transform);
            fz_irect ibounds = fz_round_rect(render_bounds);

            pix = fz_new_pixmap_with_bbox(ctx, fz_device_rgb(ctx), ibounds, NULL, 0);
            fz_clear_pixmap_with_value(ctx, pix, 255); // White background

            draw_device = fz_new_draw_device(ctx, transform, pix);

            // Render only this object region
            fz_run_page(ctx, page, draw_device, fz_identity, NULL);
            fz_close_device(ctx, draw_device);

            // Verify the rendered content has meaningful vector graphics
            int pixel_count = pix->w * pix->h;
            int non_white_pixels = 0;
            unsigned char *pixels = fz_pixmap_samples(ctx, pix);
            
            for (int p = 0; p < pixel_count; p++) {
                int pixel_offset = p * pix->n;
                // Check if pixel is not white (255,255,255)
                if (pixels[pixel_offset] != 255 || 
                    pixels[pixel_offset + 1] != 255 || 
                    pixels[pixel_offset + 2] != 255) {
                    non_white_pixels++;
                }
            }

            float content_ratio = (float)non_white_pixels / (float)pixel_count;
            
            // Only save objects with meaningful content
            if (content_ratio > 0.02f && content_ratio < 0.85f) { // 2-85% content
                // Save this vector object as an image
                char org_dir[256];
                get_org_images_path(org_dir, sizeof(org_dir), image_directory_path);

                char image_path[512];
                const char *object_type = obj->is_instructional ? "instruction" : "graphic";
                snprintf(image_path, sizeof(image_path),
                        "%s/page_%d_%s_%d_%ld.png",
                        org_dir, page_num, object_type, object_id, (long)time(NULL));

                fz_save_pixmap_as_png(ctx, pix, image_path);

                // Create JSON object for this vector object
                json_object *vector_obj = json_object_new_object();
                json_object_object_add(vector_obj, "page", json_object_new_int(page_num));
                json_object_object_add(vector_obj, "object_id", json_object_new_int(object_id));
                json_object_object_add(vector_obj, "type", json_object_new_string("vector_object"));
                json_object_object_add(vector_obj, "object_type", json_object_new_string(object_type));
                json_object_object_add(vector_obj, "bbox", create_bounding_box_json(obj->bbox));
                json_object_object_add(vector_obj, "render_bbox", create_bounding_box_json(render_bbox));
                json_object_object_add(vector_obj, "width", json_object_new_int(pix->w));
                json_object_object_add(vector_obj, "height", json_object_new_int(pix->h));
                json_object_object_add(vector_obj, "image_path", json_object_new_string(image_path));
                json_object_object_add(vector_obj, "content_ratio", json_object_new_double(content_ratio));
                json_object_object_add(vector_obj, "text_density_pct", json_object_new_int(text_density));
                json_object_object_add(vector_obj, "path_count", json_object_new_int(obj->path_count));
                json_object_object_add(vector_obj, "density", json_object_new_double(obj->density));
                json_object_object_add(vector_obj, "is_instructional", json_object_new_boolean(obj->is_instructional));

                json_object_array_add(vector_objects_array, vector_obj);
                object_id++;
                (*vector_object_count)++;

                printf("[VECTOR_OBJ_DEBUG] Extracted %s object %d: content_ratio=%.3f paths=%d path=%s\n",
                       object_type, object_id, content_ratio, obj->path_count, image_path);
            } else {
                printf("[VECTOR_OBJ_DEBUG] Object %d skipped: content_ratio=%.3f (outside meaningful range)\n",
                       i, content_ratio);
            }
        }
        fz_always(ctx) {
            if (draw_device) {
                fz_drop_device(ctx, draw_device);
            }
            if (pix) {
                fz_drop_pixmap(ctx, pix);
            }
        }
        fz_catch(ctx) {
            printf("[VECTOR_OBJ_DEBUG] Failed to render vector object %d\n", i);
        }
    }

    // Cleanup
    free(objects->objects);
    free(objects);

    return vector_objects_array;
}

/*
 * Analyze page to detect individual vector objects using display list analysis
 * This implementation uses intelligent sampling to detect concentrated areas of vector graphics
 */
static vector_object_list_t* analyze_page_vector_objects(fz_context *ctx, fz_page *page) {
    vector_object_list_t *objects = malloc(sizeof(vector_object_list_t));
    objects->objects = malloc(sizeof(vector_object_t) * 50); // Initial capacity
    objects->count = 0;
    objects->capacity = 50;

    fz_display_list *list = NULL;
    fz_device *list_device = NULL;
    fz_rect page_bounds = fz_bound_page(ctx, page);

    fz_try(ctx) {
        // Create display list to capture drawing operations
        list = fz_new_display_list(ctx, page_bounds);
        list_device = fz_new_list_device(ctx, list);

        // Run page through list device to capture vector operations
        fz_run_page(ctx, page, list_device, fz_identity, NULL);
        fz_close_device(ctx, list_device);

        // Use adaptive sampling based on page size to detect dense graphic areas
        float page_width = page_bounds.x1 - page_bounds.x0;
        float page_height = page_bounds.y1 - page_bounds.y0;
        
        // Use adaptive sampling based on page size
        int sample_cols = (int)(page_width / 30.0f); // Sample every ~30 points
        int sample_rows = (int)(page_height / 30.0f);
        sample_cols = fmaxf(8, fminf(sample_cols, 20)); // Keep reasonable bounds
        sample_rows = fmaxf(8, fminf(sample_rows, 20));

        float sample_width = page_width / (float)sample_cols;
        float sample_height = page_height / (float)sample_rows;

        // Create density map of vector content
        float **density_map = malloc(sample_rows * sizeof(float*));
        for (int i = 0; i < sample_rows; i++) {
            density_map[i] = calloc(sample_cols, sizeof(float));
        }

        // Sample each region for vector content density
        for (int row = 0; row < sample_rows; row++) {
            for (int col = 0; col < sample_cols; col++) {
                fz_rect sample_region;
                sample_region.x0 = page_bounds.x0 + ((float)col * sample_width);
                sample_region.y0 = page_bounds.y0 + ((float)row * sample_height);
                sample_region.x1 = sample_region.x0 + sample_width;
                sample_region.y1 = sample_region.y0 + sample_height;

                // Quick render test for this region
                fz_pixmap *test_pix = NULL;
                fz_device *test_device = NULL;

                fz_try(ctx) {
                    fz_matrix test_transform = fz_scale(1.5f, 1.5f);
                    fz_rect test_bounds = fz_transform_rect(sample_region, test_transform);
                    fz_irect test_ibounds = fz_round_rect(test_bounds);

                    test_pix = fz_new_pixmap_with_bbox(ctx, fz_device_rgb(ctx), test_ibounds, NULL, 0);
                    fz_clear_pixmap_with_value(ctx, test_pix, 255);

                    test_device = fz_new_draw_device(ctx, test_transform, test_pix);
                    fz_run_display_list(ctx, list, test_device, fz_identity, sample_region, NULL);
                    fz_close_device(ctx, test_device);

                    // Calculate density
                    int test_pixels = test_pix->w * test_pix->h;
                    int non_white = 0;
                    unsigned char *test_data = fz_pixmap_samples(ctx, test_pix);
                    
                    for (int p = 0; p < test_pixels; p++) {
                        int offset = p * test_pix->n;
                        if (test_data[offset] != 255 || test_data[offset + 1] != 255 || test_data[offset + 2] != 255) {
                            non_white++;
                        }
                    }
                    
                    density_map[row][col] = (float)non_white / (float)test_pixels;
                }
                fz_always(ctx) {
                    if (test_device) fz_drop_device(ctx, test_device);
                    if (test_pix) fz_drop_pixmap(ctx, test_pix);
                }
                fz_catch(ctx) {
                    density_map[row][col] = 0.0f;
                }
            }
        }

        // Find connected regions of high density (potential vector objects)
        int **visited = malloc(sample_rows * sizeof(int*));
        for (int i = 0; i < sample_rows; i++) {
            visited[i] = calloc(sample_cols, sizeof(int));
        }

        float density_threshold = 0.05f; // 5% minimum density for vector content

        for (int row = 0; row < sample_rows; row++) {
            for (int col = 0; col < sample_cols; col++) {
                if (!visited[row][col] && density_map[row][col] > density_threshold) {
                    // Found start of a potential vector object - do flood fill
                    if (objects->count >= objects->capacity) {
                        objects->capacity *= 2;
                        objects->objects = realloc(objects->objects, sizeof(vector_object_t) * objects->capacity);
                    }

                    vector_object_t *obj = &objects->objects[objects->count];
                    
                    // Initialize bounding box
                    obj->bbox.x0 = page_bounds.x0 + ((float)col * sample_width);
                    obj->bbox.y0 = page_bounds.y0 + ((float)row * sample_height);
                    obj->bbox.x1 = obj->bbox.x0 + sample_width;
                    obj->bbox.y1 = obj->bbox.y0 + sample_height;
                    obj->path_count = 0;
                    obj->density = density_map[row][col];
                    obj->text_overlap = 0;
                    obj->is_decorative = 0;
                    obj->is_instructional = 0;

                    // Simple flood fill to expand object bounds
                    int stack_size = 100;
                    int stack_top = 0;
                    int stack_row[100], stack_col[100];
                    
                    stack_row[stack_top] = row;
                    stack_col[stack_top] = col;
                    stack_top++;
                    visited[row][col] = 1;

                    while (stack_top > 0) {
                        stack_top--;
                        int curr_row = stack_row[stack_top];
                        int curr_col = stack_col[stack_top];

                        // Update bounding box
                        float region_x0 = page_bounds.x0 + ((float)curr_col * sample_width);
                        float region_y0 = page_bounds.y0 + ((float)curr_row * sample_height);
                        float region_x1 = region_x0 + sample_width;
                        float region_y1 = region_y0 + sample_height;

                        obj->bbox.x0 = fminf(obj->bbox.x0, region_x0);
                        obj->bbox.y0 = fminf(obj->bbox.y0, region_y0);
                        obj->bbox.x1 = fmaxf(obj->bbox.x1, region_x1);
                        obj->bbox.y1 = fmaxf(obj->bbox.y1, region_y1);
                        obj->density = fmaxf(obj->density, density_map[curr_row][curr_col]);
                        obj->path_count++;

                        // Check neighbors
                        for (int dr = -1; dr <= 1; dr++) {
                            for (int dc = -1; dc <= 1; dc++) {
                                int nr = curr_row + dr;
                                int nc = curr_col + dc;
                                
                                if (nr >= 0 && nr < sample_rows && nc >= 0 && nc < sample_cols &&
                                    !visited[nr][nc] && density_map[nr][nc] > density_threshold &&
                                    stack_top < stack_size - 1) {
                                    
                                    stack_row[stack_top] = nr;
                                    stack_col[stack_top] = nc;
                                    stack_top++;
                                    visited[nr][nc] = 1;
                                }
                            }
                        }
                    }

                    objects->count++;
                    printf("[VECTOR_OBJ_DEBUG] Detected vector object %d: bbox=%.1f,%.1f,%.1f,%.1f density=%.3f paths=%d\n",
                           objects->count - 1, obj->bbox.x0, obj->bbox.y0, obj->bbox.x1, obj->bbox.y1, 
                           obj->density, obj->path_count);
                }
            }
        }

        // Cleanup
        for (int i = 0; i < sample_rows; i++) {
            free(density_map[i]);
            free(visited[i]);
        }
        free(density_map);
        free(visited);
    }
    fz_always(ctx) {
        if (list_device) fz_drop_device(ctx, list_device);
        if (list) fz_drop_display_list(ctx, list);
    }
    fz_catch(ctx) {
        printf("[VECTOR_OBJ_DEBUG] Failed to analyze vector objects\n");
    }

    return objects;
}

/*
 * Calculate text density (percentage) in a given region
 * Returns 0-100 representing percentage of region covered by text
 */
static int calculate_text_density_in_region(fz_context *ctx, fz_page *page, fz_rect region) {
    fz_stext_page *stext = NULL;
    int text_density_pct = 0;

    fz_try(ctx) {
        // Extract text structure from page
        fz_stext_options opts = { 0 };
        opts.flags = FZ_STEXT_PRESERVE_WHITESPACE;
        stext = fz_new_stext_page_from_page(ctx, page, &opts);

        int char_count = 0;
        float region_area = (region.x1 - region.x0) * (region.y1 - region.y0);

        if (region_area <= 0) {
            fz_drop_stext_page(ctx, stext);
            return 0;
        }

        // Count characters that intersect with the region
        for (fz_stext_block *block = stext->first_block; block; block = block->next) {
            if (block->type != FZ_STEXT_BLOCK_TEXT)
                continue;

            for (fz_stext_line *line = block->u.t.first_line; line; line = line->next) {
                for (fz_stext_char *ch = line->first_char; ch; ch = ch->next) {
                    fz_rect char_bbox = fz_rect_from_quad(ch->quad);

                    // Check if character intersects with region
                    fz_rect intersection = fz_intersect_rect(char_bbox, region);
                    if (!fz_is_empty_rect(intersection)) {
                        char_count++;
                    }
                }
            }
        }

        // Estimate text coverage
        // Average character size: 7 pixels wide x 12 pixels high
        float avg_char_area = 7.0f * 12.0f;
        float estimated_text_area = char_count * avg_char_area;
        float text_coverage = estimated_text_area / region_area;

        // Convert to percentage (0-100), cap at 100
        text_density_pct = (int)(text_coverage * 100.0f);
        if (text_density_pct > 100) text_density_pct = 100;

    }
    fz_always(ctx) {
        if (stext) {
            fz_drop_stext_page(ctx, stext);
        }
    }
    fz_catch(ctx) {
        printf("[TEXT_DENSITY_DEBUG] Failed to calculate text density\n");
        return 0;
    }

    return text_density_pct;
}

/*
 * Check if a vector object is meaningful for RAG ingestion
 */
static int is_meaningful_vector_object(vector_object_t *obj, float page_width, float page_height) {
    float width = obj->bbox.x1 - obj->bbox.x0;
    float height = obj->bbox.y1 - obj->bbox.y0;
    float area = width * height;
    float page_area = page_width * page_height;

    // Filter out tiny objects (likely artifacts)
    if (width < 25.0f || height < 25.0f) {
        return 0;
    }

    // Filter out objects that cover most of the page (likely backgrounds)
    if (area > (page_area * 0.7f)) {
        return 0;
    }

    // Filter out very thin objects (likely lines/borders) unless they're instructional
    float aspect_ratio = width / height;
    if (!obj->is_instructional && (aspect_ratio > 20.0f || aspect_ratio < 0.05f)) {
        return 0;
    }

    // Filter out very low density objects (likely scattered elements)
    if (obj->density < 0.03f) {
        return 0;
    }

    // Filter out purely decorative elements
    if (obj->is_decorative && !obj->is_instructional) {
        return 0;
    }

    return 1;
}

/*
 * Check if two vector objects overlap significantly
 */
static int objects_overlap(vector_object_t *obj1, vector_object_t *obj2) {
    fz_rect intersection;
    intersection.x0 = fmaxf(obj1->bbox.x0, obj2->bbox.x0);
    intersection.y0 = fmaxf(obj1->bbox.y0, obj2->bbox.y0);
    intersection.x1 = fminf(obj1->bbox.x1, obj2->bbox.x1);
    intersection.y1 = fminf(obj1->bbox.y1, obj2->bbox.y1);

    if (intersection.x0 >= intersection.x1 || intersection.y0 >= intersection.y1) {
        return 0; // No overlap
    }

    float overlap_area = (intersection.x1 - intersection.x0) * (intersection.y1 - intersection.y0);
    float obj1_area = (obj1->bbox.x1 - obj1->bbox.x0) * (obj1->bbox.y1 - obj1->bbox.y0);
    float obj2_area = (obj2->bbox.x1 - obj2->bbox.x0) * (obj2->bbox.y1 - obj2->bbox.y0);
    
    float min_area = fminf(obj1_area, obj2_area);
    
    // Consider overlap if intersection is more than 30% of smaller object
    return (overlap_area / min_area) > 0.3f;
}

/*
 * Merge overlapping vector objects that likely belong together
 */
static void merge_overlapping_objects(vector_object_list_t *objects) {
    for (int i = 0; i < objects->count; i++) {
        for (int j = i + 1; j < objects->count; j++) {
            if (objects_overlap(&objects->objects[i], &objects->objects[j])) {
                // Merge object j into object i
                vector_object_t *obj1 = &objects->objects[i];
                vector_object_t *obj2 = &objects->objects[j];

                obj1->bbox.x0 = fminf(obj1->bbox.x0, obj2->bbox.x0);
                obj1->bbox.y0 = fminf(obj1->bbox.y0, obj2->bbox.y0);
                obj1->bbox.x1 = fmaxf(obj1->bbox.x1, obj2->bbox.x1);
                obj1->bbox.y1 = fmaxf(obj1->bbox.y1, obj2->bbox.y1);
                obj1->path_count += obj2->path_count;
                obj1->density = fmaxf(obj1->density, obj2->density);
                obj1->is_instructional = obj1->is_instructional || obj2->is_instructional;

                // Remove object j by shifting remaining objects
                for (int k = j; k < objects->count - 1; k++) {
                    objects->objects[k] = objects->objects[k + 1];
                }
                objects->count--;
                j--; // Check the same position again
            }
        }
    }
}

/*
 * Classify vector objects to identify instructional vs decorative content
 * This is a simplified heuristic-based approach
 */
static void classify_vector_objects(fz_context *ctx, fz_page *page, vector_object_list_t *objects) {
    fz_rect page_bounds = fz_bound_page(ctx, page);
    float page_width = page_bounds.x1 - page_bounds.x0;
    float page_height = page_bounds.y1 - page_bounds.y0;

    for (int i = 0; i < objects->count; i++) {
        vector_object_t *obj = &objects->objects[i];
        float width = obj->bbox.x1 - obj->bbox.x0;
        float height = obj->bbox.y1 - obj->bbox.y0;
        float area = width * height;
        float page_area = page_width * page_height;

        // Heuristics for instructional content
        // 1. Medium-sized objects (not too small, not too large)
        float area_ratio = area / page_area;
        if (area_ratio > 0.02f && area_ratio < 0.3f) {
            obj->is_instructional = 1;
        }

        // 2. Objects with good density and complexity
        if (obj->density > 0.1f && obj->path_count > 5) {
            obj->is_instructional = 1;
        }

        // 3. Objects in certain regions (center, specific areas)
        float center_x = page_bounds.x0 + page_width * 0.5f;
        float center_y = page_bounds.y0 + page_height * 0.5f;
        float obj_center_x = obj->bbox.x0 + width * 0.5f;
        float obj_center_y = obj->bbox.y0 + height * 0.5f;
        
        float dist_from_center = sqrtf(powf(obj_center_x - center_x, 2) + powf(obj_center_y - center_y, 2));
        float max_dist = sqrtf(powf(page_width * 0.5f, 2) + powf(page_height * 0.5f, 2));
        
        if (dist_from_center < max_dist * 0.6f) { // Within 60% of center
            obj->is_instructional = 1;
        }

        // Heuristics for decorative content
        // 1. Very thin or wide objects (likely borders, lines)
        float aspect_ratio = width / height;
        if (aspect_ratio > 15.0f || aspect_ratio < 0.067f) {
            obj->is_decorative = 1;
        }

        // 2. Objects along edges
        float edge_threshold = 30.0f;
        if (obj->bbox.x0 < page_bounds.x0 + edge_threshold ||
            obj->bbox.x1 > page_bounds.x1 - edge_threshold ||
            obj->bbox.y0 < page_bounds.y0 + edge_threshold ||
            obj->bbox.y1 > page_bounds.y1 - edge_threshold) {
            obj->is_decorative = 1;
        }

        // 3. Very low density objects
        if (obj->density < 0.05f) {
            obj->is_decorative = 1;
        }
    }
}

/*
 * Expand bounding box by a factor to capture complete context
 */
static fz_rect expand_bbox_for_context(fz_rect bbox, float expansion_factor) {
    float width = bbox.x1 - bbox.x0;
    float height = bbox.y1 - bbox.y0;
    float expansion_x = width * (expansion_factor - 1.0f) * 0.5f;
    float expansion_y = height * (expansion_factor - 1.0f) * 0.5f;

    fz_rect expanded;
    expanded.x0 = bbox.x0 - expansion_x;
    expanded.y0 = bbox.y0 - expansion_y;
    expanded.x1 = bbox.x1 + expansion_x;
    expanded.y1 = bbox.y1 + expansion_y;

    return expanded;
}