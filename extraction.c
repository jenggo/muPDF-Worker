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
static json_object* extract_text_blocks_from_page(fz_context *ctx, fz_stext_page *stext_page, int page_num);
static json_object* extract_paragraphs_from_page(fz_context *ctx, fz_stext_page *stext_page, int page_num);
static json_object* extract_images_from_page_filtered(fz_context *ctx, fz_page *page, int page_num, const char *image_directory_path, int no_filter);
static json_object* extract_vector_images_from_page(fz_context *ctx, fz_page *page, int page_num, const char *image_directory_path, int *vector_image_count);
static json_object* create_bounding_box_json(fz_rect bbox);
static json_object* create_quad_json(fz_quad quad);
static int is_paragraph_break(fz_stext_line *current_line, fz_stext_line *next_line);
static int is_meaningful_image(fz_context *ctx, fz_image *image, fz_rect bbox, int no_filter);
static int is_meaningful_vector_area(fz_rect bbox, float page_width, float page_height);

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
                vector_images = extract_vector_images_from_page(ctx, page, page_num + 1, image_directory_path, &vector_count);
                printf("[IMAGE_DEBUG] Found %d raster + %d vector images on page %d\n",
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
static int is_meaningful_vector_area(fz_rect bbox, float page_width, float page_height) {
    float width = bbox.x1 - bbox.x0;
    float height = bbox.y1 - bbox.y0;
    float area = width * height;
    float page_area = page_width * page_height;

    // Filter out very small areas (likely decorative elements)
    if (width < 50.0F || height < 50.0F) {
        return 0;
    }

    // Filter out areas that cover most of the page (likely backgrounds)
    if (area > (page_area * 0.8F)) {
        return 0;
    }

    // Filter out very thin/wide areas (likely lines or borders)
    float aspect_ratio = width / height;
    if (aspect_ratio > 10.0F || aspect_ratio < 0.1F) {
        return 0;
    }

    return 1;
}

/*
 * Extract vector images from page by rendering display list regions
 */
static json_object* extract_vector_images_from_page(fz_context *ctx, fz_page *page, int page_num, const char *image_directory_path, int *vector_image_count) {
    json_object *vector_images = json_object_new_array();
    *vector_image_count = 0;

    fz_display_list *list = NULL;
    fz_device *list_device = NULL;
    fz_rect page_bounds;

    fz_try(ctx) {
        // Get page bounds
        page_bounds = fz_bound_page(ctx, page);
        printf("[VECTOR_DEBUG] Page %d bounds: %.1f,%.1f,%.1f,%.1f\n",
               page_num, page_bounds.x0, page_bounds.y0, page_bounds.x1, page_bounds.y1);

        // Create display list to capture all drawing operations
        list = fz_new_display_list(ctx, page_bounds);
        list_device = fz_new_list_device(ctx, list);

        // Run page through list device to capture vector operations
        fz_run_page(ctx, page, list_device, fz_identity, NULL);
        fz_close_device(ctx, list_device);

        // Analyze display list for potential vector image regions
        // This is a simplified approach - we'll divide the page into grid regions
        // and render each region to detect vector content

        float page_width = page_bounds.x1 - page_bounds.x0;
        float page_height = page_bounds.y1 - page_bounds.y0;

        // Grid-based analysis: divide page into regions
        int grid_cols = 4;
        int grid_rows = 4;
        float region_width = page_width / (float)grid_cols;
        float region_height = page_height / (float)grid_rows;

        int vector_id = 0;

        for (int row = 0; row < grid_rows; row++) {
            for (int col = 0; col < grid_cols; col++) {
                fz_rect region;
                region.x0 = page_bounds.x0 + ((float)col * region_width);
                region.y0 = page_bounds.y0 + ((float)row * region_height);
                region.x1 = region.x0 + region_width;
                region.y1 = region.y0 + region_height;

                // Check if this region is meaningful
                if (!is_meaningful_vector_area(region, page_width, page_height)) {
                    continue;
                }

                // Render this region to check for vector content
                fz_pixmap *pix = NULL;
                fz_device *draw_device = NULL;

                fz_try(ctx) {
                    // Create pixmap for this region at moderate resolution
                    fz_matrix transform = fz_scale(2.0F, 2.0F); // 2x resolution
                    fz_rect render_bounds = fz_transform_rect(region, transform);
                    fz_irect ibounds = fz_round_rect(render_bounds);

                    pix = fz_new_pixmap_with_bbox(ctx, fz_device_rgb(ctx), ibounds, NULL, 0);
                    fz_clear_pixmap_with_value(ctx, pix, 255); // White background

                    draw_device = fz_new_draw_device(ctx, transform, pix);

                    // Render only this region from the display list
                    fz_run_display_list(ctx, list, draw_device, fz_identity, region, NULL);
                    fz_close_device(ctx, draw_device);

                    // Analyze rendered pixmap to detect if there's meaningful vector content
                    int has_content = 0;
                    int pixel_count = pix->w * pix->h;
                    int non_white_pixels = 0;

                    unsigned char *pixels = fz_pixmap_samples(ctx, pix);
                    for (int i = 0; i < pixel_count; i++) {
                        int pixel_offset = i * pix->n;
                        // Check if pixel is not white (255,255,255)
                        if (pixels[pixel_offset] != 255 ||
                            pixels[pixel_offset + 1] != 255 ||
                            pixels[pixel_offset + 2] != 255) {
                            non_white_pixels++;
                        }
                    }

                    // If region has significant non-white content, consider it a vector image
                    float content_ratio = (float)non_white_pixels / (float)pixel_count;
                    if (content_ratio > 0.05F && content_ratio < 0.9F) { // 5-90% content
                        has_content = 1;
                    }

                    if (has_content) {
                        // Save this region as a vector image in the shared uploads directory
                        char org_dir[256];
                        get_org_images_path(org_dir, sizeof(org_dir), image_directory_path);

                        char image_path[512];
                        (void)snprintf(image_path, sizeof(image_path),
                                "%s/page_%d_vector_%d_%ld.png",
                                org_dir, page_num, vector_id, (long)time(NULL));

                        fz_save_pixmap_as_png(ctx, pix, image_path);

                        // Create JSON object for this vector image
                        json_object *vector_obj = json_object_new_object();
                        json_object_object_add(vector_obj, "page", json_object_new_int(page_num));
                        json_object_object_add(vector_obj, "image_id", json_object_new_int(vector_id));
                        json_object_object_add(vector_obj, "type", json_object_new_string("vector"));
                        json_object_object_add(vector_obj, "bbox", create_bounding_box_json(region));
                        json_object_object_add(vector_obj, "width", json_object_new_int(pix->w));
                        json_object_object_add(vector_obj, "height", json_object_new_int(pix->h));
                        json_object_object_add(vector_obj, "image_path", json_object_new_string(image_path));
                        json_object_object_add(vector_obj, "content_ratio", json_object_new_double(content_ratio));

                        json_object_array_add(vector_images, vector_obj);
                        vector_id++;
                        (*vector_image_count)++;

                        printf("[VECTOR_DEBUG] Extracted vector image: region %d,%d content_ratio=%.3f path=%s\n",
                               row, col, content_ratio, image_path);
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
                    printf("[VECTOR_DEBUG] Failed to render region %d,%d\n", row, col);
                }
            }
        }
    }
    fz_always(ctx) {
        if (list_device) {
            fz_drop_device(ctx, list_device);
        }
        if (list) {
            fz_drop_display_list(ctx, list);
        }
    }
    fz_catch(ctx) {
        printf("[VECTOR_DEBUG] Failed to extract vector images from page %d\n", page_num);
    }

    return vector_images;
}