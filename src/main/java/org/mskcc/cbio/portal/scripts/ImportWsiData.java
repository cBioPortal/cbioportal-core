/*
 * Copyright (c) 2026 Memorial Sloan-Kettering Cancer Center.
 *
 * This file is part of cBioPortal and is licensed under the AGPL.
 */

package org.mskcc.cbio.portal.scripts;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import joptsimple.OptionSet;
import org.mskcc.cbio.portal.dao.ClickHouseBulkLoader;
import org.mskcc.cbio.portal.dao.DaoClinicalAttributeMeta;
import org.mskcc.cbio.portal.dao.DaoException;
import org.mskcc.cbio.portal.dao.JdbcUtil;
import org.mskcc.cbio.portal.model.ClinicalAttribute;
import org.mskcc.cbio.portal.util.ConsoleUtil;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Imports the canonical PATHOLOGY_SLIDES/WSI study files into ClickHouse.
 *
 * The complete input is parsed and resolved before any rows are written. Child
 * rows are then bulk-loaded into the inactive blue/green database. WSI imports
 * are intentionally insert-only and must start from a fresh database build.
 */
public class ImportWsiData extends ConsoleRunnable {

    private static final int COLUMN_COUNT = 38;
    private static final String[] COLUMNS = {
        "PATIENT_ID", "REFERENCE_SAMPLE_ID", "SAMPLE_ID", "IMAGE_ID",
        "PART_KEY", "PART_NUMBER", "PART_DESIGNATOR", "PART_TYPE",
        "PART_DESCRIPTION", "SUBSPECIALTY", "PATH_DX_TITLE", "BLOCK_KEY",
        "BLOCK_NUMBER", "BLOCK_LABEL", "MATCH_LEVEL", "SPECIMEN_KEY",
        "STAIN_NAME", "STAIN_GROUP", "IS_HNE", "IS_IHC", "MAGNIFICATION",
        "FILE_SIZE_BYTES", "BARCODE", "SLIDE_TYPE", "CAN_SERVE_TILES", "SOURCE_URL",
        "TILE_METADATA_JSON", "THUMBNAIL_URL", "THUMBNAIL_WIDTH",
        "THUMBNAIL_HEIGHT", "THUMBNAIL_CONTENT_TYPE", "TIMELINE_START_DAYS",
        "TIMELINE_DATE_STATUS", "TIMELINE_DATE_KIND", "TIMELINE_DATE_SOURCE",
        "TIMELINE_DATE_REASON", "TIMELINE_COORDINATE_SYSTEM", "TIMEPOINT_SOURCE"
    };

    private static final String[] PATIENT_FIELDS = {
        "cancer_study_id", "patient_id", "reference_sample_id"
    };
    private static final String[] PART_FIELDS = {
        "cancer_study_id", "patient_id", "part_key",
        "part_number", "part_designator", "part_type", "part_description",
        "subspecialty", "path_dx_title"
    };
    private static final String[] BLOCK_FIELDS = {
        "cancer_study_id", "patient_id", "part_key", "block_key",
        "block_number", "block_label"
    };
    private static final String[] SLIDE_FIELDS = {
        "cancer_study_id", "patient_id", "image_id", "stain_name",
        "stain_group", "is_hne", "is_ihc", "magnification", "file_size_bytes",
        "can_serve_tiles", "barcode", "slide_type", "source_url",
        "tile_metadata_json", "thumbnail_url", "thumbnail_width",
        "thumbnail_height", "thumbnail_content_type"
    };
    private static final String[] PLACEMENT_FIELDS = {
        "cancer_study_id", "patient_id", "image_id", "part_key",
        "block_key", "sample_id", "match_level", "specimen_key"
    };
    private static final String[] TIMING_FIELDS = {
        "cancer_study_id", "patient_id", "image_id", "timeline_start_days",
        "timeline_date_status", "timeline_date_kind", "timeline_date_source",
        "timeline_date_reason", "timeline_coordinate_system", "timepoint_source"
    };
    private static final String[] CLINICAL_SAMPLE_FIELDS = {
        "internal_id", "attr_id", "attr_value"
    };
    private static final String[] CLINICAL_PATIENT_FIELDS = {
        "internal_id", "attr_id", "attr_value"
    };
    private static final String WSI_SAMPLE_SLIDE_COUNT = "WSI_SAMPLE_SLIDE_COUNT";
    private static final String WSI_SAMPLE_PART_MATCHED_SLIDE_COUNT =
        "WSI_SAMPLE_PART_MATCHED_SLIDE_COUNT";
    private static final String WSI_SAMPLE_BLOCK_MATCHED_SLIDE_COUNT =
        "WSI_SAMPLE_BLOCK_MATCHED_SLIDE_COUNT";
    private static final String WSI_PATIENT_SLIDE_COUNT = "WSI_PATIENT_SLIDE_COUNT";
    private static final String WSI_PATIENT_PART_MATCHED_SLIDE_COUNT =
        "WSI_PATIENT_PART_MATCHED_SLIDE_COUNT";
    private static final String WSI_PATIENT_BLOCK_MATCHED_SLIDE_COUNT =
        "WSI_PATIENT_BLOCK_MATCHED_SLIDE_COUNT";

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final int TILE_METADATA_SCHEMA_VERSION = 2;
    private static final String DECODE_POLICY_VERSION =
        "geometry-v2;tile-max=16777216;thumbnail-max=16777216";
    private static final int MAX_DECODE_PIXELS = 16_777_216;
    private record SampleRef(long internalId, long patientId) {}
    private record StudyRefs(long studyId, Map<String, Long> patients,
                             Map<String, SampleRef> samples) {}

    private static final class ImportRows {
        private final Map<String, String[]> patients = new LinkedHashMap<>();
        private final Map<String, String[]> parts = new LinkedHashMap<>();
        private final Map<String, String[]> blocks = new LinkedHashMap<>();
        private final Map<String, String[]> slides = new LinkedHashMap<>();
        private final Map<String, String[]> placements = new LinkedHashMap<>();
        private final Map<String, String[]> timings = new LinkedHashMap<>();
    }

    private static String value(String[] fields, int index) {
        return fields[index].trim();
    }

    private static String nullable(String value) {
        return value == null || value.isBlank() ? null : value;
    }

    private static long requiredLong(String value, String field, int line) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Line " + line + ": " + field + " is required");
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("Line " + line + ": invalid " + field, exception);
        }
    }

    private static Long optionalLong(String value, String field, int line) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("Line " + line + ": invalid " + field, exception);
        }
    }

    private static boolean requiredBoolean(String value, String field, int line) {
        if (!"TRUE".equals(value) && !"FALSE".equals(value)) {
            throw new IllegalArgumentException("Line " + line + ": " + field + " must be TRUE or FALSE");
        }
        return "TRUE".equals(value);
    }

    private static void require(String value, String field, int line) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Line " + line + ": " + field + " is required");
        }
    }

    private static void requireAbsoluteUrl(String value, String field, int line) {
        if (value == null || value.isBlank()) {
            return;
        }
        try {
            var uri = java.net.URI.create(value);
            if (uri.getScheme() == null || (uri.getHost() == null && uri.getPath() == null)) {
                throw new IllegalArgumentException();
            }
        } catch (IllegalArgumentException exception) {
            throw new IllegalArgumentException("Line " + line + ": " + field + " must be an absolute URL");
        }
    }

    private static boolean positiveInteger(JsonNode node) {
        return node != null && node.isIntegralNumber() && node.asLong() > 0;
    }

    static boolean validTileMetadata(JsonNode node) {
        if (node == null || !node.isObject()) {
            return false;
        }
        JsonNode dimensions = node.get("dimensions");
        if (dimensions == null
            || !positiveInteger(dimensions.get("width"))
            || !positiveInteger(dimensions.get("height"))) {
            return false;
        }
        JsonNode levels = node.get("levels");
        JsonNode levelDimensions = node.get("level_dimensions");
        if (levels == null || !levels.isIntegralNumber() || levels.asLong() <= 0
            || levelDimensions == null || !levelDimensions.isArray()
            || levels.asLong() != levelDimensions.size()) {
            return false;
        }
        for (JsonNode level : levelDimensions) {
            if (level == null || !level.isObject()
                || !positiveInteger(level.get("width"))
                || !positiveInteger(level.get("height"))) {
                return false;
            }
        }
        JsonNode maxZoom = node.get("max_zoom");
        JsonNode tileSize = node.get("tile_size");
        if (maxZoom == null || !maxZoom.isIntegralNumber() || maxZoom.asLong() < 0
            || !positiveInteger(tileSize)) {
            return false;
        }
        JsonNode schema = node.get("tile_metadata_schema_version");
        if (schema == null) {
            return true;
        }
        JsonNode safeMinLevel = node.get("safe_min_level");
        JsonNode downsamples = node.get("level_downsamples");
        JsonNode decodePolicy = node.get("decode_policy_version");
        return schema.isIntegralNumber() && schema.asInt() == TILE_METADATA_SCHEMA_VERSION
            && safeMinLevel != null && safeMinLevel.isIntegralNumber()
            && safeMinLevel.asInt() >= 0 && safeMinLevel.asInt() <= maxZoom.asInt()
            && downsamples != null && downsamples.isArray()
            && downsamples.size() == levels.asInt()
            && java.util.stream.StreamSupport.stream(downsamples.spliterator(), false)
                .allMatch(value -> value.isNumber() && value.asDouble() > 0
                    && Double.isFinite(value.asDouble()))
            && decodePolicy != null && DECODE_POLICY_VERSION.equals(decodePolicy.asText())
            && node.has("max_decode_pixels")
            && node.get("max_decode_pixels").isIntegralNumber()
            && node.get("max_decode_pixels").asLong() == MAX_DECODE_PIXELS
            && node.has("thumbnail_max_decode_pixels")
            && node.get("thumbnail_max_decode_pixels").isIntegralNumber()
            && node.get("thumbnail_max_decode_pixels").asLong() == MAX_DECODE_PIXELS;
    }

    private static void requireJsonObject(String value, int line) {
        if (value == null || value.isBlank()) {
            return;
        }
        try {
            JsonNode node = JSON.readTree(value);
            if (node == null || !node.isObject()) {
                throw new IllegalArgumentException();
            }
        } catch (IOException | IllegalArgumentException exception) {
            throw new IllegalArgumentException("Line " + line + ": TILE_METADATA_JSON must be a JSON object");
        }
    }

    private static void requireValidTileMetadata(String value, int line) {
        try {
            if (!validTileMetadata(JSON.readTree(value))) {
                throw new IllegalArgumentException();
            }
        } catch (IOException | IllegalArgumentException exception) {
            throw new IllegalArgumentException("Line " + line
                + ": TILE_METADATA_JSON must contain a valid tile contract");
        }
    }

    private static Properties readMetadata(String metadataFile) throws IOException {
        Properties properties = new TrimmedProperties();
        try (FileReader reader = new FileReader(metadataFile, StandardCharsets.UTF_8)) {
            properties.load(reader);
        }
        if (!"PATHOLOGY_SLIDES".equals(properties.getProperty("genetic_alteration_type"))
            || !"WSI".equals(properties.getProperty("datatype"))) {
            throw new IllegalArgumentException("WSI metadata must use PATHOLOGY_SLIDES / WSI");
        }
        if (!"3".equals(properties.getProperty("format_version"))) {
            throw new IllegalArgumentException("Unsupported WSI format_version; expected 3");
        }
        for (String field : List.of("cancer_study_identifier", "data_filename")) {
            require(properties.getProperty(field), field, 0);
        }
        return properties;
    }

    private static List<String[]> readRows(String dataFile) throws IOException {
        List<String[]> rows = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(dataFile, StandardCharsets.UTF_8))) {
            for (int i = 1; i <= 4; i++) {
                String comment = reader.readLine();
                if (comment == null || !comment.startsWith("#")) {
                    throw new IllegalArgumentException("WSI data must begin with four comment rows");
                }
            }
            String header = reader.readLine();
            if (header != null && header.endsWith("\r")) {
                header = header.substring(0, header.length() - 1);
            }
            if (header == null || !Arrays.equals(COLUMNS, header.split("\\t", -1))) {
                throw new IllegalArgumentException("WSI data has an invalid header or column order");
            }
            String line;
            int lineNumber = 5;
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                if (line.endsWith("\r")) {
                    line = line.substring(0, line.length() - 1);
                }
                String[] fields = line.split("\\t", -1);
                if (fields.length != COLUMN_COUNT) {
                    throw new IllegalArgumentException("Line " + lineNumber + ": expected "
                        + COLUMN_COUNT + " columns, found " + fields.length);
                }
                if (Arrays.stream(fields).allMatch(String::isBlank)) {
                    throw new IllegalArgumentException("Line " + lineNumber + ": blank WSI row");
                }
                rows.add(fields);
            }
        }
        if (rows.isEmpty()) {
            throw new IllegalArgumentException("WSI data file contains no slide rows");
        }
        return rows;
    }

    private static StudyRefs resolveReferences(Properties metadata) throws SQLException {
        Connection connection = null;
        try {
            connection = JdbcUtil.getDbConnection(ImportWsiData.class);
            long studyId;
            try (PreparedStatement statement = connection.prepareStatement(
                "SELECT cancer_study_id FROM cancer_study WHERE cancer_study_identifier = ?")) {
                statement.setString(1, metadata.getProperty("cancer_study_identifier"));
                try (ResultSet result = statement.executeQuery()) {
                    if (!result.next()) {
                        throw new IllegalArgumentException("Cancer study not found: "
                            + metadata.getProperty("cancer_study_identifier"));
                    }
                    studyId = result.getLong(1);
                }
            }
            Map<String, Long> patients = new HashMap<>();
            try (PreparedStatement statement = connection.prepareStatement(
                "SELECT internal_id, stable_id FROM patient WHERE cancer_study_id = ?")) {
                statement.setLong(1, studyId);
                try (ResultSet result = statement.executeQuery()) {
                    while (result.next()) {
                        patients.put(result.getString("stable_id"), result.getLong("internal_id"));
                    }
                }
            }
            Map<String, SampleRef> samples = new HashMap<>();
            try (PreparedStatement statement = connection.prepareStatement(
                "SELECT s.internal_id, s.stable_id, s.patient_id FROM sample s "
                    + "INNER JOIN patient p ON p.internal_id = s.patient_id "
                    + "WHERE p.cancer_study_id = ?")) {
                statement.setLong(1, studyId);
                try (ResultSet result = statement.executeQuery()) {
                    while (result.next()) {
                        samples.put(result.getString("stable_id"),
                            new SampleRef(result.getLong("internal_id"), result.getLong("patient_id")));
                    }
                }
            }
            return new StudyRefs(studyId, patients, samples);
        } finally {
            JdbcUtil.closeAll(ImportWsiData.class, connection, null, null);
        }
    }

    private static ImportRows normalize(List<String[]> input, StudyRefs refs) {
        ImportRows output = new ImportRows();
        Set<String> imageIds = new HashSet<>();
        Map<String, Long> patientReferences = new HashMap<>();
        for (int rowIndex = 0; rowIndex < input.size(); rowIndex++) {
            int line = rowIndex + 6;
            String[] fields = input.get(rowIndex);
            String patientStableId = value(fields, 0);
            String patientKey = patientStableId;
            long patientId = refs.patients.getOrDefault(patientStableId, -1L);
            if (patientId < 0) {
                throw new IllegalArgumentException("Line " + line + ": patient not found: " + patientStableId);
            }
            String imageId = value(fields, 3);
            require(imageId, "IMAGE_ID", line);
            if (!imageIds.add(imageId)) {
                throw new IllegalArgumentException("Line " + line + ": IMAGE_ID is not unique: " + imageId);
            }
            String partKey = value(fields, 4);
            String blockKey = value(fields, 11);
            require(partKey, "PART_KEY", line);
            require(blockKey, "BLOCK_KEY", line);
            if (partKey.contains("?") || blockKey.contains("?")) {
                throw new IllegalArgumentException("Line " + line + ": part/block keys cannot contain ?");
            }
            String matchLevel = value(fields, 14);
            if (!Set.of("BLOCK", "PART", "UNMATCHED").contains(matchLevel)) {
                throw new IllegalArgumentException("Line " + line + ": invalid MATCH_LEVEL");
            }
            String sampleStableId = value(fields, 2);
            if ("UNMATCHED".equals(matchLevel) && !sampleStableId.isBlank()) {
                throw new IllegalArgumentException("Line " + line + ": UNMATCHED rows cannot have SAMPLE_ID");
            }
            if (!"UNMATCHED".equals(matchLevel) && sampleStableId.isBlank()) {
                throw new IllegalArgumentException("Line " + line + ": matched rows require SAMPLE_ID");
            }
            SampleRef sample = null;
            if (!sampleStableId.isBlank()) {
                sample = refs.samples.get(sampleStableId);
                if (sample == null || sample.patientId() != patientId) {
                    throw new IllegalArgumentException("Line " + line + ": sample does not belong to patient");
                }
            }
            String referenceStableId = value(fields, 1);
            Long referenceSampleId = null;
            if (!referenceStableId.isBlank() && !"UNMATCHED".equalsIgnoreCase(referenceStableId)) {
                SampleRef reference = refs.samples.get(referenceStableId);
                if (reference == null || reference.patientId() != patientId) {
                    throw new IllegalArgumentException("Line " + line + ": reference sample does not belong to patient");
                }
                referenceSampleId = reference.internalId();
            }
            if (patientReferences.containsKey(patientKey)
                && !java.util.Objects.equals(patientReferences.get(patientKey), referenceSampleId)) {
                throw new IllegalArgumentException("Line " + line + ": patient has conflicting reference samples");
            }
            patientReferences.put(patientKey, referenceSampleId);
            output.patients.putIfAbsent(patientKey, new String[] {
                Long.toString(refs.studyId()), Long.toString(patientId), nullableLong(referenceSampleId)
            });

            String partMapKey = patientKey + "\u0000" + partKey;
            String[] part = new String[] {
                Long.toString(refs.studyId()), Long.toString(patientId), partKey,
                nullable(value(fields, 5)), nullable(value(fields, 6)), nullable(value(fields, 7)),
                nullable(value(fields, 8)), nullable(value(fields, 9)), nullable(value(fields, 10))
            };
            putConsistent(output.parts, partMapKey, part, line, "part");

            String blockMapKey = partMapKey + "\u0000" + blockKey;
            String[] block = new String[] {
                Long.toString(refs.studyId()), Long.toString(patientId), partKey, blockKey,
                nullable(value(fields, 12)), nullable(value(fields, 13))
            };
            putConsistent(output.blocks, blockMapKey, block, line, "block");

            boolean isHne = requiredBoolean(value(fields, 18), "IS_HNE", line);
            boolean isIhc = requiredBoolean(value(fields, 19), "IS_IHC", line);
            boolean canServe = requiredBoolean(value(fields, 24), "CAN_SERVE_TILES", line);
            Long fileSize = optionalLong(value(fields, 21), "FILE_SIZE_BYTES", line);
            if (fileSize != null && fileSize < 0) {
                throw new IllegalArgumentException("Line " + line + ": FILE_SIZE_BYTES cannot be negative");
            }
            String sourceUrl = nullable(value(fields, 25));
            String tileMetadata = nullable(value(fields, 26));
            String thumbnailUrl = nullable(value(fields, 27));
            String thumbnailContentType = nullable(value(fields, 30));
            Long thumbnailWidth = optionalLong(value(fields, 28), "THUMBNAIL_WIDTH", line);
            Long thumbnailHeight = optionalLong(value(fields, 29), "THUMBNAIL_HEIGHT", line);
            if ((thumbnailWidth != null && (thumbnailWidth < 0 || thumbnailWidth > 0xffffffffL))
                || (thumbnailHeight != null && (thumbnailHeight < 0 || thumbnailHeight > 0xffffffffL))) {
                throw new IllegalArgumentException("Line " + line + ": thumbnail dimension is out of range");
            }
            requireAbsoluteUrl(sourceUrl, "SOURCE_URL", line);
            requireAbsoluteUrl(thumbnailUrl, "THUMBNAIL_URL", line);
            if (sourceUrl != null && !safeArtifactUrl(sourceUrl, "WSI_ALLOWED_SOURCE_PREFIXES")) {
                throw new IllegalArgumentException(
                    "Line " + line + ": SOURCE_URL is unsafe or outside the configured allowlist");
            }
            if (thumbnailUrl != null && !safeArtifactUrl(thumbnailUrl, "WSI_ALLOWED_THUMBNAIL_PREFIXES")) {
                throw new IllegalArgumentException(
                    "Line " + line + ": THUMBNAIL_URL is unsafe or outside the configured allowlist");
            }
            if (thumbnailContentType != null && !isImageContentType(thumbnailContentType)) {
                throw new IllegalArgumentException(
                    "Line " + line + ": THUMBNAIL_CONTENT_TYPE must be an image media type");
            }
            requireJsonObject(tileMetadata, line);
            if (canServe) {
                require(sourceUrl, "SOURCE_URL", line);
                require(tileMetadata, "TILE_METADATA_JSON", line);
                requireValidTileMetadata(tileMetadata, line);
                require(thumbnailUrl, "THUMBNAIL_URL", line);
                require(value(fields, 30), "THUMBNAIL_CONTENT_TYPE", line);
                if (thumbnailWidth == null || thumbnailWidth < 1 || thumbnailWidth > 8192
                    || thumbnailHeight == null || thumbnailHeight < 1 || thumbnailHeight > 8192) {
                    throw new IllegalArgumentException(
                        "Line " + line + ": servable thumbnail dimensions must be between 1 and 8192");
                }
            } else {
                sourceUrl = null;
                tileMetadata = null;
                thumbnailUrl = null;
                thumbnailWidth = null;
                thumbnailHeight = null;
                thumbnailContentType = null;
            }
            Long timelineStartDays = optionalLong(value(fields, 31), "TIMELINE_START_DAYS", line);
            String timelineStatus = value(fields, 32);
            String timelineKind = value(fields, 33);
            String timelineSource = value(fields, 34);
            String timelineReason = nullable(value(fields, 35));
            String timelineCoordinateSystem = value(fields, 36);
            String timepointSource = nullable(value(fields, 37));
            validateTiming(
                timelineStartDays,
                timelineStatus,
                timelineKind,
                timelineSource,
                timelineReason,
                timelineCoordinateSystem,
                line
            );
            if (timepointSource == null) {
                timepointSource = deriveTimepointSource(timelineKind, timelineReason, timelineSource, timelineStatus);
            }
            String[] slide = new String[] {
                Long.toString(refs.studyId()), Long.toString(patientId), imageId,
                nullable(value(fields, 16)), nullable(value(fields, 17)), isHne ? "1" : "0",
                isIhc ? "1" : "0", nullable(value(fields, 20)), nullableLong(fileSize),
                canServe ? "1" : "0", nullable(value(fields, 22)), nullable(value(fields, 23)),
                sourceUrl, tileMetadata, thumbnailUrl, nullableLong(thumbnailWidth),
                nullableLong(thumbnailHeight), thumbnailContentType
            };
            output.slides.put(imageId, slide);

            String[] placement = new String[] {
                Long.toString(refs.studyId()), Long.toString(patientId), imageId, partKey,
                blockKey, sample == null ? null : Long.toString(sample.internalId()), matchLevel,
                value(fields, 15)
            };
            output.placements.put(imageId, placement);
            output.timings.put(imageId, new String[] {
                Long.toString(refs.studyId()), Long.toString(patientId), imageId,
                nullableLong(timelineStartDays), timelineStatus, timelineKind,
                timelineSource, timelineReason, timelineCoordinateSystem, timepointSource
            });
        }
        return output;
    }

    private static void validateTiming(
        Long timelineStartDays,
        String timelineStatus,
        String timelineKind,
        String timelineSource,
        String timelineReason,
        String timelineCoordinateSystem,
        int line
    ) {
        if (!Set.of("AVAILABLE", "MISSING_PROCEDURE_DATE", "MISSING_REFERENCE_SEQUENCING_DATE")
            .contains(timelineStatus)) {
            throw new IllegalArgumentException("Line " + line + ": invalid TIMELINE_DATE_STATUS");
        }
        if (!Set.of("RECORDED", "ESTIMATED", "UNDATED").contains(timelineKind)) {
            throw new IllegalArgumentException("Line " + line + ": invalid TIMELINE_DATE_KIND");
        }
        require(timelineSource, "TIMELINE_DATE_SOURCE", line);
        if (!"patient_first_tumor_sequencing_day_zero".equals(timelineCoordinateSystem)) {
            throw new IllegalArgumentException("Line " + line + ": unsupported TIMELINE_COORDINATE_SYSTEM");
        }
        if ("AVAILABLE".equals(timelineStatus)) {
            if (timelineStartDays == null || "UNDATED".equals(timelineKind) || timelineReason != null) {
                throw new IllegalArgumentException("Line " + line + ": AVAILABLE timing is inconsistent");
            }
            return;
        }
        if (timelineStartDays != null) {
            throw new IllegalArgumentException("Line " + line + ": non-AVAILABLE timing cannot have TIMELINE_START_DAYS");
        }
        if ("MISSING_PROCEDURE_DATE".equals(timelineStatus) && !"UNDATED".equals(timelineKind)) {
            throw new IllegalArgumentException("Line " + line + ": missing procedure date must be UNDATED");
        }
        if ("MISSING_REFERENCE_SEQUENCING_DATE".equals(timelineStatus)
            && "UNDATED".equals(timelineKind)) {
            throw new IllegalArgumentException("Line " + line + ": missing reference date cannot be UNDATED");
        }
    }

    private static String deriveTimepointSource(
        String timelineKind, String timelineReason, String timelineSource, String timelineStatus
    ) {
        if ("ESTIMATED".equals(timelineKind)) {
            return "Verified estimated procedure date relative to first tumor sequencing";
        }
        if ("RECORDED".equals(timelineKind)) {
            return "Recorded procedure date relative to first tumor sequencing";
        }
        return timelineReason != null ? timelineReason : timelineSource != null ? timelineSource : timelineStatus;
    }

    static boolean isImageContentType(String contentType) {
        String normalized = contentType.trim().toLowerCase(Locale.ROOT);
        if (!normalized.startsWith("image/")) {
            return false;
        }
        String subtype = normalized.substring("image/".length());
        return !subtype.isBlank() && subtype.chars().allMatch(ImportWsiData::isMediaTypeTokenCharacter);
    }

    private static boolean isMediaTypeTokenCharacter(int character) {
        return (character >= 'a' && character <= 'z')
            || (character >= 'A' && character <= 'Z')
            || (character >= '0' && character <= '9')
            || "!#$%&'*+-.^_`|~".indexOf(character) >= 0;
    }

    private static String decodedArtifactPath(URI uri) {
        String path = uri.getRawPath();
        if (path == null) {
            return null;
        }
        for (int pass = 0; pass < 2; pass++) {
            // URLDecoder treats '+' as a space, so protect literal path plus signs.
            path = path.replace("+", "%2B");
            if (pass == 1) {
                // The first decode may expose a literal percent (for example,
                // from %25). Preserve it unless it starts another valid escape.
                path = path.replaceAll("%(?![0-9A-Fa-f]{2})", "%25");
            }
            path = URLDecoder.decode(path, StandardCharsets.UTF_8);
        }
        return path;
    }

    static boolean safeArtifactUrl(String value, String prefixEnv) {
        try {
            URI uri = new URI(value);
            String scheme = uri.getScheme();
            if (scheme == null || (uri.getHost() == null && uri.getPath() == null)
                || uri.getUserInfo() != null || uri.getQuery() != null || uri.getFragment() != null) {
                return false;
            }
            if ("file".equalsIgnoreCase(scheme)
                && uri.getHost() != null
                && !uri.getHost().isBlank()
                && !"localhost".equalsIgnoreCase(uri.getHost())) {
                return false;
            }
            String path = decodedArtifactPath(uri);
            if (path == null || path.isBlank() || path.endsWith("/")
                || Arrays.stream(path.split("/", -1))
                    .anyMatch(segment -> ".".equals(segment) || "..".equals(segment))) {
                return false;
            }
            String prefixes = System.getenv(prefixEnv);
            boolean approved = false;
            if (prefixes != null && !prefixes.isBlank()) {
                approved = Arrays.stream(prefixes.split(","))
                    .map(String::trim).filter(prefix -> !prefix.isBlank())
                    .anyMatch(prefix -> value.startsWith(prefix.replaceAll("/+$", "") + "/"));
                if (!approved) return false;
            }
            return true;
        } catch (URISyntaxException | IllegalArgumentException exception) {
            return false;
        }
    }

    private static String nullableLong(Long value) {
        return value == null ? null : Long.toString(value);
    }

    private static void putConsistent(Map<String, String[]> values, String key, String[] value,
                                      int line, String name) {
        String[] previous = values.putIfAbsent(key, value);
        if (previous != null && !Arrays.equals(previous, value)) {
            throw new IllegalArgumentException("Line " + line + ": conflicting " + name + " metadata");
        }
    }

    /**
     * Adds the sample- and patient-level WSI attributes consumed by Study View.
     * Patient totals are written directly so pagination cannot produce partial
     * values; the frontend still supports sample aggregation for older studies.
     * Counts intentionally include all associations, including slides that are
     * not currently tile-servable, matching the WSI study-file contract and the
     * standalone count-file generator.
     */
    private static void insertSampleSlideCounts(ImportRows rows, long studyId)
        throws DaoException {
        Map<Long, int[]> countsBySample = new LinkedHashMap<>();
        Map<Long, int[]> countsByPatient = countPatientSlidePlacements(rows.placements.values());
        for (String[] placement : rows.placements.values()) {
            if (placement[5] == null || placement[5].isBlank()) {
                continue; // unmatched slides have no sample-level count
            }
            long sampleId = Long.parseLong(placement[5]);
            int[] counts = countsBySample.computeIfAbsent(sampleId, ignored -> new int[3]);
            counts[0]++;
            if ("PART".equals(placement[6])) {
                counts[1]++;
            } else if ("BLOCK".equals(placement[6])) {
                counts[2]++;
            }
        }

        String[][] attributes = {
            {WSI_SAMPLE_SLIDE_COUNT, "WSI Slides per Sample",
                "Associated pathology slide count for the sample."},
            {WSI_SAMPLE_PART_MATCHED_SLIDE_COUNT, "WSI Slides per Sample, Part-matched",
                "Associated pathology slides matched to a specimen part."},
            {WSI_SAMPLE_BLOCK_MATCHED_SLIDE_COUNT, "WSI Slides per Sample, Block-matched",
                "Associated pathology slides matched to a specimen block."},
            {WSI_PATIENT_SLIDE_COUNT, "WSI Slides per Patient",
                "Associated pathology slide count for the patient."},
            {WSI_PATIENT_PART_MATCHED_SLIDE_COUNT, "WSI Slides per Patient, Part-matched",
                "Associated pathology slides matched to a specimen part for the patient."},
            {WSI_PATIENT_BLOCK_MATCHED_SLIDE_COUNT, "WSI Slides per Patient, Block-matched",
                "Associated pathology slides matched to a specimen block for the patient."}
        };
        for (String[] attribute : attributes) {
            if (DaoClinicalAttributeMeta.getDatum(attribute[0], Math.toIntExact(studyId)) == null) {
                boolean patientAttribute = attribute[0].startsWith("WSI_PATIENT_");
                DaoClinicalAttributeMeta.addDatum(new ClinicalAttribute(
                    attribute[0], attribute[1], attribute[2], "NUMBER", patientAttribute, "1",
                    Math.toIntExact(studyId)));
            }
        }

        Set<String> existingSamples = existingSlideCountKeys("clinical_sample", countsBySample.keySet());
        ClickHouseBulkLoader sampleLoader =
            ClickHouseBulkLoader.getClickHouseBulkLoader("clinical_sample");
        sampleLoader.setFieldNames(CLINICAL_SAMPLE_FIELDS);
        for (Map.Entry<Long, int[]> entry : countsBySample.entrySet()) {
            int[] counts = entry.getValue();
            String sampleId = Long.toString(entry.getKey());
            if (!existingSamples.contains(sampleId + "\u0000" + WSI_SAMPLE_SLIDE_COUNT)) {
                sampleLoader.insertRecord(sampleId, WSI_SAMPLE_SLIDE_COUNT, Integer.toString(counts[0]));
            }
            if (!existingSamples.contains(sampleId + "\u0000" + WSI_SAMPLE_PART_MATCHED_SLIDE_COUNT)) {
                sampleLoader.insertRecord(sampleId, WSI_SAMPLE_PART_MATCHED_SLIDE_COUNT,
                    Integer.toString(counts[1]));
            }
            if (!existingSamples.contains(sampleId + "\u0000" + WSI_SAMPLE_BLOCK_MATCHED_SLIDE_COUNT)) {
                sampleLoader.insertRecord(sampleId, WSI_SAMPLE_BLOCK_MATCHED_SLIDE_COUNT,
                    Integer.toString(counts[2]));
            }
        }

        Set<String> existingPatients = existingSlideCountKeys("clinical_patient", countsByPatient.keySet());
        ClickHouseBulkLoader patientLoader =
            ClickHouseBulkLoader.getClickHouseBulkLoader("clinical_patient");
        patientLoader.setFieldNames(CLINICAL_PATIENT_FIELDS);
        for (Map.Entry<Long, int[]> entry : countsByPatient.entrySet()) {
            int[] counts = entry.getValue();
            String patientId = Long.toString(entry.getKey());
            if (!existingPatients.contains(patientId + "\u0000" + WSI_PATIENT_SLIDE_COUNT)) {
                patientLoader.insertRecord(patientId, WSI_PATIENT_SLIDE_COUNT, Integer.toString(counts[0]));
            }
            if (!existingPatients.contains(patientId + "\u0000" + WSI_PATIENT_PART_MATCHED_SLIDE_COUNT)) {
                patientLoader.insertRecord(patientId, WSI_PATIENT_PART_MATCHED_SLIDE_COUNT,
                    Integer.toString(counts[1]));
            }
            if (!existingPatients.contains(patientId + "\u0000" + WSI_PATIENT_BLOCK_MATCHED_SLIDE_COUNT)) {
                patientLoader.insertRecord(patientId, WSI_PATIENT_BLOCK_MATCHED_SLIDE_COUNT,
                    Integer.toString(counts[2]));
            }
        }
    }

    static Map<Long, int[]> countPatientSlidePlacements(Iterable<String[]> placements) {
        Map<Long, int[]> countsByPatient = new LinkedHashMap<>();
        for (String[] placement : placements) {
            int[] counts = countsByPatient.computeIfAbsent(
                Long.parseLong(placement[1]), ignored -> new int[3]);
            counts[0]++;
            if ("PART".equals(placement[6])) {
                counts[1]++;
            } else if ("BLOCK".equals(placement[6])) {
                counts[2]++;
            }
        }
        return countsByPatient;
    }

    private static Set<String> existingSlideCountKeys(String tableName, Set<Long> entityIds)
        throws DaoException {
        Set<String> existing = new HashSet<>();
        if (entityIds.isEmpty()) {
            return existing;
        }
        String placeholders = String.join(",", Collections.nCopies(entityIds.size(), "?"));
        String query = "SELECT internal_id, attr_id FROM " + tableName + " WHERE internal_id IN ("
            + placeholders + ") AND attr_id IN ('" + WSI_SAMPLE_SLIDE_COUNT + "','"
            + WSI_SAMPLE_PART_MATCHED_SLIDE_COUNT + "','" + WSI_SAMPLE_BLOCK_MATCHED_SLIDE_COUNT
            + "','" + WSI_PATIENT_SLIDE_COUNT + "','" + WSI_PATIENT_PART_MATCHED_SLIDE_COUNT
            + "','" + WSI_PATIENT_BLOCK_MATCHED_SLIDE_COUNT + "')";
        Connection connection = null;
        try {
            connection = JdbcUtil.getDbConnection(ImportWsiData.class);
            try (PreparedStatement statement = connection.prepareStatement(query)) {
                int index = 1;
                for (Long entityId : entityIds) {
                    statement.setLong(index++, entityId);
                }
                try (ResultSet results = statement.executeQuery()) {
                    while (results.next()) {
                        existing.add(results.getLong("internal_id") + "\u0000" + results.getString("attr_id"));
                    }
                }
            }
        } catch (SQLException exception) {
            throw new DaoException(exception);
        } finally {
            JdbcUtil.closeAll(ImportWsiData.class, connection, null, null);
        }
        return existing;
    }

    private static void insertRows(ImportRows rows, long studyId) throws DaoException {
        ClickHouseBulkLoader.getClickHouseBulkLoader("wsi_patient").setFieldNames(PATIENT_FIELDS);
        rows.patients.values().forEach(record -> ClickHouseBulkLoader.getClickHouseBulkLoader("wsi_patient").insertRecord(record));
        ClickHouseBulkLoader.getClickHouseBulkLoader("wsi_part").setFieldNames(PART_FIELDS);
        rows.parts.values().forEach(record -> ClickHouseBulkLoader.getClickHouseBulkLoader("wsi_part").insertRecord(record));
        ClickHouseBulkLoader.getClickHouseBulkLoader("wsi_block").setFieldNames(BLOCK_FIELDS);
        rows.blocks.values().forEach(record -> ClickHouseBulkLoader.getClickHouseBulkLoader("wsi_block").insertRecord(record));
        ClickHouseBulkLoader.getClickHouseBulkLoader("wsi_slide").setFieldNames(SLIDE_FIELDS);
        rows.slides.values().forEach(record -> ClickHouseBulkLoader.getClickHouseBulkLoader("wsi_slide").insertRecord(record));
        ClickHouseBulkLoader.getClickHouseBulkLoader("wsi_slide_placement").setFieldNames(PLACEMENT_FIELDS);
        rows.placements.values().forEach(record -> ClickHouseBulkLoader.getClickHouseBulkLoader("wsi_slide_placement").insertRecord(record));
        ClickHouseBulkLoader.getClickHouseBulkLoader("wsi_slide_timing").setFieldNames(TIMING_FIELDS);
        rows.timings.values().forEach(record -> ClickHouseBulkLoader.getClickHouseBulkLoader("wsi_slide_timing").insertRecord(record));
        insertSampleSlideCounts(rows, studyId);
        ClickHouseBulkLoader.flushAll();
    }

    private static void importData(String metadataFile, String dataFile)
        throws IOException, SQLException, DaoException {
        Properties metadata = readMetadata(metadataFile);
        String expectedData = new File(new File(metadataFile).getParentFile(), metadata.getProperty("data_filename"))
            .getCanonicalPath();
        if (!new File(dataFile).getCanonicalPath().equals(expectedData)) {
            throw new IllegalArgumentException("data filename does not match meta_wsi.txt");
        }
        List<String[]> rows = readRows(dataFile);
        StudyRefs references = resolveReferences(metadata);
        ImportRows normalized = normalize(rows, references);

        ClickHouseBulkLoader.bulkLoadOn();
        try {
            insertRows(normalized, references.studyId());
        } finally {
            ClickHouseBulkLoader.bulkLoadOff();
        }
    }

    @Override
    public void run() {
        String description = "Import PATHOLOGY_SLIDES/WSI data";
        OptionSet options = ConsoleUtil.parseStandardDataAndMetaOptions(args, description, true);
        String loadMode = options.has("loadMode") ? (String) options.valueOf("loadMode") : "bulkLoad";
        if (!"bulkLoad".equalsIgnoreCase(loadMode)) {
            throw new UnsupportedOperationException("WSI importer supports bulkLoad load mode only");
        }
        try {
            importData((String) options.valueOf("meta"), (String) options.valueOf("data"));
        } catch (IOException | SQLException | DaoException exception) {
            throw new RuntimeException(exception);
        }
    }

    public ImportWsiData(String[] args) {
        super(args);
    }

    public static void main(String[] args) {
        new ImportWsiData(args).runInConsole();
    }
}
