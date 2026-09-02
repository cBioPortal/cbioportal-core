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
import java.util.regex.Pattern;

/**
 * Imports the canonical PATHOLOGY_SLIDES/WSI study files into ClickHouse.
 *
 * The complete input is parsed and resolved before any rows are written. Child
 * rows are then bulk-loaded into the inactive blue/green database. WSI imports
 * are intentionally insert-only and must start from a fresh database build.
 */
public class ImportWsiData extends ConsoleRunnable {

    private static final int COLUMN_COUNT = 31;
    private static final String[] COLUMNS = {
        "PATIENT_ID", "REFERENCE_SAMPLE_ID", "SAMPLE_ID", "IMAGE_ID",
        "PART_KEY", "PART_NUMBER", "PART_DESIGNATOR", "PART_TYPE",
        "PART_DESCRIPTION", "SUBSPECIALTY", "PATH_DX_TITLE", "BLOCK_KEY",
        "BLOCK_NUMBER", "BLOCK_LABEL", "MATCH_LEVEL", "SPECIMEN_KEY",
        "STAIN_NAME", "STAIN_GROUP", "IS_HNE", "IS_IHC", "MAGNIFICATION",
        "FILE_SIZE_BYTES", "BARCODE", "SLIDE_TYPE", "CAN_SERVE_TILES", "SOURCE_URL",
        "TILE_METADATA_JSON", "THUMBNAIL_URL", "THUMBNAIL_WIDTH",
        "THUMBNAIL_HEIGHT", "THUMBNAIL_CONTENT_TYPE"
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
    private static final Pattern ABSOLUTE_DATE = Pattern.compile(
        "(?<!\\d)(?:19|20)\\d{2}[-_/](?:0?[1-9]|1[0-2])[-_/](?:0?[1-9]|[12]\\d|3[01])(?!\\d)");
    private static final Pattern MONTH_FIRST_DATE = Pattern.compile(
        "(?<!\\d)(?:0?[1-9]|1[0-2])[-_/](?:0?[1-9]|[12]\\d|3[01])[-_/](?:19|20)\\d{2}(?!\\d)");
    private static final Pattern DAY_FIRST_DATE = Pattern.compile(
        "(?<!\\d)(?:0?[1-9]|[12]\\d|3[01])[-_/](?:0?[1-9]|1[0-2])[-_/](?:19|20)\\d{2}(?!\\d)");
    private static final Pattern NAMED_MONTH_DATE = Pattern.compile(
        "(?i)(?<![a-z0-9])(?:(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|"
            + "may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|"
            + "nov(?:ember)?|dec(?:ember)?)\\s+(?:0?[1-9]|[12]\\d|3[01])(?:st|nd|rd|th)?"
            + "(?:,)?\\s+(?:19|20)\\d{2}|(?:0?[1-9]|[12]\\d|3[01])[-/\\s]+"
            + "(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|"
            + "jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|"
            + "dec(?:ember)?)[-/\\s]+(?:19|20)\\d{2})(?![a-z0-9])");
    private static final Pattern COMPACT_DATE = Pattern.compile(
        "(?<!\\d)(?:19|20)\\d{6}(?!\\d)");
    private static final Pattern LABELLED_MRN = Pattern.compile(
        "(?i)\\b(?:mrn|medical[ _-]?record(?:[ _-]?number)?)\\b\\s*[:=#-]?\\s*\\d{4,}");
    private static final Set<String> SOURCE_EXTENSIONS = Set.of("svs", "tif", "tiff", "ndpi", "mrxs", "scn");
    private static final Set<String> THUMBNAIL_EXTENSIONS = Set.of("jpg", "jpeg", "png");
    private static final Set<String> ALLOWED_METADATA_KEYS = Set.of(
        "dimensions", "levels", "level_dimensions", "level_downsamples", "max_zoom",
        "tile_size", "mpp", "objective_power", "vendor", "identity_version", "safe_min_level",
        "tile_metadata_schema_version", "decode_policy_version", "max_decode_pixels",
        "thumbnail_max_decode_pixels", "source_fingerprint");
    private static final Set<Integer> NON_TEXT_WSI_COLUMNS = Set.of(18, 19, 21, 24, 26, 28, 29);
    private static final Map<String, String> THUMBNAIL_CONTENT_TYPES = Map.of(
        "jpg", "image/jpeg", "jpeg", "image/jpeg", "png", "image/png");

    private record SampleRef(long internalId, long patientId) {}
    private record StudyRefs(long studyId, Map<String, Long> patients,
                             Map<String, SampleRef> samples) {}

    private static final class ImportRows {
        private final Map<String, String[]> patients = new LinkedHashMap<>();
        private final Map<String, String[]> parts = new LinkedHashMap<>();
        private final Map<String, String[]> blocks = new LinkedHashMap<>();
        private final Map<String, String[]> slides = new LinkedHashMap<>();
        private final Map<String, String[]> placements = new LinkedHashMap<>();
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

    private static boolean validTileMetadata(JsonNode node) {
        if (node == null || !node.isObject()) {
            return false;
        }
        var fieldNames = node.fieldNames();
        while (fieldNames.hasNext()) {
            if (!ALLOWED_METADATA_KEYS.contains(fieldNames.next())) {
                return false;
            }
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
        return maxZoom != null && maxZoom.isIntegralNumber() && maxZoom.asLong() >= 0
            && positiveInteger(tileSize);
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
        if (!"2".equals(properties.getProperty("format_version"))) {
            throw new IllegalArgumentException("Unsupported WSI format_version; expected 2");
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
            validateDeidRow(fields, line);
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
        }
        return output;
    }

    private static void validateDeidRow(String[] fields, int line) {
        String imageId = value(fields, 3);
        if (imageId.isBlank()) {
            return;
        }
        for (int index = 0; index < fields.length; index++) {
            if (index == 0 || index == 1 || index == 2 || index == 3
                || NON_TEXT_WSI_COLUMNS.contains(index)) {
                continue; // approved portal/image pseudonyms
            }
            String fieldValue = fields[index] == null ? "" : fields[index].trim();
            if (LABELLED_MRN.matcher(fieldValue).find()
                || containsAbsoluteDate(fieldValue)
                || COMPACT_DATE.matcher(fieldValue).find()) {
                throw new IllegalArgumentException(
                    "Line " + line + ": WSI value violates the de-identification contract");
            }
        }
        String metadata = nullable(value(fields, 26));
        if (metadata != null && containsForbiddenMetadataText(metadata)) {
            throw new IllegalArgumentException(
                "Line " + line + ": TILE_METADATA_JSON violates the de-identification contract");
        }
        String source = nullable(value(fields, 25));
        String thumbnail = nullable(value(fields, 27));
        if (source != null && !safeArtifactUrl(source, SOURCE_EXTENSIONS, "WSI_ALLOWED_SOURCE_PREFIXES")) {
            throw new IllegalArgumentException(
                "Line " + line + ": SOURCE_URL violates the de-identification contract");
        }
        if (thumbnail != null && !safeArtifactUrl(thumbnail, THUMBNAIL_EXTENSIONS, "WSI_ALLOWED_THUMBNAIL_PREFIXES")) {
            throw new IllegalArgumentException(
                "Line " + line + ": THUMBNAIL_URL violates the de-identification contract");
        }
        for (int index : new int[] {0, 1, 2, 22}) {
            String identifier = fields[index] == null ? "" : fields[index].trim();
            if (!identifier.isBlank()) {
                String lower = identifier.toLowerCase(Locale.ROOT);
                if ((source != null && containsDecoded(source, lower))
                    || (thumbnail != null && containsDecoded(thumbnail, lower))) {
                    throw new IllegalArgumentException(
                        "Line " + line + ": WSI URI contains a related identifier");
                }
            }
        }
        if (thumbnail != null) {
            String contentType = nullable(value(fields, 30));
            if (contentType != null && !thumbnailContentTypeMatches(thumbnail, contentType)) {
                throw new IllegalArgumentException(
                    "Line " + line + ": THUMBNAIL_CONTENT_TYPE does not match THUMBNAIL_URL");
            }
        }
    }

    private static boolean containsForbiddenMetadataText(JsonNode node) {
        if (node == null) return false;
        if (node.isTextual()) {
            String value = node.asText();
            return LABELLED_MRN.matcher(value).find()
                || containsAbsoluteDate(value)
                || COMPACT_DATE.matcher(value).find();
        }
        if (node.isObject()) {
            var values = node.elements();
            while (values.hasNext()) {
                if (containsForbiddenMetadataText(values.next())) return true;
            }
        } else if (node.isArray()) {
            for (JsonNode child : node) {
                if (containsForbiddenMetadataText(child)) return true;
            }
        }
        return false;
    }

    private static boolean containsForbiddenMetadataText(String value) {
        try {
            return containsForbiddenMetadataText(JSON.readTree(value));
        } catch (IOException | IllegalArgumentException exception) {
            return true;
        }
    }

    private static boolean thumbnailContentTypeMatches(String value, String contentType) {
        try {
            URI uri = new URI(value);
            String path = uri.getPath();
            if (path == null) return false;
            int dot = path.lastIndexOf('.');
            if (dot <= path.lastIndexOf('/')) return false;
            String extension = path.substring(dot + 1).toLowerCase(Locale.ROOT);
            return contentType.trim().toLowerCase(Locale.ROOT)
                .equals(THUMBNAIL_CONTENT_TYPES.get(extension));
        } catch (URISyntaxException exception) {
            return false;
        }
    }

    private static boolean safeArtifactUrl(String value, Set<String> extensions, String prefixEnv) {
        try {
            URI uri = new URI(value);
            String scheme = uri.getScheme();
            if (scheme == null || !("s3".equalsIgnoreCase(scheme) || "file".equalsIgnoreCase(scheme))
                || uri.getUserInfo() != null || uri.getQuery() != null || uri.getFragment() != null) {
                return false;
            }
            if ("s3".equalsIgnoreCase(scheme) && (uri.getHost() == null || uri.getHost().isBlank())) {
                return false;
            }
            if ("file".equalsIgnoreCase(scheme)
                && uri.getHost() != null
                && !uri.getHost().isBlank()
                && !"localhost".equalsIgnoreCase(uri.getHost())) {
                return false;
            }
            String path = uri.getPath();
            String rawPath = uri.getRawPath();
            if (path == null || path.endsWith("/")
                || Arrays.stream(path.split("/", -1))
                    .anyMatch(segment -> ".".equals(segment) || "..".equals(segment))) {
                return false;
            }
            String prefixes = System.getenv(prefixEnv);
            if (prefixes != null && !prefixes.isBlank()) {
                boolean approved = Arrays.stream(prefixes.split(","))
                    .map(String::trim).filter(prefix -> !prefix.isBlank())
                    .anyMatch(prefix -> value.startsWith(prefix.replaceAll("/+$", "") + "/"));
                if (!approved) return false;
            }
            String filename = path.substring(path.lastIndexOf('/') + 1);
            int dot = filename.lastIndexOf('.');
            return dot > 0
                && !containsAbsoluteDate(value)
                && !containsAbsoluteDate(path)
                && !COMPACT_DATE.matcher(value).find()
                && !COMPACT_DATE.matcher(path).find()
                && (rawPath == null || !COMPACT_DATE.matcher(rawPath).find())
                && !LABELLED_MRN.matcher(value).find()
                && !LABELLED_MRN.matcher(path).find()
                && extensions.contains(filename.substring(dot + 1).toLowerCase(Locale.ROOT));
        } catch (URISyntaxException exception) {
            return false;
        }
    }

    private static boolean containsDecoded(String value, String identifier) {
        if (value.toLowerCase(Locale.ROOT).contains(identifier)) {
            return true;
        }
        try {
            URI uri = new URI(value);
            return uri.getPath() != null
                && uri.getPath().toLowerCase(Locale.ROOT).contains(identifier);
        } catch (URISyntaxException exception) {
            return true;
        }
    }

    private static boolean containsAbsoluteDate(String value) {
        return ABSOLUTE_DATE.matcher(value).find()
            || MONTH_FIRST_DATE.matcher(value).find()
            || DAY_FIRST_DATE.matcher(value).find()
            || NAMED_MONTH_DATE.matcher(value).find();
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
