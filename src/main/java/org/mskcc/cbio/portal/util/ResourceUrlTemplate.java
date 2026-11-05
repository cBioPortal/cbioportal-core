/*
 * Copyright (c) 2026 Memorial Sloan Kettering Cancer Center.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package org.mskcc.cbio.portal.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;

/**
 * A resource definition's {@code URL_TEMPLATE}: the URL the portal builds for a resource row that
 * carries no URL of its own. Placeholders are written {@code ${name}}; the portal fills
 * {@code ${base_url}}, {@code ${STUDY_ID}}, {@code ${PATIENT_ID}}, {@code ${SAMPLE_ID}},
 * {@code ${RESOURCE_ID}} and {@code ${metadata.<key>}} when it serves the row. The importer stores
 * the template as given; the validator checks its syntax.
 */
public final class ResourceUrlTemplate {

    public static final String PATIENT_ID = "PATIENT_ID";
    public static final String SAMPLE_ID = "SAMPLE_ID";
    public static final String METADATA_PREFIX = "metadata.";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ResourceUrlTemplate() {
    }

    /**
     * The placeholder names in the template, in order, without {@code ${} and {@code }}. An
     * unterminated {@code ${} ends the scan.
     */
    public static List<String> placeholders(String template) {
        List<String> names = new ArrayList<>();
        int start = template.indexOf("${");
        while (start >= 0) {
            int end = template.indexOf('}', start + 2);
            if (end < 0) {
                break;
            }
            names.add(template.substring(start + 2, end));
            start = template.indexOf("${", end + 1);
        }
        return names;
    }

    /**
     * The values a row supplies to the template's row-level placeholders ({@code ${PATIENT_ID}},
     * {@code ${SAMPLE_ID}} and {@code ${metadata.<key>}}), in placeholder order, with null for a
     * value the row does not have.
     *
     * <p>The other placeholders are the same for every row of a resource in one import, so two
     * rows of one resource resolve to the same URL exactly when these lists are equal. The
     * importer uses that as the identity of a row that relies on the template.
     *
     * @param metadataJson the row's METADATA column; anything other than a JSON object supplies
     *                     no metadata values
     */
    public static List<String> rowValues(String template, String patientId, String sampleId,
                                         String metadataJson) {
        JsonNode metadata = parseObject(metadataJson);
        List<String> values = new ArrayList<>();
        for (String name : placeholders(template)) {
            if (PATIENT_ID.equals(name)) {
                values.add(patientId);
            } else if (SAMPLE_ID.equals(name)) {
                values.add(sampleId);
            } else if (name.startsWith(METADATA_PREFIX)) {
                JsonNode value = metadata == null ? null : metadata.get(name.substring(METADATA_PREFIX.length()));
                if (value == null || value.isNull()) {
                    values.add(null);
                } else {
                    values.add(value.isTextual() ? value.asText() : value.toString());
                }
            }
        }
        return values;
    }

    private static JsonNode parseObject(String json) {
        if (json == null || json.isEmpty()) {
            return null;
        }
        try {
            JsonNode node = MAPPER.readTree(json);
            return node != null && node.isObject() ? node : null;
        } catch (JsonProcessingException e) {
            return null;
        }
    }
}
