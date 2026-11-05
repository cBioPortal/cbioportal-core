/*
 * Copyright (c) 2026 Memorial Sloan Kettering Cancer Center.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package org.mskcc.cbio.portal.util;

import java.util.Arrays;
import java.util.List;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotEquals;

public class TestResourceUrlTemplate {

    private static final String SLIDE_TEMPLATE =
        "${base_url}/wsi/patient/${PATIENT_ID}?studyId=${STUDY_ID}&slideKey=${metadata.slide_key}";

    @Test
    public void placeholdersAreListedInOrder() {
        assertEquals(List.of("base_url", "PATIENT_ID", "STUDY_ID", "metadata.slide_key"),
            ResourceUrlTemplate.placeholders(SLIDE_TEMPLATE));
        assertEquals(List.of(), ResourceUrlTemplate.placeholders("https://example.org/static"));
        // An unterminated placeholder ends the scan rather than swallowing the rest.
        assertEquals(List.of("PATIENT_ID"),
            ResourceUrlTemplate.placeholders("https://x/${PATIENT_ID}/${SAMPLE_ID"));
    }

    @Test
    public void rowValuesAreTheRowLevelPlaceholderValues() {
        assertEquals(Arrays.asList("P-1", "abc"),
            ResourceUrlTemplate.rowValues(SLIDE_TEMPLATE, "P-1", "S-1", "{\"slide_key\": \"abc\", \"stain\": \"H&E\"}"));
        // Non-string metadata values are compared by their JSON text.
        assertEquals(Arrays.asList("P-1", "12"),
            ResourceUrlTemplate.rowValues(SLIDE_TEMPLATE, "P-1", null, "{\"slide_key\": 12}"));
    }

    @Test
    public void absentValuesAreNull() {
        assertEquals(Arrays.asList("P-1", null),
            ResourceUrlTemplate.rowValues(SLIDE_TEMPLATE, "P-1", null, null));
        assertEquals(Arrays.asList("P-1", null),
            ResourceUrlTemplate.rowValues(SLIDE_TEMPLATE, "P-1", null, "[\"not\", \"an object\"]"));
        assertEquals(Arrays.asList("P-1", null),
            ResourceUrlTemplate.rowValues(SLIDE_TEMPLATE, "P-1", null, "{not json"));
    }

    @Test
    public void metadataTheTemplateDoesNotReadDoesNotDistinguishRows() {
        assertEquals(
            ResourceUrlTemplate.rowValues(SLIDE_TEMPLATE, "P-1", "S-1", "{\"slide_key\": \"abc\", \"stain\": \"H&E\"}"),
            ResourceUrlTemplate.rowValues(SLIDE_TEMPLATE, "P-1", "S-1", "{\"slide_key\": \"abc\", \"stain\": \"IHC\"}"));
        assertNotEquals(
            ResourceUrlTemplate.rowValues(SLIDE_TEMPLATE, "P-1", "S-1", "{\"slide_key\": \"abc\"}"),
            ResourceUrlTemplate.rowValues(SLIDE_TEMPLATE, "P-1", "S-1", "{\"slide_key\": \"def\"}"));
    }

    @Test
    public void onlyBaseUrlAndHttpStartsAreAllowed() {
        assertTrue(ResourceUrlTemplate.hasAllowedStart(SLIDE_TEMPLATE));
        assertTrue(ResourceUrlTemplate.hasAllowedStart("https://example.org/${PATIENT_ID}"));
        assertTrue(ResourceUrlTemplate.hasAllowedStart("http://example.org/${PATIENT_ID}"));
        // The scheme is compared case-insensitively, the placeholder name is not.
        assertTrue(ResourceUrlTemplate.hasAllowedStart("HTTPS://example.org/"));
        assertFalse(ResourceUrlTemplate.hasAllowedStart("${BASE_URL}/x"));
        assertFalse(ResourceUrlTemplate.hasAllowedStart("javascript:alert(1)"));
        assertFalse(ResourceUrlTemplate.hasAllowedStart("ftp://example.org/x"));
        assertFalse(ResourceUrlTemplate.hasAllowedStart("/relative/${PATIENT_ID}"));
        assertFalse(ResourceUrlTemplate.hasAllowedStart(" https://leading.space/"));
        assertFalse(ResourceUrlTemplate.hasAllowedStart(null));
    }
}
