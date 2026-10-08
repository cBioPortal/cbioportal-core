/*
 * Copyright (c) 2026 Memorial Sloan-Kettering Cancer Center.
 *
 * This file is part of cBioPortal and is licensed under the AGPL.
 */

package org.mskcc.cbio.portal.integrationTest.scripts;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mskcc.cbio.portal.dao.ClickHouseAutoIncrement;
import org.mskcc.cbio.portal.dao.ClickHouseBulkLoader;
import org.mskcc.cbio.portal.dao.DaoCancerStudy;
import org.mskcc.cbio.portal.dao.DaoResourceData;
import org.mskcc.cbio.portal.dao.JdbcUtil;
import org.mskcc.cbio.portal.integrationTest.IntegrationTestBase;
import org.mskcc.cbio.portal.model.CancerStudy;
import org.mskcc.cbio.portal.scripts.ImportResourceData;
import org.mskcc.cbio.portal.scripts.ImportResourceDefinition;
import org.mskcc.cbio.portal.util.ProgressMonitor;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Imports the resource files produced by scripts/importer/convertWsiToResources.py from the
 * legacy WSI fixture (tests/test_data/wsi_convert) into ClickHouse and checks the resource
 * lifecycle: metadata JSON survives the round trip, resource IDs stay unique across importer
 * processes, and a deleted study can be imported again.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:/applicationContext-dao.xml" })
public class TestImportResourceData extends IntegrationTestBase {

    private static final String STUDY_ID = "wsi_convert_test";
    private static final String FIXTURE_DIR = "src/test/resources/wsi_resources/";
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String SLIDE_1 = "2c96f13783250ad2c6bcfcd5b7c3ef22";
    private static final String SLIDE_2 = "90f033ed369247b19bd7a252e7ae86c8";
    private static final String SLIDE_3 = "f9bce50b1498c94fa0fa6cce809f64f2";
    private static final String SLIDE_4 = "c66ee336f70e8b59e48bd7afbf0e606d";
    private static final String SLIDE_5 = "a72487fd68ef59b99fcee054e738f893";
    private static final String SLIDE_6 = "3474b861eb683902b420f4ee95dfe0fa";
    // contract wsi-serving-v6 test vector, the fixture's sealed source for SLIDE_1
    private static final String SEALED_SOURCE_VECTOR =
        "AAECAwQFBgcICQoLNpOTYnY8HVb-KcPzu1F5HPykr7D0YY_UhVbbyjOFRlxC63fxCt09YO1aYC-phb85wDhN5PPPpC0X46RS"
            + "D0K0bRRgSptRb9wDiqMLtftFQ6VpBGfGILaddEV_s-Zsmpp28fG5Z3XTNWnyoBRa9qfr9t209wS7V-LhGl8i";

    private record ResourceRow(long resourceDataId, String resourceId, String entityType,
                               String patientId, String sampleId, String url, String type,
                               String metadata) {
    }

    @Before
    public void setUp() {
        ProgressMonitor.resetWarnings();
    }

    @Test
    public void testImportConvertedWsiResources() throws Exception {
        CancerStudy study = createStudy();
        importConvertedResources(study);

        Map<String, ResourceRow> rows = rowsBySlideKey(study);
        assertEquals(List.of(SLIDE_1, SLIDE_2, SLIDE_3, SLIDE_5, SLIDE_4, SLIDE_6),
            new ArrayList<>(rows.keySet()));

        ResourceRow first = rows.get(SLIDE_1);
        assertEquals("WSI_SAMPLE", first.resourceId());
        assertEquals("SAMPLE", first.entityType());
        assertEquals("WSI-P1", first.patientId());
        assertEquals("WSI-P1-S1", first.sampleId());
        assertEquals("WHOLE_SLIDE_IMAGE", first.type());
        assertEquals("https://portal.example.org/cbioportal/wsi/patient/WSI-P1"
            + "?studyId=wsi_convert_test&slideKey=2c96f13783250ad2c6bcfcd5b7c3ef22", first.url());
        JsonNode metadata = JSON.readTree(first.metadata());
        assertEquals("2c96f13783250ad2c6bcfcd5b7c3ef22", metadata.get("slide_key").textValue());
        for (String removed : List.of("image_id", "barcode", "part_designator", "path_dx_title",
                "source_url", "thumbnail_url", "sealed_source")) {
            assertFalse(removed, metadata.has(removed));
        }
        // slide timing is not part of the foundation metadata
        for (String timing : List.of("timeline_start_days", "timeline_date_status", "timepoint_source")) {
            assertFalse(timing, metadata.has(timing));
        }
        assertTrue(metadata.get("is_hne").isBoolean());
        assertTrue(metadata.get("is_hne").booleanValue());
        assertFalse(metadata.get("is_ihc").booleanValue());
        assertEquals(716956681L, metadata.get("file_size_bytes").longValue());
        assertEquals("1", metadata.get("part_number").textValue());
        assertEquals("Left \"upper\" lobe \\ wedge", metadata.get("part_description").textValue());
        JsonNode serving = metadata.get("wsi_serving");
        assertEquals(List.of("sealed_source", "thumbnail_content_type", "thumbnail_height",
            "thumbnail_width", "tile_metadata_json"), fieldNames(serving));
        assertEquals(SEALED_SOURCE_VECTOR, serving.get("sealed_source").textValue());
        assertEquals(256, serving.get("thumbnail_width").intValue());
        assertEquals(1024, serving.at("/tile_metadata_json/dimensions/width").intValue());
        assertEquals("Scan \"Q\" \\ 40", serving.at("/tile_metadata_json/vendor/scanner/model").textValue());
        assertTrue(serving.at("/tile_metadata_json/vendor/scanner/calibrated").booleanValue());

        JsonNode unservable = JSON.readTree(rows.get(SLIDE_2).metadata());
        assertFalse(unservable.get("can_serve_tiles").booleanValue());
        assertFalse(unservable.has("wsi_serving"));

        assertEquals("WSI-P2-S1", rows.get(SLIDE_5).sampleId());

        ResourceRow unmatched = rows.get(SLIDE_6);
        assertEquals("WSI_PATIENT", unmatched.resourceId());
        assertEquals("PATIENT", unmatched.entityType());
        assertEquals("WSI+P3", unmatched.patientId());
        assertNull(unmatched.sampleId());
        assertEquals("https://portal.example.org/cbioportal/wsi/patient/WSI%2BP3"
            + "?studyId=wsi_convert_test&slideKey=3474b861eb683902b420f4ee95dfe0fa", unmatched.url());
        assertEquals("WSI-P1", rows.get(SLIDE_4).patientId());
        assertNull(rows.get(SLIDE_4).sampleId());

        assertUniquePositiveIds(rows.values());
    }

    @Test
    public void testResourceIdsStayUniqueAcrossProcessesAndStudyReimport() throws Exception {
        CancerStudy study = createStudy();
        importConvertedResources(study);
        Set<Long> firstIds = ids(rowsBySlideKey(study).values());

        // A second importer process starts with empty in-memory counters.
        ClickHouseAutoIncrement.resetCounters();
        importConvertedResources(study);
        Map<String, ResourceRow> reimported = rowsBySlideKey(study);
        assertEquals("reimport replaces rows instead of duplicating them", 6, reimported.size());
        // rowsBySlideKey collapses duplicates by key, so also count the stored rows directly.
        assertEquals("reimport leaves exactly one row per slide", 6L, singleLong(
            "SELECT count() FROM resource_data WHERE cancer_study_id = ? AND type = 'WHOLE_SLIDE_IMAGE'",
            study.getInternalId()));
        Set<Long> secondIds = ids(reimported.values());
        assertUniquePositiveIds(reimported.values());
        assertTrue("reimported IDs must not reuse earlier IDs: " + firstIds + " / " + secondIds,
            disjoint(firstIds, secondIds));

        // The direct (non-bulk) insert path allocates from the same sequence.
        ClickHouseBulkLoader.bulkLoadOff();
        DaoResourceData.addResourceDatum(study.getInternalId(), "WSI_PATIENT", "PATIENT", "WSI-P1",
            null, "https://portal.example.org/cbioportal/direct", "direct", "LINK", null);
        long directId = singleLong(
            "SELECT resource_data_id FROM resource_data WHERE cancer_study_id = ? AND display_name = 'direct'",
            study.getInternalId());
        assertNotEquals(0L, directId);
        assertFalse(firstIds.contains(directId) || secondIds.contains(directId));

        DaoCancerStudy.deleteCancerStudy(study.getInternalId());
        assertEquals(0L, singleLong("SELECT count() FROM resource_data WHERE cancer_study_id = ?",
            study.getInternalId()));
        assertEquals(0L, singleLong("SELECT count() FROM resource_definition WHERE cancer_study_id = ?",
            study.getInternalId()));
        assertNull(DaoCancerStudy.getCancerStudyByStableId(STUDY_ID));

        ClickHouseAutoIncrement.resetCounters();
        CancerStudy recreated = createStudy();
        importConvertedResources(recreated);
        Map<String, ResourceRow> afterDelete = rowsBySlideKey(recreated);
        assertEquals(6, afterDelete.size());
        assertUniquePositiveIds(afterDelete.values());

        // Sequences declared with lower-case columns still initialize after a reset.
        ClickHouseAutoIncrement.resetCounters();
        long maxStudyId = singleLong("SELECT max(cancer_study_id) FROM cancer_study");
        assertTrue(ClickHouseAutoIncrement.nextId("seq_cancer_study") > maxStudyId);
        assertTrue(ClickHouseAutoIncrement.nextId("seq_clinical_event") > 0);
        long maxResourceId = singleLong("SELECT max(resource_data_id) FROM resource_data");
        assertTrue(ClickHouseAutoIncrement.nextId("seq_resource_data") > maxResourceId);
    }

    private static CancerStudy createStudy() throws Exception {
        CancerStudy study = new CancerStudy("WSI conversion", "Converted WSI fixture", STUDY_ID, "brca", true);
        study.setReferenceGenome("hg19");
        DaoCancerStudy.addCancerStudy(study);
        return DaoCancerStudy.getCancerStudyByStableId(STUDY_ID);
    }

    private static void importConvertedResources(CancerStudy study) throws Exception {
        ImportResourceDefinition definitions = new ImportResourceDefinition(null);
        definitions.setFile(study, new File(FIXTURE_DIR + "data_resource_definition.txt"), false);
        definitions.importData();
        ImportResourceData samples = new ImportResourceData(null);
        samples.setFile(study, new File(FIXTURE_DIR + "data_resource_sample.txt"), "SAMPLE", false);
        samples.importData();
        assertEquals(4, samples.getNumSampleSpecificResourcesAdded());
        ImportResourceData patients = new ImportResourceData(null);
        patients.setFile(study, new File(FIXTURE_DIR + "data_resource_patient.txt"), "PATIENT", false);
        patients.importData();
        assertEquals(2, patients.getNumPatientSpecificResourcesAdded());
    }

    private static Map<String, ResourceRow> rowsBySlideKey(CancerStudy study) throws Exception {
        Map<String, ResourceRow> rows = new LinkedHashMap<>();
        try (Connection connection = JdbcUtil.getDbConnection(TestImportResourceData.class);
             PreparedStatement statement = connection.prepareStatement(
                 "SELECT resource_data_id, resource_id, entity_type, patient_id, sample_id, url, type, metadata "
                     + "FROM resource_data WHERE cancer_study_id = ? AND type = 'WHOLE_SLIDE_IMAGE' "
                     + "ORDER BY resource_id DESC, resource_data_id")) {
            statement.setInt(1, study.getInternalId());
            try (ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    ResourceRow row = new ResourceRow(result.getLong(1), result.getString(2),
                        result.getString(3), result.getString(4), result.getString(5),
                        result.getString(6), result.getString(7), result.getString(8));
                    rows.put(JSON.readTree(row.metadata()).get("slide_key").textValue(), row);
                }
            }
        }
        return rows;
    }

    private static List<String> fieldNames(JsonNode node) {
        List<String> names = new ArrayList<>();
        node.fieldNames().forEachRemaining(names::add);
        Collections.sort(names);
        return names;
    }

    private static long singleLong(String sql, Object... parameters) throws Exception {
        try (Connection connection = JdbcUtil.getDbConnection(TestImportResourceData.class);
             PreparedStatement statement = connection.prepareStatement(sql)) {
            for (int i = 0; i < parameters.length; i++) {
                statement.setObject(i + 1, parameters[i]);
            }
            try (ResultSet result = statement.executeQuery()) {
                assertTrue(result.next());
                return result.getLong(1);
            }
        }
    }

    private static Set<Long> ids(Iterable<ResourceRow> rows) {
        Set<Long> ids = new HashSet<>();
        rows.forEach(row -> ids.add(row.resourceDataId()));
        return ids;
    }

    private static void assertUniquePositiveIds(java.util.Collection<ResourceRow> rows) {
        Set<Long> ids = ids(rows);
        assertEquals("resource_data_id values must be unique", rows.size(), ids.size());
        assertTrue("resource_data_id values must be allocated", ids.stream().allMatch(id -> id > 0));
    }

    private static boolean disjoint(Set<Long> left, Set<Long> right) {
        Set<Long> overlap = new HashSet<>(left);
        overlap.retainAll(right);
        return overlap.isEmpty();
    }
}
