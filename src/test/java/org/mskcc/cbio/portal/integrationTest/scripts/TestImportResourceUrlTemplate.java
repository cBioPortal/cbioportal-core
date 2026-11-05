/*
 * Copyright (c) 2026 Memorial Sloan Kettering Cancer Center.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package org.mskcc.cbio.portal.integrationTest.scripts;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mskcc.cbio.portal.dao.DaoCancerStudy;
import org.mskcc.cbio.portal.dao.DaoException;
import org.mskcc.cbio.portal.dao.DaoResourceDefinition;
import org.mskcc.cbio.portal.dao.JdbcUtil;
import org.mskcc.cbio.portal.integrationTest.IntegrationTestBase;
import org.mskcc.cbio.portal.model.CancerStudy;
import org.mskcc.cbio.portal.scripts.ImportResourceData;
import org.mskcc.cbio.portal.scripts.ImportResourceDefinition;
import org.mskcc.cbio.portal.util.ProgressMonitor;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Tests the optional URL_TEMPLATE column of the resource definition file and the resource data
 * rows that rely on it by leaving URL empty.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:/applicationContext-dao.xml" })
public class TestImportResourceUrlTemplate extends IntegrationTestBase {

    private static final String SLIDE_TEMPLATE =
        "${base_url}/wsi/patient/${PATIENT_ID}?studyId=${STUDY_ID}&slideKey=${metadata.slide_key}";

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private CancerStudy cancerStudy;

    @Before
    public void setUp() throws DaoException {
        DaoCancerStudy.reCacheAll();
        ProgressMonitor.resetWarnings();
        cancerStudy = DaoCancerStudy.getCancerStudyByStableId("study_tcga_pub");
    }

    @Test
    public void urlTemplateIsStoredWithTheDefinition() throws Exception {
        importDefinitions("resource_definition_url_template.txt");

        int studyId = cancerStudy.getInternalId();
        assertEquals(SLIDE_TEMPLATE, DaoResourceDefinition.getDatum("WSI_SLIDE", studyId).getUrlTemplate());
        assertEquals("${base_url}/study/summary?id=${STUDY_ID}",
            DaoResourceDefinition.getDatum("STUDY_PAGE", studyId).getUrlTemplate());
        // An empty URL_TEMPLATE cell means the resource has no template.
        assertNull(DaoResourceDefinition.getDatum("PATHOLOGY_REPORT", studyId).getUrlTemplate());
    }

    @Test
    public void aDefinitionFileWithoutTheColumnStillImports() throws Exception {
        ImportResourceDefinition importer = importDefinitions("resource_definition_without_url_template.txt");

        assertEquals(1, importer.getNumResourceDefinitionsAdded());
        assertNull(DaoResourceDefinition.getDatum("PATHOLOGY_REPORT", cancerStudy.getInternalId()).getUrlTemplate());
    }

    /**
     * A row of a resource with a template may leave URL empty and is stored with an empty url;
     * a row's own URL is kept as given. A resource without a template still skips a row with no
     * URL, as before.
     */
    @Test
    public void sampleRowsWithoutUrlAreImportedWhenTheResourceHasATemplate() throws Exception {
        importDefinitions("resource_definition_url_template.txt");
        ImportResourceData importer = importData("resource_sample_url_template.txt", "SAMPLE");

        assertEquals(3, importer.getNumSampleSpecificResourcesAdded());
        assertEquals(1, importer.getNumEmptyResourcesSkipped());
        assertEquals(List.of("TCGA-A1-A0SD-01|", "TCGA-A1-A0SD-01|", "TCGA-A1-A0SE-01|https://example.org/slides/own.svs"),
            readRows("WSI_SLIDE", "sample_id"));
        assertEquals(List.of(), readRows("PATHOLOGY_REPORT", "sample_id"));
    }

    /**
     * Two rows relying on the template are duplicates when they give it the same values, since
     * they resolve to the same URL. Metadata the template does not read does not tell them apart.
     */
    @Test
    public void rowsGivingTheTemplateTheSameValuesAreDuplicates() throws Exception {
        importDefinitions("resource_definition_url_template.txt");

        exception.expect(RuntimeException.class);
        exception.expectMessage("Duplicated sample resource in file");
        importData("resource_sample_url_template_duplicate.txt", "SAMPLE");
    }

    @Test
    public void patientRowsWithoutUrlAreImportedWhenTheResourceHasATemplate() throws Exception {
        importDefinitions("resource_definition_url_template.txt");
        ImportResourceData importer = importData("resource_patient_url_template.txt", "PATIENT");

        assertEquals(2, importer.getNumPatientSpecificResourcesAdded());
        assertEquals(List.of("TCGA-A1-A0SD|", "TCGA-A1-A0SE|"), readRows("PATIENT_PAGE", "patient_id"));
    }

    @Test
    public void studyRowsWithoutUrlAreImportedWhenTheResourceHasATemplate() throws Exception {
        importDefinitions("resource_definition_url_template.txt");
        ImportResourceData importer = importData("resource_study_url_template.txt", "STUDY");

        assertEquals(1, importer.getNumStudySpecificResourcesAdded());
        assertEquals(List.of("STUDY|"), readRows("STUDY_PAGE", "resource_scope"));
    }

    private ImportResourceDefinition importDefinitions(String file) throws Exception {
        ImportResourceDefinition importer = new ImportResourceDefinition(null);
        importer.setFile(cancerStudy, new File("src/test/resources/" + file), false);
        importer.importData();
        return importer;
    }

    private ImportResourceData importData(String file, String resourceType) throws Exception {
        ImportResourceData importer = new ImportResourceData(null);
        importer.setFile(cancerStudy, new File("src/test/resources/" + file), resourceType, false);
        importer.importData();
        return importer;
    }

    /** "{idColumn}|{url}" for each row of the resource, sorted. */
    private List<String> readRows(String resourceId, String idColumn) throws DaoException {
        Connection con = null;
        try {
            con = JdbcUtil.getDbConnection(TestImportResourceUrlTemplate.class);
            PreparedStatement ps = con.prepareStatement(
                "SELECT " + idColumn + ", url FROM resource WHERE cancer_study_id = ? AND resource_id = ?"
                    + " ORDER BY " + idColumn + ", url");
            ps.setInt(1, cancerStudy.getInternalId());
            ps.setString(2, resourceId);
            ResultSet rs = ps.executeQuery();
            List<String> rows = new ArrayList<>();
            while (rs.next()) {
                rows.add(rs.getString(1) + "|" + rs.getString(2));
            }
            return rows;
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(TestImportResourceUrlTemplate.class, con, null, null);
        }
    }
}
