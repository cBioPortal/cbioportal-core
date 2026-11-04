package org.mskcc.cbio.portal.integrationTest.scripts;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mskcc.cbio.portal.dao.DaoCancerStudy;
import org.mskcc.cbio.portal.dao.DaoEmbeddingData;
import org.mskcc.cbio.portal.dao.DaoEmbeddingDefinition;
import org.mskcc.cbio.portal.dao.DaoException;
import org.mskcc.cbio.portal.integrationTest.IntegrationTestBase;
import org.mskcc.cbio.portal.model.CancerStudy;
import org.mskcc.cbio.portal.model.EmbeddingData;
import org.mskcc.cbio.portal.model.EmbeddingDefinition;
import org.mskcc.cbio.portal.scripts.ImportEmbeddingData;
import org.mskcc.cbio.portal.util.ProgressMonitor;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:/applicationContext-dao.xml" })
public class TestImportEmbeddingData extends IntegrationTestBase {

    private CancerStudy cancerStudy = null;

    /**
     * This is executed n times, for each of the n test methods below:
     * @throws DaoException
     */
    @Before
    public void setUp() throws DaoException
    {
        // new dummy study to simulate importing clinical data in empty study:
        cancerStudy = new CancerStudy("testnew","testnew","testnew","brca",true);
        cancerStudy.setReferenceGenome("hg19");
        DaoCancerStudy.addCancerStudy(cancerStudy);
        // implicit test:
        cancerStudy = DaoCancerStudy.getCancerStudyByStableId("testnew");

        // Insert embedding definitions matching the IDs used in data_embedding.txt
        DaoEmbeddingDefinition.addDatum(new EmbeddingDefinition(
                "umap_1", "UMAP 1", "UMAP Embedding", "UMAP coordinates based on H&E histopathology embeddings",
                "patient", "UMAP"
        ));
        DaoEmbeddingDefinition.addDatum(new EmbeddingDefinition(
                "pca_1", "PCA 1", "PCA Embedding", "PCA coordinates based on H&E histopathology embeddings",
                "sample", "PCA"
        ));
    }

    @Test
    public void importData() throws Exception {
        ProgressMonitor.setConsoleMode(false);
        String[] args = {
                "--data","src/test/resources/data_embedding.txt",
                "--meta","src/test/resources/meta_embedding.txt",
                "--loadMode", "bulkLoad"
        };

        ImportEmbeddingData importEmbeddingData = new ImportEmbeddingData(args);
        importEmbeddingData.run();
        // Check the definition exists
        assertTrue(DaoEmbeddingDefinition.checkDefinitionExists("umap_1"));
        assertTrue(DaoEmbeddingDefinition.checkDefinitionExists("pca_1"));
        // Check the actual embedding data exists
        List<EmbeddingData> data = DaoEmbeddingData.getEmbeddingDataByCancerStudy(cancerStudy.getInternalId());
        assertEquals(8, data.size());

        // Value-level assertions: confirm patientId/sampleId land in the correct fields,
        // not swapped. data_embedding.txt's pca_1 rows have genuinely distinguishable
        // patient vs. sample values (e.g. "TCGA-B6-A0WX" vs "TCGA-B6-A0WX-01"), so this
        // would fail loudly if the two ever got transposed again.
        EmbeddingData pcaRow = data.stream()
                .filter(d -> "TCGA-B6-A0WX-01".equals(d.getSampleId()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Expected a row with sampleId TCGA-B6-A0WX-01"));
        assertEquals("TCGA-B6-A0WX", pcaRow.getPatientId());
        assertEquals("TCGA-B6-A0WX-01", pcaRow.getSampleId());
        assertEquals(0.5, pcaRow.getX(), 0.001);
        assertEquals(1.9, pcaRow.getY(), 0.001);

        // umap_1 rows are patient-level: sampleId must come back empty, never null and
        // never accidentally holding the patient value.
        EmbeddingData umapRow = data.stream()
                .filter(d -> "TCGA-A2-A04P".equals(d.getPatientId()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Expected a row with patientId TCGA-A2-A04P"));
        assertEquals("", umapRow.getSampleId());
    }

    @Test
    public void importData_overwriteExisting_doesNotDuplicateRows() throws Exception {
        ProgressMonitor.setConsoleMode(false);
        String[] args = {
                "--data","src/test/resources/data_embedding.txt",
                "--meta","src/test/resources/meta_embedding.txt",
                "--loadMode", "bulkLoad"
        };
        new ImportEmbeddingData(args).run();
        assertEquals(8, DaoEmbeddingData.getEmbeddingDataByCancerStudy(cancerStudy.getInternalId()).size());

        // Re-running with --overwrite-existing must replace the previously loaded
        // rows for this definition, not pile duplicates on top of them.
        String[] overwriteArgs = {
                "--data","src/test/resources/data_embedding.txt",
                "--meta","src/test/resources/meta_embedding.txt",
                "--loadMode", "bulkLoad",
                "--overwrite-existing"
        };
        new ImportEmbeddingData(overwriteArgs).run();

        assertEquals(8, DaoEmbeddingData.getEmbeddingDataByCancerStudy(cancerStudy.getInternalId()).size());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void importData_unsupportedLoadMode_throws() throws Exception {
        ProgressMonitor.setConsoleMode(false);
        String[] args = {
                "--data","src/test/resources/data_embedding.txt",
                "--meta","src/test/resources/meta_embedding.txt",
                "--loadMode", "directLoad"
        };

        // This importer only ever bulk-loads; a directLoad request must be rejected
        // instead of silently falling back to bulk load.
        new ImportEmbeddingData(args).run();
    }

    @Test(expected = RuntimeException.class)
    public void importData_missingRequiredColumn_throws() throws Exception {
        ProgressMonitor.setConsoleMode(false);
        String[] args = {
                "--data", "src/test/resources/data_embedding_missing_column.txt",
                "--meta", "src/test/resources/meta_embedding.txt",
                "--loadMode", "bulkLoad"
        };

        // File has no X column at all — must fail fast with a clear error rather than
        // silently importing incomplete points.
        new ImportEmbeddingData(args).run();
    }

    @Test(expected = IllegalArgumentException.class)
    public void importData_malformedRow_throws() throws Exception {
        ProgressMonitor.setConsoleMode(false);
        String[] args = {
                "--data", "src/test/resources/data_embedding_malformed_row.txt",
                "--meta", "src/test/resources/meta_embedding.txt",
                "--loadMode", "bulkLoad"
        };

        // Data row has fewer fields than the header — must be rejected instead of
        // silently misaligning columns.
        new ImportEmbeddingData(args).run();
    }

    @Test
    public void importData_unknownEmbeddingDefinition_throwsIllegalArgumentException() throws Exception {
        ProgressMonitor.setConsoleMode(false);
        String[] args = {
                "--data", "src/test/resources/data_embedding_unknown_definition.txt",
                "--meta", "src/test/resources/meta_embedding.txt",
                "--loadMode", "bulkLoad"
        };

        try {
            new ImportEmbeddingData(args).run();
            fail("Expected an IllegalArgumentException for an EMBEDDING_ID with no matching definition");
        } catch (IllegalArgumentException e) {
            // run() rethrows RuntimeExceptions (including this one) unwrapped, so the
            // message itself must identify what actually went wrong, not a generic failure.
            assertTrue(e.getMessage().contains("Embedding definition not found"));
        }
    }
}
