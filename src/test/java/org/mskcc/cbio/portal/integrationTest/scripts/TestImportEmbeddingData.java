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
//        DaoCancerStudy.reCacheAll();
//        ProgressMonitor.resetWarnings();

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
        List<EmbeddingData> data = DaoEmbeddingData.getEmbeddingDataByCancerStudy(2);
        assertEquals(8, data.size());
    }
}
