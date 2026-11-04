package org.mskcc.cbio.portal.integrationTest.scripts;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mskcc.cbio.portal.dao.DaoEmbeddingDefinition;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.mskcc.cbio.portal.integrationTest.IntegrationTestBase;
import org.mskcc.cbio.portal.model.EmbeddingDefinition;
import org.mskcc.cbio.portal.scripts.ImportEmbeddingDefinition;
import org.mskcc.cbio.portal.util.ProgressMonitor;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner .class)
@ContextConfiguration(locations = { "classpath:/applicationContext-dao.xml" })
public class TestImportEmbeddingDefinition extends IntegrationTestBase {

    @Test
    public void importData() throws Exception {
        ProgressMonitor.setConsoleMode(false);
        String[] args = {
                "--data","src/test/resources/data_embedding_definition.txt",
                "--noprogress"
        };

        ImportEmbeddingDefinition importEmbeddingDefinition = new ImportEmbeddingDefinition(args);
        importEmbeddingDefinition.run();
       //for now we will change this later to use the script to check existence of t instead of the Dao
       assertTrue(DaoEmbeddingDefinition.checkDefinitionExists("mosaic_patient"));
       assertTrue(DaoEmbeddingDefinition.checkDefinitionExists("patient_record_embedding"));
    }

    @Test
    public void importData_duplicateLines_areSkippedNotReinserted() throws Exception {
        ProgressMonitor.setConsoleMode(false);
        String[] args = {
                "--data","src/test/resources/data_embedding_definition_test_with_duplicate.txt",
                "--noprogress"
        };

        ImportEmbeddingDefinition importEmbeddingDefinition = new ImportEmbeddingDefinition(args);
        // File has 4 duplicate lines for mosaic_patient followed by one line for
        // patient_record_embedding. The skip-if-exists branch must absorb the repeats
        // without throwing, and both real definitions must still end up present.
        importEmbeddingDefinition.run();

        assertTrue(DaoEmbeddingDefinition.checkDefinitionExists("mosaic_patient"));
        assertTrue(DaoEmbeddingDefinition.checkDefinitionExists("patient_record_embedding"));

        int mosaicId = DaoEmbeddingDefinition.getDefinitionId("mosaic_patient");
        int patientRecordId = DaoEmbeddingDefinition.getDefinitionId("patient_record_embedding");
        assertEquals(1, mosaicId);
        assertEquals(2, patientRecordId);

        // internal_id is assigned by ClickHouseAutoIncrement, which only advances on an
        // actual insert. The file has 5 lines but only 2 distinct embedding_ids, so a
        // freshly imported third definition must get internal_id 3 — not 6 or 7 — proving
        // the 4 duplicate mosaic_patient lines never consumed extra sequence values.
        DaoEmbeddingDefinition.addDatum(new EmbeddingDefinition(
                "brand_new_definition", "New", "New Embedding", "new description",
                "patient", "UMAP"
        ));
        assertEquals(3, DaoEmbeddingDefinition.getDefinitionId("brand_new_definition"));
    }
}