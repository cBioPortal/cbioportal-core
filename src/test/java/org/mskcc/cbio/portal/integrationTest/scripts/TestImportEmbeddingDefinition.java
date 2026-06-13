package org.mskcc.cbio.portal.integrationTest.scripts;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mskcc.cbio.portal.dao.DaoEmbeddingDefinition;
import static org.junit.Assert.assertTrue;
import org.mskcc.cbio.portal.integrationTest.IntegrationTestBase;
import org.mskcc.cbio.portal.scripts.ImportEmbeddingDefinition;
import org.mskcc.cbio.portal.util.ProgressMonitor;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.File;

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
}