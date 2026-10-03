package org.mskcc.cbio.portal.integrationTest.dao;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mskcc.cbio.portal.dao.DaoEmbeddingDefinition;
import org.mskcc.cbio.portal.dao.DaoException;
import org.mskcc.cbio.portal.integrationTest.IntegrationTestBase;
import org.mskcc.cbio.portal.model.EmbeddingDefinition;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:/applicationContext-dao.xml" })
public class TestDaoEmbeddingDefinition extends IntegrationTestBase {

    /**
     * Tests .
     * @throws DaoException Database Error.
     */
    @Test
    public void addDatum() throws DaoException {
        EmbeddingDefinition emb = new EmbeddingDefinition(
                "umap_rna",
                "RNA UMAP",
                "RNA Expression UMAP Embedding",
                "UMAP coordinates based on H&E histopathology embeddings",
                "sample",
                "UMAP"
        );
        DaoEmbeddingDefinition.addDatum(emb);
        assertTrue(
                DaoEmbeddingDefinition.checkDefinitionExists(
                        "umap_rna"
                )
        );
    }

    @Test
    public void getDefinitionId() throws DaoException{
        // insert into the database
        // get the id
        // assert that it is  equal
        EmbeddingDefinition emb = new EmbeddingDefinition(
                "umap_rna",
                "RNA UMAP",
                "RNA Expression UMAP Embedding",
                "UMAP coordinates based on H&E histopathology embeddings",
                "sample",
                "UMAP"
        );
        DaoEmbeddingDefinition.addDatum(emb);
        // Hardcoded to 1 because only one definition is added, meaning the definition id will be one.
        // This assumption is based on how ClickHouseAutoIncrement works.
        assertEquals(1,DaoEmbeddingDefinition.getDefinitionId(emb.getEmbeddingId()));
    }


}
