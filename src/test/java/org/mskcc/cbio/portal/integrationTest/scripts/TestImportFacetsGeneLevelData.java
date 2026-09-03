package org.mskcc.cbio.portal.integrationTest.scripts;

import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mskcc.cbio.portal.dao.DaoCancerStudy;
import org.mskcc.cbio.portal.dao.DaoException;
import org.mskcc.cbio.portal.dao.DaoFacetsGenes;
import org.mskcc.cbio.portal.dao.DaoSample;
import org.mskcc.cbio.portal.integrationTest.IntegrationTestBase;
import org.mskcc.cbio.portal.model.CancerStudy;
import org.mskcc.cbio.portal.model.FacetsGeneLevelRecord;
import org.mskcc.cbio.portal.model.Sample;
import org.mskcc.cbio.portal.scripts.ImportFacetsGeneLevelData;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests the import of FACETS gene-level data, derived from (but not directly
 * mapped to) FACETS CNCF segment calls.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:/applicationContext-dao.xml" })
public class TestImportFacetsGeneLevelData extends IntegrationTestBase {

    private static final String STUDY_ID = "study_tcga_pub";
    private static final String SAMPLE_ID = "TCGA-A1-A0SE-01";

    private CancerStudy cancerStudy;

    @Before
    public void setUp() throws DaoException {
        cancerStudy = DaoCancerStudy.getCancerStudyByStableId(STUDY_ID);
    }

    @Test
    public void testImportFacetsGeneLevelData() throws Exception {
        String[] args = {
                "--data", "src/test/resources/facets/data_facets_genes.txt",
                "--meta", "src/test/resources/facets/meta_facets_genes.txt",
                "--loadMode", "bulkLoad"
        };
        ImportFacetsGeneLevelData runner = new ImportFacetsGeneLevelData(args);
        runner.run();

        Sample sample = DaoSample.getSampleByCancerStudyAndSampleId(cancerStudy.getInternalId(), SAMPLE_ID);
        List<FacetsGeneLevelRecord> records = DaoFacetsGenes.getGeneLevelDataForSample(sample.getInternalId(), cancerStudy.getInternalId());

        // NOT_A_REAL_GENE should have been skipped since it does not resolve to a known gene
        assertEquals(2, records.size());
        assertTrue(records.stream().noneMatch(r -> "NOT_A_REAL_GENE".equals(r.getHugoGeneSymbol())));

        FacetsGeneLevelRecord tp53 = records.stream()
                .filter(r -> "TP53".equals(r.getHugoGeneSymbol()))
                .findFirst()
                .orElseThrow();
        assertEquals(7157L, tp53.getEntrezGeneId());
        assertEquals("17", tp53.getChr());
        assertEquals(2.0, tp53.getTcn(), 0.0001);
        assertEquals(1.0, tp53.getLcn(), 0.0001);
        assertEquals(0.95, tp53.getCellularFraction(), 0.0001);
        assertEquals(0.4, tp53.getPurity(), 0.0001);
    }
}
