package org.mskcc.cbio.portal.integrationTest.scripts;

import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mskcc.cbio.portal.dao.DaoCancerStudy;
import org.mskcc.cbio.portal.dao.DaoException;
import org.mskcc.cbio.portal.dao.DaoFacetsCncf;
import org.mskcc.cbio.portal.dao.DaoSample;
import org.mskcc.cbio.portal.integrationTest.IntegrationTestBase;
import org.mskcc.cbio.portal.model.CancerStudy;
import org.mskcc.cbio.portal.model.FacetsCncfSegment;
import org.mskcc.cbio.portal.model.Sample;
import org.mskcc.cbio.portal.scripts.ImportFacetsCncfData;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.junit.Assert.assertEquals;

/**
 * Tests the import of FACETS CNCF (allele-specific copy number segment) data.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:/applicationContext-dao.xml" })
public class TestImportFacetsCncfData extends IntegrationTestBase {

    private static final String STUDY_ID = "study_tcga_pub";
    private static final String SAMPLE_ID = "TCGA-A1-A0SE-01";

    private CancerStudy cancerStudy;

    @Before
    public void setUp() throws DaoException {
        cancerStudy = DaoCancerStudy.getCancerStudyByStableId(STUDY_ID);
    }

    @Test
    public void testImportFacetsCncfData() throws Exception {
        String[] args = {
                "--data", "src/test/resources/facets/data_facets_cncf.txt",
                "--meta", "src/test/resources/facets/meta_facets_cncf.txt",
                "--loadMode", "bulkLoad"
        };
        ImportFacetsCncfData runner = new ImportFacetsCncfData(args);
        runner.run();

        Sample sample = DaoSample.getSampleByCancerStudyAndSampleId(cancerStudy.getInternalId(), SAMPLE_ID);
        List<FacetsCncfSegment> segments = DaoFacetsCncf.getSegmentsForSample(sample.getInternalId(), cancerStudy.getInternalId());
        assertEquals(3, segments.size());

        FacetsCncfSegment first = segments.stream()
                .filter(s -> s.getStart() == 3218610)
                .findFirst()
                .orElseThrow();
        assertEquals("1", first.getChr());
        assertEquals(95674710L, first.getEnd());
        assertEquals(2.0, first.getTcn(), 0.0001);
        assertEquals(1.0, first.getLcn(), 0.0001);
        assertEquals(0.95, first.getCellularFraction(), 0.0001);
        assertEquals(0.4, first.getPurity(), 0.0001);
    }
}
