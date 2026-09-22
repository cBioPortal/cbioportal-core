package org.mskcc.cbio.portal.integrationTest.incremental;

import java.io.File;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mskcc.cbio.portal.dao.ClickHouseBulkLoader;
import org.mskcc.cbio.portal.dao.DaoCancerStudy;
import org.mskcc.cbio.portal.dao.DaoException;
import org.mskcc.cbio.portal.dao.DaoAscnCncf;
import org.mskcc.cbio.portal.dao.DaoSample;
import org.mskcc.cbio.portal.integrationTest.IntegrationTestBase;
import org.mskcc.cbio.portal.model.CancerStudy;
import org.mskcc.cbio.portal.model.AscnCncfSegment;
import org.mskcc.cbio.portal.model.Sample;
import org.mskcc.cbio.portal.scripts.ImportAscnCncfData;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests incremental (--overwrite-existing) import of ASCN CNCF data.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:/applicationContext-dao.xml" })
public class TestIncrementalAscnCncfDataImport extends IntegrationTestBase {

    public static final String STUDY_ID = "study_tcga_pub";
    private static final String SAMPLE_ID = "TCGA-A1-A0SE-01";

    private CancerStudy cancerStudy;

    @Before
    public void setUp() throws DaoException {
        cancerStudy = DaoCancerStudy.getCancerStudyByStableId(STUDY_ID);
    }

    @Test
    public void testIncrementalUpload() throws DaoException {
        Sample sample = DaoSample.getSampleByCancerStudyAndSampleId(cancerStudy.getInternalId(), SAMPLE_ID);

        ClickHouseBulkLoader.bulkLoadOn();
        AscnCncfSegment existingSegment = new AscnCncfSegment(
                cancerStudy.getInternalId(),
                sample.getInternalId(),
                "5",
                1000,
                2000,
                5.0,
                2.0,
                0.5,
                0.2);
        existingSegment.setId(DaoAscnCncf.getNextId());
        DaoAscnCncf.addAscnCncfSegment(existingSegment);
        ClickHouseBulkLoader.flushAll();

        File dataFolder = new File("src/test/resources/incremental/ascn_cncf/");
        File metaFile = new File(dataFolder, "meta_ascn_cncf.txt");
        File dataFile = new File(dataFolder, "data_ascn_cncf.txt");

        ImportAscnCncfData importer = new ImportAscnCncfData(new String[] {
                "--loadMode", "bulkLoad",
                "--meta", metaFile.getAbsolutePath(),
                "--data", dataFile.getAbsolutePath(),
                "--overwrite-existing",
        });
        importer.run();

        List<AscnCncfSegment> segments = DaoAscnCncf.getSegmentsForSample(sample.getInternalId(), cancerStudy.getInternalId());
        // the pre-existing segment on chr 5 should have been replaced by the 2 new segments from the incremental file
        assertEquals(2, segments.size());
        assertTrue(segments.stream().noneMatch(s -> "5".equals(s.getChr())));
    }
}
