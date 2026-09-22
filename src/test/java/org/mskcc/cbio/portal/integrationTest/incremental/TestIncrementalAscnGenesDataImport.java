package org.mskcc.cbio.portal.integrationTest.incremental;

import java.io.File;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mskcc.cbio.portal.dao.ClickHouseBulkLoader;
import org.mskcc.cbio.portal.dao.DaoCancerStudy;
import org.mskcc.cbio.portal.dao.DaoException;
import org.mskcc.cbio.portal.dao.DaoAscnGenes;
import org.mskcc.cbio.portal.dao.DaoSample;
import org.mskcc.cbio.portal.integrationTest.IntegrationTestBase;
import org.mskcc.cbio.portal.model.CancerStudy;
import org.mskcc.cbio.portal.model.AscnGeneLevelRecord;
import org.mskcc.cbio.portal.model.Sample;
import org.mskcc.cbio.portal.scripts.ImportAscnGeneLevelData;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests incremental (--overwrite-existing) import of ASCN gene-level data.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:/applicationContext-dao.xml" })
public class TestIncrementalAscnGenesDataImport extends IntegrationTestBase {

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
        AscnGeneLevelRecord existingRecord = new AscnGeneLevelRecord(
                cancerStudy.getInternalId(),
                sample.getInternalId(),
                "AKT1",
                207L,
                "14",
                1000,
                2000,
                5.0,
                2.0,
                0.5,
                0.2);
        existingRecord.setId(DaoAscnGenes.getNextId());
        DaoAscnGenes.addAscnGeneLevelRecord(existingRecord);
        ClickHouseBulkLoader.flushAll();

        File dataFolder = new File("src/test/resources/incremental/ascn_genes/");
        File metaFile = new File(dataFolder, "meta_ascn_genes.txt");
        File dataFile = new File(dataFolder, "data_ascn_genes.txt");

        ImportAscnGeneLevelData importer = new ImportAscnGeneLevelData(new String[] {
                "--loadMode", "bulkLoad",
                "--meta", metaFile.getAbsolutePath(),
                "--data", dataFile.getAbsolutePath(),
                "--overwrite-existing",
        });
        importer.run();

        List<AscnGeneLevelRecord> records = DaoAscnGenes.getGeneLevelDataForSample(sample.getInternalId(), cancerStudy.getInternalId());
        // the pre-existing AKT1 record should have been replaced by the 2 new records (TP53, BRCA1) from the incremental file
        assertEquals(2, records.size());
        assertTrue(records.stream().noneMatch(r -> "AKT1".equals(r.getHugoGeneSymbol())));
    }
}
