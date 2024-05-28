/*
 * This file is part of cBioPortal.
 *
 * cBioPortal is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

package org.mskcc.cbio.portal.integrationTest.incremental;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mskcc.cbio.portal.dao.DaoCancerStudy;
import org.mskcc.cbio.portal.dao.DaoClinicalData;
import org.mskcc.cbio.portal.dao.DaoCopyNumberSegment;
import org.mskcc.cbio.portal.dao.DaoCopyNumberSegmentFile;
import org.mskcc.cbio.portal.dao.DaoException;
import org.mskcc.cbio.portal.dao.DaoSample;
import org.mskcc.cbio.portal.dao.MySQLbulkLoader;
import org.mskcc.cbio.portal.model.CancerStudy;
import org.mskcc.cbio.portal.model.ClinicalData;
import org.mskcc.cbio.portal.model.CopyNumberSegment;
import org.mskcc.cbio.portal.model.CopyNumberSegmentFile;
import org.mskcc.cbio.portal.model.Sample;
import org.mskcc.cbio.portal.scripts.ImportCopyNumberSegmentData;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.annotation.Transactional;

import java.io.File;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Tests Incremental Import of CNA segmented data.
 *
 * @author Ruslan Forostianov
 * @author Pieter Lukasse
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:/applicationContext-dao.xml" })
@Rollback
@Transactional
public class TestIncrementalCopyNumberSegmentDataImport {

    /**
     * Test incremental upload of CNA SEG data
     */
	@Test
    public void testIncrementalUpload() throws DaoException {
        String segSampleId = "TCGA-A1-A0SE-01";
        Sample segDataSample = DaoSample.getSampleByCancerStudyAndSampleId(cancerStudy.getInternalId(), segSampleId);

        CopyNumberSegmentFile copyNumberSegmentFile = new CopyNumberSegmentFile();
        copyNumberSegmentFile.cancerStudyId = cancerStudy.getInternalId();
        copyNumberSegmentFile.referenceGenomeId = CopyNumberSegmentFile.ReferenceGenomeId.hg19;
        copyNumberSegmentFile.segFileId = 1;
        copyNumberSegmentFile.filename = "test_file.seg";
        copyNumberSegmentFile.description = "test seg file description";
        DaoCopyNumberSegmentFile.addCopyNumberSegmentFile(copyNumberSegmentFile);
        DaoClinicalData.addSampleDatum(segDataSample.getInternalId(), "FRACTION_GENOME_ALTERED", "TEST");
        MySQLbulkLoader.bulkLoadOn();
        CopyNumberSegment copyNumberSegment = new CopyNumberSegment(
                cancerStudy.getInternalId(),
                segDataSample.getInternalId(),
                "1",
                3218610,
                95674710,
                100,
                0.01);
        copyNumberSegment.setSegId(1L);
        DaoCopyNumberSegment.addCopyNumberSegment(copyNumberSegment);
        MySQLbulkLoader.flushAll();

        File dataFolder = new File("src/test/resources/incremental/copy_number_alteration/");
        File metaFile = new File(dataFolder, "meta_cna_seg.txt");
        File dataFile = new File(dataFolder, "data_cna.seg");

        ImportCopyNumberSegmentData importCnaSegData = new ImportCopyNumberSegmentData(new String[] {
                "--loadMode", "bulkLoad",
                "--meta", metaFile.getAbsolutePath(),
                "--data", dataFile.getAbsolutePath(),
                "--overwrite-existing",
        });
        importCnaSegData.run();

        CopyNumberSegmentFile fetchedCopyNumberSegmentFile = DaoCopyNumberSegmentFile.getCopyNumberSegmentFile(cancerStudy.getInternalId());
        assertNotNull(fetchedCopyNumberSegmentFile);
        assertEquals("test_file.seg", fetchedCopyNumberSegmentFile.filename);
        List<CopyNumberSegment> cnaSegments = DaoCopyNumberSegment
                .getSegmentForASample(segDataSample.getInternalId(), cancerStudy.getInternalId());
        assertEquals(9, cnaSegments.size());
        List<ClinicalData> clinicalData = DaoClinicalData.getSampleData(cancerStudy.getInternalId(), Set.of(segSampleId));
        ClinicalData fractionGenomeAltered = clinicalData.stream()
                .filter(cd -> "FRACTION_GENOME_ALTERED".equals(cd.getAttrId())).findFirst().get();
        assertEquals("0.0000", fractionGenomeAltered.getAttrVal());
    }

    public static final String STUDY_ID = "study_tcga_pub";
    private CancerStudy cancerStudy;

    @Before
    public void setUp() throws DaoException {
        cancerStudy = DaoCancerStudy.getCancerStudyByStableId(STUDY_ID);
    }

}
