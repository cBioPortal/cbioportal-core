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
import org.mskcc.cbio.portal.dao.*;
import org.mskcc.cbio.portal.model.*;
import org.mskcc.cbio.portal.scripts.UpdateCaseListsSampleIds;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.annotation.Transactional;

import java.io.File;

import static org.junit.Assert.*;

/**
 * Tests Incremental Import of Case Lists.
 *
 * @author Ruslan Forostianov
 * @author Pieter Lukasse
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:/applicationContext-dao.xml" })
@Rollback
@Transactional
public class TestUpdateCaseListsSampleIds {

    DaoSampleList daoSampleList = new DaoSampleList();
    /**
     * Test adding sample id to the all case list. It is the default behaviour of the command.
     */
    @Test
    public void testAddSampleIdToAllCaseList() throws DaoException {
        String sampleIdToAdd = "TCGA-XX-0800-01";
        File singleTcgaSampleFolder = new File("src/test/resources/update_case_lists/add_sample_to_case_list/");
        File metaFile = new File(singleTcgaSampleFolder, "meta_clinical_sample.txt");

        assertSampleIdNotInCaseLists(sampleIdToAdd, "study_tcga_pub_all");

        UpdateCaseListsSampleIds importClinicalData = new UpdateCaseListsSampleIds(new String[] {
                "--meta", metaFile.getAbsolutePath(),
        });
        importClinicalData.run();

        assertSampleIdInCaseLists(sampleIdToAdd, "study_tcga_pub_all");
    }

    /**
     * Test adding sample id to a MRNA case list.
     * Sample has to be added to the all case list as well.
     */
	@Test
    public void testAddSampleIdToMrnaCaseList() throws DaoException {
        String sampleIdToAdd = "TCGA-XX-0800-01";
        File singleTcgaSampleFolder = new File("src/test/resources/update_case_lists/add_sample_to_case_list/");
        File metaFile = new File(singleTcgaSampleFolder, "meta_clinical_sample.txt");
        File caseListsDir = new File(singleTcgaSampleFolder, "case_lists/");

        assertSampleIdNotInCaseLists(sampleIdToAdd, "study_tcga_pub_all", "study_tcga_pub_mrna");

        UpdateCaseListsSampleIds importClinicalData = new UpdateCaseListsSampleIds(new String[] {
                "--meta", metaFile.getAbsolutePath(),
                "--case-lists", caseListsDir.getAbsolutePath()
        });
        importClinicalData.run();

        assertSampleIdInCaseLists(sampleIdToAdd, "study_tcga_pub_all", "study_tcga_pub_mrna");
	}

    /**
     * Test re-adding sample to very same case list (efficiently no-op) should not complain.
     */
    @Test
    public void testReAddingSampleToTheSameListShouldWork() throws DaoException {
        String sampleIdToAdd = "TCGA-A1-A0SH-01";
        String[] caseListsSampleIsPartOf = new String[] {
                "study_tcga_pub_all",
                "study_tcga_pub_acgh",
                "study_tcga_pub_cnaseq",
                "study_tcga_pub_complete",
                "study_tcga_pub_log2CNA",
                "study_tcga_pub_mrna",
                "study_tcga_pub_sequenced"};
        String[] caseListsSampleIsNotPartOf = new String[] {
                "study_tcga_pub_methylation_hm27",
        };

        File singleTcgaSampleFolder = new File("src/test/resources/update_case_lists/update_tcga_samples/");
        File metaFile = new File(singleTcgaSampleFolder, "meta_clinical_sample.txt");
        File caseListsDir = new File(singleTcgaSampleFolder, "case_lists/");

        assertSampleIdInCaseLists(sampleIdToAdd, caseListsSampleIsPartOf);
        assertSampleIdNotInCaseLists(sampleIdToAdd, caseListsSampleIsNotPartOf);

        UpdateCaseListsSampleIds importClinicalData = new UpdateCaseListsSampleIds(new String[] {
                "--meta", metaFile.getAbsolutePath(),
                "--case-lists", caseListsDir.getAbsolutePath()
        });
        importClinicalData.run();

        assertSampleIdInCaseLists(sampleIdToAdd, caseListsSampleIsPartOf);
        assertSampleIdNotInCaseLists(sampleIdToAdd, caseListsSampleIsNotPartOf);
    }

    /**
     * Test removing sample ids from not specified case lists
     */
    @Test
    public void testRemovingSampleIdsFromNotSpecifiedCaseLists() throws DaoException {
        String sampleIdToAdd = "TCGA-A1-A0SH-01";

        File singleTcgaSampleFolder = new File("src/test/resources/update_case_lists/update_tcga_samples/");
        File metaFile = new File(singleTcgaSampleFolder, "meta_clinical_sample.txt");
        File caseListsDir = new File(singleTcgaSampleFolder, "case_lists/");
        File caseAcghFile = new File(caseListsDir, "case_acgh.txt");

        UpdateCaseListsSampleIds importClinicalData = new UpdateCaseListsSampleIds(new String[] {
                "--meta", metaFile.getAbsolutePath(),
                "--case-lists", caseAcghFile.getAbsolutePath()
        });
        importClinicalData.run();

        assertSampleIdInCaseLists(sampleIdToAdd, "study_tcga_pub_all", "study_tcga_pub_acgh");
        assertSampleIdNotInCaseLists(sampleIdToAdd, "study_tcga_pub_cnaseq",
                "study_tcga_pub_complete",
                "study_tcga_pub_log2CNA",
                "study_tcga_pub_methylation_hm27",
                "study_tcga_pub_mrna",
                "study_tcga_pub_sequenced");
    }
    @Before
    public void init() {
        // FIXME How we can remove this re-caching and keep tests to work?
        // pre conditions (asserts before the testee operation is called) are relying on it
        DaoCancerStudy.reCacheAll();
    }

    private void assertSampleIdInCaseLists(String sampleId, String... caseListStableIds) throws DaoException {
        for (String caseListStableId : caseListStableIds) {
            SampleList sampleList = daoSampleList.getSampleListByStableId(caseListStableId);
            assertNotNull(caseListStableId + " case list has to exist", sampleList);
            assertTrue(sampleId + " has to be in the " + caseListStableId + " case list", sampleList.getSampleList().contains(sampleId));
        };
    }

    private void assertSampleIdNotInCaseLists(String sampleId, String... caseListStableIds) throws DaoException {
        for (String caseListStableId : caseListStableIds) {
            SampleList sampleList = daoSampleList.getSampleListByStableId(caseListStableId);
            assertNotNull(caseListStableId + " case list has to exist", sampleList);
            assertTrue(sampleId + " has not to be in the " + caseListStableId + " case list", !sampleList.getSampleList().contains(sampleId));
        };
    }
}
