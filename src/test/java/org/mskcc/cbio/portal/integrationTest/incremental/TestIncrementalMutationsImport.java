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
import org.mskcc.cbio.portal.scripts.ImportProfileData;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.annotation.Transactional;

import java.io.File;
import java.util.ArrayList;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mskcc.cbio.portal.dao.DaoMutation.getMutations;

/**
 * Tests Incremental Import of Mutation Molecular Data.
 *
 * @author Ruslan Forostianov
 * @author Pieter Lukasse
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:/applicationContext-dao.xml" })
@Rollback
@Transactional
public class TestIncrementalMutationsImport {

    static final String INSERT_MUTATION_DATA_SAMPLE_ID = "TCGA-A1-A0SE-01";
    static final String UPDATE_MUTATION_DATA_SAMPLE_ID = "TCGA-A1-A0SH-01";
    static final String STUDY_ID = "study_tcga_pub";
    CancerStudy cancerStudy;
    static final File STUDY_FOLDER = new File("src/test/resources/incremental/study_tcga_pub");
    static final File META_FILE = new File(STUDY_FOLDER, "meta_mutations.txt");
    static final File DATA_FILE = new File(STUDY_FOLDER, "data_mutations_extended.txt");

    @Before
    public void setUp() throws DaoException {
        cancerStudy = DaoCancerStudy.getCancerStudyByStableId(STUDY_ID);
    }
    /**
     * Test inserting new mutation profile data for existing sample and genetic profile
     */
	@Test
    public void testInsertNewMutationProfileDataForExistingSampleAndProfile() throws DaoException {
        GeneticProfile mutationGeneticProfile = DaoGeneticProfile.getGeneticProfileByStableId("study_tcga_pub_mutations");
        assertNotNull(mutationGeneticProfile);
        /**
         * this sample does not have mutation data attached
         */
        Sample mutationDataSample = DaoSample.getSampleByCancerStudyAndSampleId(cancerStudy.getInternalId(), INSERT_MUTATION_DATA_SAMPLE_ID);

        ImportProfileData importProfileData = new ImportProfileData(new String[] {
                "--loadMode", "bulkLoad",
                "--meta", META_FILE.getAbsolutePath(),
                "--data", DATA_FILE.getAbsolutePath(),
                "--sample-ids-only", INSERT_MUTATION_DATA_SAMPLE_ID
        });
        importProfileData.run();

        ArrayList<ExtendedMutation> insertedMutations = getMutations(
                mutationGeneticProfile.getGeneticProfileId(),
                mutationDataSample.getInternalId());
        assertEquals(3, insertedMutations.size());
        assertNotNull(insertedMutations.get(0).getEvent());
        assertNotNull(insertedMutations.get(1).getEvent());
        assertNotNull(insertedMutations.get(2).getEvent());
    }
    /**
     * Test updating mutation profile data for existing sample. The mutation genetic profile exists.
     */
    @Test
    public void testUpdateMutationProfileDataForExistingSampleAndProfile() throws DaoException {
        GeneticProfile mutationGeneticProfile = DaoGeneticProfile.getGeneticProfileByStableId("study_tcga_pub_mutations");
        assertNotNull(mutationGeneticProfile);
        /**
         * this sample does have 2 mutation data rows attached. See seed_mini.sql
         */
        Sample mutationDataSample = DaoSample.getSampleByCancerStudyAndSampleId(cancerStudy.getInternalId(), UPDATE_MUTATION_DATA_SAMPLE_ID);

        ImportProfileData importProfileData = new ImportProfileData(new String[] {
                "--loadMode", "bulkLoad",
                "--meta", META_FILE.getAbsolutePath(),
                "--data", DATA_FILE.getAbsolutePath(),
                "--sample-ids-only", UPDATE_MUTATION_DATA_SAMPLE_ID
        });
        importProfileData.run();

        ArrayList<ExtendedMutation> insertedMutations = getMutations(
                mutationGeneticProfile.getGeneticProfileId(),
                mutationDataSample.getInternalId());
        assertEquals(3, insertedMutations.size());
        assertNotNull(insertedMutations.get(0).getEvent());
        assertNotNull(insertedMutations.get(1).getEvent());
        assertNotNull(insertedMutations.get(2).getEvent());
        Set<Long> entrezIds = insertedMutations.stream().map(m -> m.getEntrezGeneId()).collect(Collectors.toSet());
        Set<Long> expected = Set.of(207L, 208L, 672L);
        assertEquals(expected, entrezIds);
    }

    @Test
    public void testSampleIdSelection() throws DaoException {
        GeneticProfile mutationGeneticProfile = DaoGeneticProfile.getGeneticProfileByStableId("study_tcga_pub_mutations");
        assertNotNull(mutationGeneticProfile);
        Sample insertMutationSample = DaoSample.getSampleByCancerStudyAndSampleId(cancerStudy.getInternalId(), INSERT_MUTATION_DATA_SAMPLE_ID);

        ImportProfileData importProfileData = new ImportProfileData(new String[] {
                "--loadMode", "bulkLoad",
                "--meta", META_FILE.getAbsolutePath(),
                "--data", DATA_FILE.getAbsolutePath(),
                "--sample-ids-only", UPDATE_MUTATION_DATA_SAMPLE_ID
        });
        importProfileData.run();

        ArrayList<ExtendedMutation> insertedMutations = getMutations(
                mutationGeneticProfile.getGeneticProfileId(),
                insertMutationSample.getInternalId());
        assertTrue("Mutations datat for " + INSERT_MUTATION_DATA_SAMPLE_ID + " sample must not be inserted to the database as its sample is not selected",
                insertedMutations.isEmpty());
    }

}
