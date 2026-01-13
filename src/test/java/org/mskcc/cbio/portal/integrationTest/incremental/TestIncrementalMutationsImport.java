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

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.Test;
import org.mskcc.cbio.portal.dao.DaoCancerStudy;
import org.mskcc.cbio.portal.dao.DaoException;
import org.mskcc.cbio.portal.dao.DaoGenePanel;
import org.mskcc.cbio.portal.dao.DaoGeneticProfile;
import org.mskcc.cbio.portal.dao.DaoSample;
import org.mskcc.cbio.portal.dao.DaoSampleProfile;
import org.mskcc.cbio.portal.model.CancerStudy;
import org.mskcc.cbio.portal.model.ExtendedMutation;
import org.mskcc.cbio.portal.model.GenePanel;
import org.mskcc.cbio.portal.model.GeneticProfile;
import org.mskcc.cbio.portal.model.Sample;
import org.mskcc.cbio.portal.scripts.ImportProfileData;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.annotation.Transactional;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
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

    public static final String STUDY_ID = "study_tcga_pub";
    private CancerStudy cancerStudy;

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
        String mutationDataSampleId = "TCGA-A1-A0SE-01";
        /**
         * this sample does not have mutation data attached
         */
        Sample mutationDataSample = DaoSample.getSampleByCancerStudyAndSampleId(cancerStudy.getInternalId(), mutationDataSampleId);

        File singleTcgaSampleFolder = new File("src/test/resources/incremental/insert_mutation_data/");
        File metaFile = new File(singleTcgaSampleFolder, "meta_mutations.txt");
        File dataFile = new File(singleTcgaSampleFolder, "data_mutations_extended.txt");

        ImportProfileData importProfileData = new ImportProfileData(new String[] {
                "--loadMode", "bulkLoad",
                "--meta", metaFile.getAbsolutePath(),
                "--data", dataFile.getAbsolutePath(),
                "--overwrite-existing",
        });
        importProfileData.run();

        ArrayList<ExtendedMutation> insertedMutations = getMutations(
                mutationGeneticProfile.getGeneticProfileId(),
                mutationDataSample.getInternalId());
        assertEquals(3, insertedMutations.size());
        assertNotNull(insertedMutations.get(0).getEvent());
        assertNotNull(insertedMutations.get(1).getEvent());
        assertNotNull(insertedMutations.get(2).getEvent());
        GenePanel genePanel = DaoGenePanel.getGenePanelByStableId("TSTGNPNLMUTEXT");
        assertEquals("Sample profile has to point to TSTGNPNLMUTEXT panel",
                genePanel.getInternalId(),
                DaoSampleProfile.getPanelId(mutationDataSample.getInternalId(), mutationGeneticProfile.getGeneticProfileId()));
    }
    /**
     * Test updating mutation profile data for existing sample. The mutation genetic profile exists.
     */
    @Test
    public void testUpdateMutationProfileDataForExistingSampleAndProfile() throws DaoException {
        GeneticProfile mutationGeneticProfile = DaoGeneticProfile.getGeneticProfileByStableId("study_tcga_pub_mutations");
        assertNotNull(mutationGeneticProfile);
        String mutationDataSampleId = "TCGA-A1-A0SH-01";
        /**
         * this sample does have 2 mutation data rows attached. See seed_mini.sql
         */
        Sample mutationDataSample = DaoSample.getSampleByCancerStudyAndSampleId(cancerStudy.getInternalId(), mutationDataSampleId);

        File singleTcgaSampleFolder = new File("src/test/resources/incremental/update_mutation_data/");
        File metaFile = new File(singleTcgaSampleFolder, "meta_mutations.txt");
        File dataFile = new File(singleTcgaSampleFolder, "data_mutations_extended.txt");

        ImportProfileData importProfileData = new ImportProfileData(new String[] {
                "--loadMode", "bulkLoad",
                "--meta", metaFile.getAbsolutePath(),
                "--data", dataFile.getAbsolutePath(),
                "--overwrite-existing",
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
        GenePanel genePanel = DaoGenePanel.getGenePanelByStableId("TSTGNPNLMUTEXT");
        assertEquals("Sample profile has to point to TSTGNPNLMUTEXT panel",
                genePanel.getInternalId(),
                DaoSampleProfile.getPanelId(mutationDataSample.getInternalId(), mutationGeneticProfile.getGeneticProfileId()));
    }

}
