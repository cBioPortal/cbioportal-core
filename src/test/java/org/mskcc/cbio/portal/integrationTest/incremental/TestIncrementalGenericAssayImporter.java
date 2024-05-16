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
import org.mskcc.cbio.portal.dao.DaoException;
import org.mskcc.cbio.portal.dao.DaoGeneticAlteration;
import org.mskcc.cbio.portal.dao.DaoGeneticEntity;
import org.mskcc.cbio.portal.dao.DaoGeneticProfile;
import org.mskcc.cbio.portal.model.GeneticProfile;
import org.mskcc.cbio.portal.scripts.ImportProfileData;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.annotation.Transactional;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mskcc.cbio.portal.integrationTest.incremental.GeneticAlterationsTestHelper.assertNoChange;
import static org.mskcc.cbio.portal.integrationTest.incremental.GeneticAlterationsTestHelper.assertPriorDataState;
import static org.mskcc.cbio.portal.integrationTest.incremental.GeneticAlterationsTestHelper.geneStableIdToEntityId;
import static org.mskcc.cbio.portal.integrationTest.incremental.GeneticAlterationsTestHelper.geneStableIdsToEntityIds;

/**
 * Tests Incremental Import of Generic Assay data
 *
 * @author Ruslan Forostianov
 * @author Pieter Lukasse
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:/applicationContext-dao.xml" })
@Rollback
@Transactional
public class TestIncrementalGenericAssayImporter {

    /**
     * Test incremental upload of GENERIC_ASSAY
     */
    @Test
    public void testGenericAssay() throws DaoException, IOException {
        /**
         * Prior checks
         */
        // Stable id that is part of the platform, but absent during the incremental upload
        final String absentStableId = "L-685458";
        final Set<String> noChangeStableIds = Set.of("Erlotinib", "Irinotecan", "Lapatinib");
        final Set<String> beforeStableIds = new HashSet<>(noChangeStableIds);
        beforeStableIds.add(absentStableId);

        // stable_id: TCGA-A1-A0SB-01
        final int newSampleId = 1;
        // stable_id: TCGA-A1-A0SD-01
        final int updateSampleId = 2;
        // stable_id: TCGA-A1-A0SE-01
        final int noChangeSampleId = 3;
        final Set<Integer> beforeSampleIds = Set.of(updateSampleId, noChangeSampleId);

        GeneticProfile ic50Profile = DaoGeneticProfile.getGeneticProfileByStableId("study_tcga_pub_treatment_ic50");
        assertNotNull(ic50Profile);

        HashMap<Integer, HashMap<Integer, String>> beforeResult = DaoGeneticAlteration.getInstance().getGeneticAlterationMapForEntityIds(ic50Profile.getGeneticProfileId(), null);
        Set<Integer> beforeEntityIds = geneStableIdsToEntityIds(beforeStableIds);
        assertPriorDataState(beforeResult, beforeEntityIds, beforeSampleIds);

        File dataFolder = new File("src/test/resources/incremental/generic_assay/");
        File metaFile = new File(dataFolder, "meta_treatment_ic50.txt");
        File dataFile = new File(dataFolder, "data_treatment_ic50.txt");

        /**
         * Test
         */
        new ImportProfileData(new String[] {
                "--loadMode", "bulkLoad",
                "--meta", metaFile.getAbsolutePath(),
                "--data", dataFile.getAbsolutePath(),
                "--overwrite-existing",
        }).run();

        /**
         * After test assertions
         */
        HashMap<Integer, HashMap<Integer, String>> afterResult = DaoGeneticAlteration.getInstance().getGeneticAlterationMapForEntityIds(ic50Profile.getGeneticProfileId(), null);
        assertEquals("After result should have +1 amount of entries", beforeResult.size() + 1, afterResult.size());
        afterResult.values()
                .forEach(sampleToValue ->
                        assertEquals("Each gene row has to get one extra sample",beforeSampleIds.size() + 1, sampleToValue.size()));
        assertNoChange(beforeResult, afterResult, geneStableIdsToEntityIds(noChangeStableIds), Set.of(noChangeSampleId));
        int erlotinibEntityId = geneStableIdToEntityId("Erlotinib");
        assertEquals(">8", afterResult.get(erlotinibEntityId).get(newSampleId));
        assertEquals("7.5", afterResult.get(erlotinibEntityId).get(updateSampleId));
        int irinotecanEntityId = geneStableIdToEntityId("Irinotecan");
        assertEquals("", afterResult.get(irinotecanEntityId).get(newSampleId));
        assertEquals("0.081", afterResult.get(irinotecanEntityId).get(updateSampleId));
        int absentEntityId = geneStableIdToEntityId(absentStableId);
        assertEquals("", afterResult.get(absentEntityId).get(newSampleId));
        assertEquals("", afterResult.get(absentEntityId).get(updateSampleId));
        int lapatinibEntityId = geneStableIdToEntityId("Lapatinib");
        assertEquals("6.2", afterResult.get(lapatinibEntityId).get(newSampleId));
        assertEquals("7.848", afterResult.get(lapatinibEntityId).get(updateSampleId));
        int lbw242EntityId = geneStableIdToEntityId("LBW242");
        assertEquals("0.1", afterResult.get(lbw242EntityId).get(newSampleId));
        assertEquals(">~8", afterResult.get(lbw242EntityId).get(updateSampleId));
        assertNotNull("New generic entity has to be added", DaoGeneticEntity.getGeneticEntityByStableId("LBW242"));
    }

    @Before
    public void setUp() throws DaoException {
        DaoCancerStudy.reCacheAll();
    }

}
