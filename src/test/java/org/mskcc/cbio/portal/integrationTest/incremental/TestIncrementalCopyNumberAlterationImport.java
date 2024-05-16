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

import org.cbioportal.model.CNA;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mskcc.cbio.portal.dao.DaoCnaEvent;
import org.mskcc.cbio.portal.dao.DaoException;
import org.mskcc.cbio.portal.dao.DaoGeneticAlteration;
import org.mskcc.cbio.portal.dao.DaoGeneticProfile;
import org.mskcc.cbio.portal.model.CnaEvent;
import org.mskcc.cbio.portal.model.GeneticProfile;
import org.mskcc.cbio.portal.scripts.ImportProfileData;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.annotation.Transactional;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mskcc.cbio.portal.integrationTest.incremental.GeneticAlterationsTestHelper.assertNoChange;
import static org.mskcc.cbio.portal.integrationTest.incremental.GeneticAlterationsTestHelper.assertPriorDataState;

/**
 * Tests Incremental Import of PROTEIN_LEVEL Data.
 *
 * @author Ruslan Forostianov
 * @author Pieter Lukasse
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:/applicationContext-dao.xml"})
@Rollback
@Transactional
public class TestIncrementalCopyNumberAlterationImport {

    /**
     * Test incremental upload of COPY_NUMBER_ALTERATION DISCRETE (gistic)
     */
    @Test
    public void testDiscreteCNA() throws DaoException, IOException {
        /**
         * Prior checks
         */
        // Hugo_Symbol: CDK1
        final long newGeneEntrezId = 983l;
        // Gene that is part of the platform, but absent during the incremental upload
        // Hugo_Symbol: ATM
        final long absentGeneEntrezId = 472l;
        final Set<Long> noChangeEntrezIds = Set.of(10000l, 207l, 208l,  3265l,  3845l,  4893l,  672l,  673l,  675l);
        final Set<Long> beforeEntrezIds = new HashSet<>(noChangeEntrezIds);
        beforeEntrezIds.add(absentGeneEntrezId);

        // stable_id: TCGA-XX-0800
        final int newSampleId = 15;
        // stable_id: TCGA-A1-A0SO
        final int updateSampleId = 12;
        final Set<Integer> noChangeSampleIds = Set.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 14);
        final Set<Integer> beforeSampleIds = new HashSet<>(noChangeSampleIds);
        beforeSampleIds.add(updateSampleId);

        final Set<Integer> afterSampleIds = new HashSet<>(beforeSampleIds);
        afterSampleIds.add(newSampleId);

        GeneticProfile discreteCNAProfile = DaoGeneticProfile.getGeneticProfileByStableId("study_tcga_pub_gistic");
        assertNotNull(discreteCNAProfile);
        HashMap<Long, HashMap<Integer, String>> beforeResult = DaoGeneticAlteration.getInstance().getGeneticAlterationMap(discreteCNAProfile.getGeneticProfileId(), null);
        assertPriorDataState(beforeResult, beforeEntrezIds, beforeSampleIds);

        List<Short> allCnaLevels = Arrays.stream(CNA.values()).map(CNA::getCode).toList();
        Set<Integer> beforeCnaEventsSampleIds = Set.of(4, 13, 14, updateSampleId);
        List<CnaEvent> beforeSampleCnaEvents = DaoCnaEvent.getCnaEvents(afterSampleIds.stream().toList(),
                null,
                discreteCNAProfile.getGeneticProfileId(),
                allCnaLevels);
        Map<Integer, List<CnaEvent>> beforeSampleIdToSampleCnaEvents = beforeSampleCnaEvents.stream().collect(Collectors.groupingBy(CnaEvent::getSampleId));
        assertEquals(beforeCnaEventsSampleIds, beforeSampleIdToSampleCnaEvents.keySet());

        File dataFolder = new File("src/test/resources/incremental/copy_number_alteration/");
        File metaFile = new File(dataFolder, "meta_cna_discrete.txt");
        File dataFile = new File(dataFolder, "data_cna_discrete.txt");

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
        HashMap<Long, HashMap<Integer, String>> afterResult = DaoGeneticAlteration.getInstance().getGeneticAlterationMap(discreteCNAProfile.getGeneticProfileId(), null);
        assertEquals("After result should get exactly one new gene", beforeEntrezIds.size() + 1,
                afterResult.size());
        afterResult.values()
                .forEach(sampleToValue ->
                        assertEquals("Each gene row has to get one extra sample",beforeSampleIds.size() + 1, sampleToValue.size()));
        assertNoChange(beforeResult, afterResult, noChangeEntrezIds, noChangeSampleIds);
        assertEquals("-2", afterResult.get(newGeneEntrezId).get(newSampleId));
        assertEquals("2", afterResult.get(newGeneEntrezId).get(updateSampleId));
        assertEquals("", afterResult.get(absentGeneEntrezId).get(newSampleId));
        assertEquals("", afterResult.get(absentGeneEntrezId).get(updateSampleId));

        List<CnaEvent> afterSampleCnaEvents = DaoCnaEvent.getCnaEvents(afterSampleIds.stream().toList(),
                afterResult.keySet(),
                discreteCNAProfile.getGeneticProfileId(),
                allCnaLevels);
        Map<Integer, List<CnaEvent>> afterSampleIdToSampleCnaEvents = afterSampleCnaEvents.stream().collect(Collectors.groupingBy(CnaEvent::getSampleId));
        assertEquals("There is only one new sample that has to gain cna events", beforeCnaEventsSampleIds.size() + 1, afterSampleIdToSampleCnaEvents.size());
        beforeCnaEventsSampleIds.forEach(sampleId -> {
            if (sampleId == updateSampleId) {
                return;
            }
            Set<CnaEvent.Event> beforeCnaEvents = beforeSampleIdToSampleCnaEvents.get(sampleId).stream().map(CnaEvent::getEvent).collect(Collectors.toSet());
            Set<CnaEvent.Event> afterCnaEvents = afterSampleIdToSampleCnaEvents.get(sampleId).stream().map(CnaEvent::getEvent).collect(Collectors.toSet());
            assertEquals("CNA events for sample_id=" + sampleId + " must not change.", beforeCnaEvents, afterCnaEvents);
        });
        Map<Long, CNA> newSampleEntrezGeneIdToCnaAlteration = afterSampleIdToSampleCnaEvents.get(newSampleId).stream()
                .map(CnaEvent::getEvent)
                .collect(Collectors.toMap(
                        event -> event.getGene().getEntrezGeneId(),
                        CnaEvent.Event::getAlteration));
        assertEquals(Map.of(
                        208l, CNA.HOMDEL,
                        3265l, CNA.AMP,
                        4893l, CNA.HOMDEL,
                        672l, CNA.AMP,
                        673l, CNA.AMP,
                        675l, CNA.HOMDEL,
                        newGeneEntrezId, CNA.HOMDEL
                ),
                newSampleEntrezGeneIdToCnaAlteration);
        Map<Long, CNA> updatedSampleEntrezGeneIdToCnaAlteration = afterSampleIdToSampleCnaEvents.get(updateSampleId).stream()
                .map(CnaEvent::getEvent)
                .collect(Collectors.toMap(
                        event -> event.getGene().getEntrezGeneId(),
                        CnaEvent.Event::getAlteration));
        assertEquals(Map.of(
                        10000l, CNA.HOMDEL,
                        207l, CNA.AMP,
                        3845l, CNA.AMP,
                        673l, CNA.HOMDEL,
                        newGeneEntrezId, CNA.AMP
                ),
                updatedSampleEntrezGeneIdToCnaAlteration);
    }

}
