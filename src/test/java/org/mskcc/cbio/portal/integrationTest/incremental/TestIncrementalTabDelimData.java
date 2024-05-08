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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mskcc.cbio.portal.dao.DaoCancerStudy;
import org.mskcc.cbio.portal.dao.DaoCnaEvent;
import org.mskcc.cbio.portal.dao.DaoException;
import org.mskcc.cbio.portal.dao.DaoGeneOptimized;
import org.mskcc.cbio.portal.dao.DaoGeneticAlteration;
import org.mskcc.cbio.portal.dao.DaoGeneticProfile;
import org.mskcc.cbio.portal.model.CnaEvent;
import org.mskcc.cbio.portal.model.GeneticAlterationType;
import org.mskcc.cbio.portal.model.GeneticProfile;
import org.mskcc.cbio.portal.scripts.ImportTabDelimData;
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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Tests Incremental Import of Tab Delimited Data.
 *
 * @author Ruslan Forostianov
 * @author Pieter Lukasse
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:/applicationContext-dao.xml" })
@Rollback
@Transactional
public class TestIncrementalTabDelimData {

    @Before
    public void setUp() throws DaoException {
        DaoCancerStudy.reCacheAll();
    }

    /**
     * Test incremental upload of MRNA_EXPRESSION
     */
	@Test
    public void testMrnaExpression() throws DaoException, IOException {
        /**
         * Prior checks
         */
        // Hugo_Symbol: CDK1
        final long newGeneEntrezId = 983l;
        // Gene that is part of the platform, but absent during the incremental upload
        // Hugo_Symbol: ARAF
        final long absentGeneEntrezId = 369l;
        final Set<Long> noChangeEntrezIds = Set.of(10000l, 207l, 208l,  3265l, 3845l,  472l,  4893l,  672l,  673l,  675l);
        final Set<Long> beforeEntrezIds = new HashSet<>(noChangeEntrezIds);
        beforeEntrezIds.add(absentGeneEntrezId);

        // stable_id: TCGA-A1-A0SB-01
        final int newSampleId = 1;
        // stable_id: TCGA-A1-A0SD-01
        final int updateSampleId = 2;
        final Set<Integer> noChangeSampleIds = Set.of(3, 6, 8, 9, 10, 12, 13);
        final Set<Integer> beforeSampleIds = new HashSet<>(noChangeSampleIds);
        beforeSampleIds.add(updateSampleId);

        GeneticProfile mrnaProfile = DaoGeneticProfile.getGeneticProfileByStableId("study_tcga_pub_mrna");
        assertNotNull(mrnaProfile);

        HashMap<Long, HashMap<Integer, String>> beforeResult = DaoGeneticAlteration.getInstance().getGeneticAlterationMap(mrnaProfile.getGeneticProfileId(), null);
        assertPriorDataState(beforeResult, beforeEntrezIds, beforeSampleIds);

        File dataFolder = new File("src/test/resources/incremental/tab_delim_data/");
        File dataFile = new File(dataFolder, "data_expression_Zscores.txt");

        /**
         * Test
         */
        new ImportTabDelimData(dataFile,
                mrnaProfile.getGeneticProfileId(),
                null,
                true,
                DaoGeneticAlteration.getInstance(),
                DaoGeneOptimized.getInstance()).importData();

        /**
         * After test assertions
         */
        HashMap<Long, HashMap<Integer, String>> afterResult = DaoGeneticAlteration.getInstance().getGeneticAlterationMap(mrnaProfile.getGeneticProfileId(), null);
        assertEquals("After result should get exactly one new gene", beforeEntrezIds.size() + 1,
                afterResult.size());
        afterResult.values()
                .forEach(sampleToValue ->
                assertEquals("Each gene row has to get one extra sample",beforeSampleIds.size() + 1, sampleToValue.size()));
        assertNoChange(beforeResult, afterResult, noChangeEntrezIds, noChangeSampleIds);
        HashMap<Integer, String> newGeneRow = afterResult.get(newGeneEntrezId);
        assertEquals("-0.1735", newGeneRow.get(newSampleId));
        assertEquals("-0.6412", newGeneRow.get(updateSampleId));
        HashMap<Integer, String> absentGeneRow = afterResult.get(absentGeneEntrezId);
        assertEquals("", absentGeneRow.get(newSampleId));
        assertEquals("", absentGeneRow.get(updateSampleId));
    }

    /**
     * Test incremental upload of PROTEIN_LEVEL
     */
    @Test
    public void testRppa() throws DaoException, IOException {
        /**
         * Prior checks
         */
        // Hugo_Symbol: CDK1
        final long newGeneEntrezId = 983l;
        // Gene that is part of the platform, but absent during the incremental upload
        // Hugo_Symbol: ARAF
        final long absentGeneEntrezId = 369l;
        final Set<Long> noChangeEntrezIds = Set.of(10000l, 207l, 208l,  3265l, 3845l,  472l,  4893l,  672l,  673l,  675l);
        final Set<Long> beforeEntrezIds = new HashSet<>(noChangeEntrezIds);
        beforeEntrezIds.add(absentGeneEntrezId);

        // stable_id: TCGA-A1-A0SB-01
        final int newSampleId = 1;
        // stable_id: TCGA-A1-A0SD-01
        final int updateSampleId = 2;
        final Set<Integer> noChangeSampleIds = Set.of(3, 6, 8, 9, 10, 12, 13);
        final Set<Integer> beforeSampleIds = new HashSet<>(noChangeSampleIds);
        beforeSampleIds.add(updateSampleId);

        GeneticProfile rppaProfile = DaoGeneticProfile.getGeneticProfileByStableId("study_tcga_pub_rppa");
        assertNotNull(rppaProfile);

        HashMap<Long, HashMap<Integer, String>> beforeResult = DaoGeneticAlteration.getInstance().getGeneticAlterationMap(rppaProfile.getGeneticProfileId(), null);
        assertPriorDataState(beforeResult, beforeEntrezIds, beforeSampleIds);

        File dataFolder = new File("src/test/resources/incremental/tab_delim_data/");
        File dataFile = new File(dataFolder, "data_rppa.txt");

        /**
         * Test
         */
        new ImportTabDelimData(dataFile,
                rppaProfile.getGeneticProfileId(),
                null,
                true,
                DaoGeneticAlteration.getInstance(),
                DaoGeneOptimized.getInstance()).importData();

        /**
         * After test assertions
         */
        HashMap<Long, HashMap<Integer, String>> afterResult = DaoGeneticAlteration.getInstance().getGeneticAlterationMap(rppaProfile.getGeneticProfileId(), null);
        assertEquals("After result should get exactly one new gene", beforeEntrezIds.size() + 1,
                afterResult.size());
        afterResult.values()
                .forEach(sampleToValue ->
                        assertEquals("Each gene row has to get one extra sample",beforeSampleIds.size() + 1, sampleToValue.size()));
        assertNoChange(beforeResult, afterResult, noChangeEntrezIds, noChangeSampleIds);
        assertEquals("-0.141047088398489", afterResult.get(newGeneEntrezId).get(newSampleId));
        assertEquals("1.61253243564957", afterResult.get(newGeneEntrezId).get(updateSampleId));
        assertEquals("", afterResult.get(absentGeneEntrezId).get(newSampleId));
        assertEquals("", afterResult.get(absentGeneEntrezId).get(updateSampleId));
    }

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

        File dataFolder = new File("src/test/resources/incremental/tab_delim_data/");
        File dataFile = new File(dataFolder, "data_cna_discrete.txt");
        File pdAnnotations = new File(dataFolder, "data_cna_pd_annotations.txt");

        /**
         * Test
         */
        ImportTabDelimData importer = new ImportTabDelimData(dataFile,
                discreteCNAProfile.getGeneticProfileId(),
                null,
                true,
                DaoGeneticAlteration.getInstance(),
                DaoGeneOptimized.getInstance());
        importer.setPdAnnotationsFile(pdAnnotations);
        importer.importData();

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

    @Test
    public void testGsvaIsNotSupported() throws DaoException, IOException {
        GeneticProfile gsvaProfile = new GeneticProfile();
        gsvaProfile.setCancerStudyId(DaoCancerStudy.getCancerStudyByStableId("study_tcga_pub").getInternalId());
        gsvaProfile.setStableId("gsva_scores");
        gsvaProfile.setDatatype("GENESET_SCORE");
        gsvaProfile.setGeneticAlterationType(GeneticAlterationType.GENESET_SCORE);
        gsvaProfile.setProfileName("gsva test platform");
        DaoGeneticProfile.addGeneticProfile(gsvaProfile);

        assertThrows(UnsupportedOperationException.class, () ->
                new ImportTabDelimData(File.createTempFile("gsva", "test"),
                        DaoGeneticProfile.getGeneticProfileByStableId("gsva_scores").getGeneticProfileId(),
                        null,
                        true,
                        DaoGeneticAlteration.getInstance(),
                        DaoGeneOptimized.getInstance()));
    }

    private void assertPriorDataState(HashMap<Long, HashMap<Integer, String>> beforeResult, Set<Long> expectedGeneEntrezIds, Set<Integer> expectedSampleIds) {
        assertEquals(expectedGeneEntrezIds, beforeResult.keySet());
        beforeResult.forEach((entrezGeneId, sampleIdToValue) -> {
            assertEquals("Samples for gene with entrez_id = " + entrezGeneId + " have to match expected ones",
                    expectedSampleIds, beforeResult.get(entrezGeneId).keySet());
        });
    }

    private void assertNoChange(HashMap<Long, HashMap<Integer, String>> beforeResult,
                                HashMap<Long, HashMap<Integer, String>> afterResult,
                                Set<Long> geneEntrezIds,
                                Set<Integer> sampleIds) {
        geneEntrezIds.forEach(entrezGeneId -> {
            assertTrue("After result is expected to contain entrez_id=" + entrezGeneId,
                    afterResult.containsKey(entrezGeneId));
            sampleIds.forEach(sampleId -> {
                assertTrue("Sample_id=" + sampleId + " expected to be found for gene with entrez_id=" + entrezGeneId,
                        afterResult.get(entrezGeneId).containsKey(sampleId));
                assertEquals("The values for sample_id=" + sampleId +
                                " and entrez_id=" + entrezGeneId + " before and after upload have to match.",
                        beforeResult.get(entrezGeneId).get(sampleId), afterResult.get(entrezGeneId).get(sampleId));
            });
        });
    }

}
