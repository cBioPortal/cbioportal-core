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

import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mskcc.cbio.portal.dao.DaoCancerStudy;
import org.mskcc.cbio.portal.dao.DaoException;
import org.mskcc.cbio.portal.dao.DaoGeneOptimized;
import org.mskcc.cbio.portal.dao.DaoGeneticAlteration;
import org.mskcc.cbio.portal.dao.DaoGeneticProfile;
import org.mskcc.cbio.portal.model.GeneticProfile;
import org.mskcc.cbio.portal.scripts.ImportTabDelimData;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.annotation.Transactional;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mskcc.cbio.portal.dao.DaoMutation.getMutations;

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

    // Hugo_Symbol: CDK1
    static final long NEW_GENE_ENTREZ_ID = 983l;

    /**
     * Gene that is part of the platform, but absent during the incremental upload
     */
    // Hugo_Symbol: ARAF
    static final long ABSENT_GENE_ENTREZ_ID = 369l;
    static final Set<Long> TEST_ENTREZ_GENE_IDS = Set.of(10000l, 207l, 208l,  3265l, ABSENT_GENE_ENTREZ_ID,  3845l,  472l,  4893l,  672l,  673l,  675l, NEW_GENE_ENTREZ_ID);

    // stable_id: TCGA-A1-A0SB-01
    static final int NEW_SAMPLE_ID = 1;

    // stable_id: TCGA-A1-A0SD-01
    static final int UPDATED_SAMPLE_ID = 2;
    static final Set<Integer> TEST_SAMPLE_IDS = Set.of(NEW_SAMPLE_ID, UPDATED_SAMPLE_ID, 3, 6, 8, 9, 10, 12, 13);

    /**
     * Test incremental upload of MRNA_EXPRESSION
     */
	@Test
    public void testMrnaExpression() throws DaoException, IOException {
        /**
         * Prior checks
         */
        GeneticProfile mrnaProfile = DaoGeneticProfile.getGeneticProfileByStableId("study_tcga_pub_mrna");
        assertNotNull(mrnaProfile);
        HashMap<Long, HashMap<Integer, String>> beforeResult = DaoGeneticAlteration.getInstance().getGeneticAlterationMap(mrnaProfile.getGeneticProfileId(), null);
        assertPriorDataState(beforeResult);

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
        assertNoChange(beforeResult, afterResult);
        assertEquals("-0.1735", afterResult.get(NEW_GENE_ENTREZ_ID).get(NEW_SAMPLE_ID));
        assertEquals("-0.6412", afterResult.get(NEW_GENE_ENTREZ_ID).get(UPDATED_SAMPLE_ID));
        assertEquals("", afterResult.get(ABSENT_GENE_ENTREZ_ID).get(NEW_SAMPLE_ID));
        assertEquals("-1.12475", afterResult.get(ABSENT_GENE_ENTREZ_ID).get(UPDATED_SAMPLE_ID));
    }

    /**
     * Test incremental upload of PROTEIN_LEVEL
     */
    @Test
    public void testRppa() throws DaoException, IOException {
        /**
         * Prior checks
         */
        GeneticProfile rppaProfile = DaoGeneticProfile.getGeneticProfileByStableId("study_tcga_pub_rppa");
        assertNotNull(rppaProfile);
        HashMap<Long, HashMap<Integer, String>> beforeResult = DaoGeneticAlteration.getInstance().getGeneticAlterationMap(rppaProfile.getGeneticProfileId(), null);
        assertPriorDataState(beforeResult);

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
        assertNoChange(beforeResult, afterResult);
        assertEquals("-0.141047088398489", afterResult.get(NEW_GENE_ENTREZ_ID).get(NEW_SAMPLE_ID));
        assertEquals("1.61253243564957", afterResult.get(NEW_GENE_ENTREZ_ID).get(UPDATED_SAMPLE_ID));
        assertEquals("", afterResult.get(ABSENT_GENE_ENTREZ_ID).get(NEW_SAMPLE_ID));
        assertEquals("-1.129", afterResult.get(ABSENT_GENE_ENTREZ_ID).get(UPDATED_SAMPLE_ID));
    }

    private void assertPriorDataState(HashMap<Long, HashMap<Integer, String>> beforeResult) {
        assertEquals("All but new entrez gene id expected to be in the database for this profile before the upload", TEST_ENTREZ_GENE_IDS.size() - 1, beforeResult.size());
        assertFalse("No new entrez gene id expected to be in the database for this profile before the upload", beforeResult.containsKey(NEW_GENE_ENTREZ_ID));
        beforeResult.forEach((entrezGeneId, sampleIdToValue) -> {
            assertEquals("All but new sample id expected to be in the database for this profile for gene with entrez id " + entrezGeneId + " before the upload", TEST_SAMPLE_IDS.size() - 1, beforeResult.get(entrezGeneId).size());
            assertFalse("No new entrez gene id expected to be in the database for this profile for gene with entrez id " + entrezGeneId + " before the upload", beforeResult.get(entrezGeneId).containsKey(NEW_SAMPLE_ID));
        });
    }

    private void assertNoChange(HashMap<Long, HashMap<Integer, String>> beforeResult, HashMap<Long, HashMap<Integer, String>> afterResult) {
        assertEquals("These genes expected to be found after upload", TEST_ENTREZ_GENE_IDS, afterResult.keySet());
        afterResult.forEach((entrezGeneId, sampleIdToValue) -> {
            assertEquals("These sample ids expected to be found for gene with entrez id " + entrezGeneId+ " after upload", TEST_SAMPLE_IDS, afterResult.get(entrezGeneId).keySet());
            if (entrezGeneId == NEW_GENE_ENTREZ_ID || entrezGeneId == ABSENT_GENE_ENTREZ_ID) {
                return;
            }
            sampleIdToValue.forEach((sampleId, value) -> {
                if (sampleId == NEW_SAMPLE_ID || sampleId == UPDATED_SAMPLE_ID) {
                    return;
                }
                assertEquals("The associated value is not expected change associated sample id " + sampleId + " and entrez gene id " + entrezGeneId,
                        beforeResult.get(entrezGeneId).get(sampleId), afterResult.get(entrezGeneId).get(sampleId));
            });
        });
    }

}
