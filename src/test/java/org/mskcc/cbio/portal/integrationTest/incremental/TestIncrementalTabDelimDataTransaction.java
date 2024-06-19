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
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.runner.RunWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mskcc.cbio.portal.dao.DaoCancerStudy;
import org.mskcc.cbio.portal.dao.DaoException;
import org.mskcc.cbio.portal.dao.DaoGeneOptimized;
import org.mskcc.cbio.portal.dao.DaoGeneticAlteration;
import org.mskcc.cbio.portal.dao.DaoGeneticProfile;
import org.mskcc.cbio.portal.model.CanonicalGene;
import org.mskcc.cbio.portal.model.GeneticProfile;
import org.mskcc.cbio.portal.scripts.ImportTabDelimData;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.io.File;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests Transaction for Incremental Import of Tab Delimited Data.
 *
 * @author Ruslan Forostianov
 * @author Pieter Lukasse
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:/applicationContext-dao.xml" })
public class TestIncrementalTabDelimDataTransaction {

    /**
     * Test transaction
     */
    @Test
    @ExtendWith(MockitoExtension.class)
    //Mysql does not support nested transactions. That's why we disable the outer transaction.
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    public void testTransaction() throws Exception {
        GeneticProfile mrnaProfile = DaoGeneticProfile.getGeneticProfileByStableId("study_tcga_pub_mrna");

        File dataFolder = new File("src/test/resources/incremental/mrna_expression/");
        File dataFile = new File(dataFolder, "data_expression_Zscores.txt");

        HashMap<Long, HashMap<Integer, String>> beforeResult = DaoGeneticAlteration.getInstance().getGeneticAlterationMap(mrnaProfile.getGeneticProfileId(), null);

        DaoGeneOptimized mockedDao = mock(DaoGeneOptimized.class);

        when(mockedDao.getGene(anyLong()))
                .thenThrow(new RuntimeException("Simulated error"));
        /**
         * Test
         */
        try {
            new ImportTabDelimData(dataFile,
                    mrnaProfile.getGeneticProfileId(),
                    null,
                    true,
                    mockedDao).importData();
            fail("Import has to fail");
        } catch (RuntimeException runtimeException) {
            assertTrue(runtimeException.getMessage(), runtimeException.getMessage().contains("Simulated error"));
            assertTrue(true);
        }

        /**
         * After test assertions
         */
        HashMap<Long, HashMap<Integer, String>> afterResult = DaoGeneticAlteration.getInstance().getGeneticAlterationMap(mrnaProfile.getGeneticProfileId(), null);
        assertEquals(beforeResult, afterResult);
    }

    @Before
    public void setUp() throws DaoException {
        DaoCancerStudy.reCacheAll();
    }
}
