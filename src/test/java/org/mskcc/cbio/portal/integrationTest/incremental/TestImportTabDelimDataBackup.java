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
import org.junit.runner.RunWith;
import org.mskcc.cbio.portal.dao.DaoCancerStudy;
import org.mskcc.cbio.portal.dao.DaoException;
import org.mskcc.cbio.portal.dao.DaoGeneOptimized;
import org.mskcc.cbio.portal.dao.DaoGeneticAlteration;
import org.mskcc.cbio.portal.dao.DaoGeneticProfile;
import org.mskcc.cbio.portal.scripts.ImportTabDelimData;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.File;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:/applicationContext-dao.xml"})
public class TestImportTabDelimDataBackup extends AbstractBackupTransactionTest {

    private static final List<String> TABLES = List.of("genetic_alteration", "genetic_profile_samples");

    private static final File DATA_FILE =
        new File("src/test/resources/incremental/mrna_expression/data_expression_Zscores.txt");

    private int profileId;

    @Before
    public void setUp() throws DaoException {
        DaoCancerStudy.reCacheAll();
        profileId = DaoGeneticProfile.getGeneticProfileByStableId("study_tcga_pub_mrna").getGeneticProfileId();
    }

    @Override
    protected List<String> getBackedUpTables() {
        return TABLES;
    }

    @Override
    protected void runSuccessfulImport() throws Exception {
        new ImportTabDelimData(DATA_FILE, profileId, null, false, DaoGeneOptimized.getInstance()).importData();
    }

    @Override
    protected void runFailingImport() throws Exception {
        DaoGeneOptimized mockedDao = mock(DaoGeneOptimized.class);
        when(mockedDao.getGene(anyLong())).thenAnswer(invocation -> {
            assertBackupTablesExist();
            throw new RuntimeException("Simulated error");
        });
        new ImportTabDelimData(DATA_FILE, profileId, null, false, mockedDao).importData();
    }

    @Override
    protected Object captureDataState() throws DaoException {
        return DaoGeneticAlteration.getInstance().getGeneticAlterationMap(profileId, null);
    }

    @Override
    protected void assertDataStateUnchanged(Object stateBefore) throws DaoException {
        HashMap<Long, HashMap<Integer, String>> after =
            DaoGeneticAlteration.getInstance().getGeneticAlterationMap(profileId, null);
        assertEquals(stateBefore, after);
    }
}
