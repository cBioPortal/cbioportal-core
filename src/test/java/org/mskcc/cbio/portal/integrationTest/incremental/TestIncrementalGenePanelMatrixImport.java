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
import org.mskcc.cbio.portal.dao.DaoGenePanel;
import org.mskcc.cbio.portal.dao.DaoGeneticProfile;
import org.mskcc.cbio.portal.dao.DaoSample;
import org.mskcc.cbio.portal.dao.DaoSampleProfile;
import org.mskcc.cbio.portal.model.CancerStudy;
import org.mskcc.cbio.portal.model.ExtendedMutation;
import org.mskcc.cbio.portal.model.GenePanel;
import org.mskcc.cbio.portal.model.GeneticProfile;
import org.mskcc.cbio.portal.model.Sample;
import org.mskcc.cbio.portal.scripts.ImportGenePanelProfileMap;
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
import static org.junit.Assert.assertNull;
import static org.mskcc.cbio.portal.dao.DaoMutation.getMutations;

/**
 * Tests Incremental Import of Gene Panel Matrix Data.
 *
 * @author Ruslan Forostianov
 * @author Pieter Lukasse
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:/applicationContext-dao.xml" })
@Rollback
@Transactional
public class TestIncrementalGenePanelMatrixImport {

    /**
     * Test incremental upload
     */
	@Test
    public void testIncrementalUpload() throws DaoException {
        File dataFolder = new File("src/test/resources/incremental/gene_panel_matrix/");
        File metaFile = new File(dataFolder, "meta_gene_panel_matrix.txt");
        File dataFile = new File(dataFolder, "data_gene_panel_matrix.txt");

        ImportGenePanelProfileMap importGenePanelProfileMap = new ImportGenePanelProfileMap(new String[] {
                "--meta", metaFile.getAbsolutePath(),
                "--data", dataFile.getAbsolutePath(),
                "--overwrite-existing",
        });
        importGenePanelProfileMap.run();

        GenePanel mutationGenePanel = DaoGenePanel.getGenePanelByStableId("TSTGNPNLMUTEXT");
        GeneticProfile mutationsProfile = DaoGeneticProfile.getGeneticProfileByStableId("study_tcga_pub_mutations");
        GenePanel longGenePanel = DaoGenePanel.getGenePanelByStableId("TESTPANEL_CNA_DISCRETE_LONG_FORMAT");
        GeneticProfile geneticProfile = DaoGeneticProfile.getGeneticProfileByStableId("study_tcga_pub_gistic");
        GeneticProfile ic50Profile = DaoGeneticProfile.getGeneticProfileByStableId("study_tcga_pub_treatment_ic50");
        CancerStudy cancerStudy = DaoCancerStudy.getCancerStudyByStableId("study_tcga_pub");
        Sample sample = DaoSample.getSampleByCancerStudyAndSampleId(cancerStudy.getInternalId(), "TCGA-A1-A0SB-01");
        assertEquals(mutationGenePanel.getInternalId(),
                DaoSampleProfile.getPanelId(sample.getInternalId(), mutationsProfile.getGeneticProfileId()));
        assertEquals(longGenePanel.getInternalId(),
                DaoSampleProfile.getPanelId(sample.getInternalId(), geneticProfile.getGeneticProfileId()));
        assertNull(DaoSampleProfile.getPanelId(sample.getInternalId(), ic50Profile.getGeneticProfileId()));
    }

}
