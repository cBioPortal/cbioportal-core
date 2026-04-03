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

package org.mskcc.cbio.portal.integrationTest.dao;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mskcc.cbio.portal.dao.DaoCancerStudy;
import org.mskcc.cbio.portal.dao.DaoException;
import org.mskcc.cbio.portal.dao.DaoGeneticProfile;
import org.mskcc.cbio.portal.dao.DaoSample;
import org.mskcc.cbio.portal.dao.JdbcUtil;
import org.mskcc.cbio.portal.dao.ClickHouseBulkDeleter;
import org.mskcc.cbio.portal.model.CancerStudy;
import org.mskcc.cbio.portal.model.GeneticProfile;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:/applicationContext-dao.xml"})
public class TestDaoCancerStudyDelete extends AbstractDaoDeleteTest {

    private CancerStudy study;
    private int studyId;

    @Before
    public void setUp() throws DaoException {
        DaoCancerStudy.reCacheAll();
        study = DaoCancerStudy.getCancerStudyByStableId("study_tcga_pub");
        assertNotNull("seed data must contain study_tcga_pub", study);
        studyId = study.getInternalId();
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testDeleteCancerStudy_studyIsGone() throws DaoException {
        DaoCancerStudy.deleteCancerStudy(studyId);

        assertNull("study should no longer be accessible after deletion",
            DaoCancerStudy.getCancerStudyByStableId("study_tcga_pub"));
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testDeleteCancerStudy_geneticProfileRowsAreGone() throws Exception {
        List<Integer> profileIds = collectIds(
            "SELECT genetic_profile_id FROM genetic_profile WHERE cancer_study_id=?", studyId);
        assertFalse("seed data must have genetic profiles", profileIds.isEmpty());

        DaoCancerStudy.deleteCancerStudy(studyId);

        for (int profileId : profileIds) {
            assertEquals(0L, countRowsWhereEq("genetic_alteration",        "genetic_profile_id", profileId));
            assertEquals(0L, countRowsWhereEq("genetic_profile_samples",   "genetic_profile_id", profileId));
            assertEquals(0L, countRowsWhereEq("sample_profile",            "genetic_profile_id", profileId));
            assertEquals(0L, countRowsWhereEq("mutation",                  "genetic_profile_id", profileId));
            assertEquals(0L, countRowsWhereEq("sample_cna_event",          "genetic_profile_id", profileId));
        }
        assertFalse("genetic_profile rows should be gone",
            DaoGeneticProfile.getAllGeneticProfiles(studyId).stream()
                .anyMatch(p -> profileIds.contains(p.getGeneticProfileId())));
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testDeleteCancerStudy_sampleAndPatientRowsAreGone() throws Exception {
        List<Integer> sampleIds = collectIds(
            "SELECT s.internal_id FROM sample s JOIN patient p ON s.patient_id = p.internal_id WHERE p.cancer_study_id=?",
            studyId);
        List<Integer> patientIds = collectIds(
            "SELECT internal_id FROM patient WHERE cancer_study_id=?", studyId);

        assertFalse("seed data must have samples", sampleIds.isEmpty());
        assertFalse("seed data must have patients", patientIds.isEmpty());

        DaoCancerStudy.deleteCancerStudy(studyId);

        for (int sampleId : sampleIds) {
            assertEquals(0L, countRowsWhereEq("sample",          "internal_id", sampleId));
            assertEquals(0L, countRowsWhereEq("clinical_sample", "internal_id", sampleId));
        }
        for (int patientId : patientIds) {
            assertEquals(0L, countRowsWhereEq("patient",          "internal_id", patientId));
            assertEquals(0L, countRowsWhereEq("clinical_patient", "internal_id", patientId));
        }
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testDeleteCancerStudy_noStagingTablesRemain() throws DaoException {
        DaoCancerStudy.deleteCancerStudy(studyId);
        assertNoStagingTablesRemain();
    }

    // ── helper ──────────────────────────────────────────────────────────────

    private List<Integer> collectIds(String sql, int param) throws DaoException {
        Connection con = null;
        try {
            con = JdbcUtil.getDbConnection(ClickHouseBulkDeleter.class);
            PreparedStatement ps = con.prepareStatement(sql);
            ps.setInt(1, param);
            ResultSet rs = ps.executeQuery();
            List<Integer> ids = new ArrayList<>();
            while (rs.next()) ids.add(rs.getInt(1));
            return ids;
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(ClickHouseBulkDeleter.class, con, null, null);
        }
    }
}
