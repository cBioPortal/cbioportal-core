/*
 * This file is part of cBioPortal.
 * Copyright (C) 2026  Memorial Sloan-Kettering Cancer Center.
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
import org.mskcc.cbio.portal.dao.DaoPatient;
import org.mskcc.cbio.portal.dao.DaoSample;
import org.mskcc.cbio.portal.model.CancerStudy;
import org.mskcc.cbio.portal.model.Patient;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:/applicationContext-dao.xml"})
public class TestDaoSampleDelete extends AbstractDaoDeleteTest {

    // Seed sample TCGA-A1-A0SB-01 belongs to patient TCGA-A1-A0SB
    private static final String SAMPLE_STABLE_ID  = "TCGA-A1-A0SB-01";
    private static final String PATIENT_STABLE_ID = "TCGA-A1-A0SB";

    private CancerStudy study;
    private int studyId;
    private int sampleInternalId;
    private int patientInternalId;

    @Before
    public void setUp() throws DaoException {
        DaoCancerStudy.reCacheAll();
        study = DaoCancerStudy.getCancerStudyByStableId("study_tcga_pub");
        assertNotNull(study);
        studyId = study.getInternalId();

        sampleInternalId = DaoSample.getSampleByCancerStudyAndSampleId(studyId, SAMPLE_STABLE_ID).getInternalId();

        Patient patient = DaoPatient.getPatientByCancerStudyAndPatientId(studyId, PATIENT_STABLE_ID);
        assertNotNull(patient);
        patientInternalId = patient.getInternalId();
    }

    @Test
    public void testDeleteSamples_sampleIsGone() throws DaoException {
        DaoSample.deleteSamples(studyId, Set.of(SAMPLE_STABLE_ID));

        assertNull("sample should be gone after deletion",
            DaoSample.getSampleByCancerStudyAndSampleId(studyId, SAMPLE_STABLE_ID));
    }

    @Test
    public void testDeleteSamples_sampleRowsAreGone() throws DaoException {
        DaoSample.deleteSamples(studyId, Set.of(SAMPLE_STABLE_ID));

        assertEquals(0L, countRowsWhereEq("sample",          "internal_id", sampleInternalId));
        assertEquals(0L, countRowsWhereEq("clinical_sample", "internal_id", sampleInternalId));
    }

    @Test
    public void testDeleteSamples_relatedProfileRowsAreGone() throws DaoException {
        DaoSample.deleteSamples(studyId, Set.of(SAMPLE_STABLE_ID));

        assertEquals(0L, countRowsWhereEq("sample_profile",   "sample_id", sampleInternalId));
        assertEquals(0L, countRowsWhereEq("sample_cna_event", "sample_id", sampleInternalId));
        assertEquals(0L, countRowsWhereEq("mutation",         "sample_id", sampleInternalId));
    }

    @Test
    public void testDeleteSamples_patientIsPreserved() throws DaoException {
        DaoSample.deleteSamples(studyId, Set.of(SAMPLE_STABLE_ID));

        // Deleting a sample must not delete the parent patient
        assertNotNull("patient should still exist after sample-only deletion",
            DaoPatient.getPatientByCancerStudyAndPatientId(studyId, PATIENT_STABLE_ID));
        assertEquals(1L, countRowsWhereEq("patient", "internal_id", patientInternalId));
    }

    @Test
    public void testDeleteSamples_noStagingTablesRemain() throws DaoException {
        DaoSample.deleteSamples(studyId, Set.of(SAMPLE_STABLE_ID));
        assertNoStagingTablesRemain();
    }

    @Test
    public void testDeleteSamples_emptyInput_isNoOp() throws DaoException {
        DaoSample.deleteSamples(studyId, Set.of());

        assertNotNull("sample should still exist when nothing was deleted",
            DaoSample.getSampleByCancerStudyAndSampleId(studyId, SAMPLE_STABLE_ID));
    }
}
