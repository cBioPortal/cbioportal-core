/*
 * Copyright (C) 2026  Memorial Sloan-Kettering Cancer Center.
 *
 * SPDX-License-Identifier: Apache-2.0
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
public class TestDaoPatientDelete extends AbstractDaoDeleteTest {

    // Seed patient TCGA-A1-A0SB has one sample: TCGA-A1-A0SB-01
    private static final String PATIENT_STABLE_ID = "TCGA-A1-A0SB";
    private static final String SAMPLE_STABLE_ID  = "TCGA-A1-A0SB-01";

    private CancerStudy study;
    private int studyId;
    private int patientInternalId;
    private int sampleInternalId;

    @Before
    public void setUp() throws DaoException {
        DaoCancerStudy.reCacheAll();
        study = DaoCancerStudy.getCancerStudyByStableId("study_tcga_pub");
        assertNotNull(study);
        studyId = study.getInternalId();

        Patient patient = DaoPatient.getPatientByCancerStudyAndPatientId(studyId, PATIENT_STABLE_ID);
        assertNotNull("seed data must contain " + PATIENT_STABLE_ID, patient);
        patientInternalId = patient.getInternalId();

        sampleInternalId = DaoSample.getSampleByCancerStudyAndSampleId(studyId, SAMPLE_STABLE_ID).getInternalId();
    }

    @Test
    public void testDeletePatients_patientIsGone() throws DaoException {
        DaoPatient.deletePatients(studyId, Set.of(PATIENT_STABLE_ID));

        assertNull("patient should be gone after deletion",
            DaoPatient.getPatientByCancerStudyAndPatientId(studyId, PATIENT_STABLE_ID));
    }

    @Test
    public void testDeletePatients_patientRowsAreGone() throws DaoException {
        DaoPatient.deletePatients(studyId, Set.of(PATIENT_STABLE_ID));

        assertEquals(0L, countRowsWhereEq("patient",          "internal_id", patientInternalId));
        assertEquals(0L, countRowsWhereEq("clinical_patient", "internal_id", patientInternalId));
    }

    @Test
    public void testDeletePatients_cascadesToSamples() throws DaoException {
        DaoPatient.deletePatients(studyId, Set.of(PATIENT_STABLE_ID));

        assertNull("patient's sample should be gone after patient deletion",
            DaoSample.getSampleByCancerStudyAndSampleId(studyId, SAMPLE_STABLE_ID));
        assertEquals(0L, countRowsWhereEq("sample",          "internal_id", sampleInternalId));
        assertEquals(0L, countRowsWhereEq("clinical_sample", "internal_id", sampleInternalId));
        assertEquals(0L, countRowsWhereEq("sample_profile",  "sample_id",   sampleInternalId));
    }

    @Test
    public void testDeletePatients_noStagingTablesRemain() throws DaoException {
        DaoPatient.deletePatients(studyId, Set.of(PATIENT_STABLE_ID));
        assertNoStagingTablesRemain();
    }

    @Test
    public void testDeletePatients_emptyInput_isNoOp() throws DaoException {
        DaoPatient.deletePatients(studyId, Set.of());

        assertNotNull("patient should still exist when nothing was deleted",
            DaoPatient.getPatientByCancerStudyAndPatientId(studyId, PATIENT_STABLE_ID));
    }
}
