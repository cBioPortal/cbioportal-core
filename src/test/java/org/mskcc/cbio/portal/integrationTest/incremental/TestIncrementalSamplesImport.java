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
import org.mskcc.cbio.portal.dao.*;
import org.mskcc.cbio.portal.model.*;
import org.mskcc.cbio.portal.scripts.ImportClinicalData;
import org.mskcc.cbio.portal.scripts.UsageException;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.annotation.Transactional;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

/**
 * Tests Incremental Import of Sample Clinical Data.
 *
 * @author Ruslan Forostianov
 * @author Pieter Lukasse
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:/applicationContext-dao.xml"})
@Rollback
@Transactional
public class TestIncrementalSamplesImport {

    static final String STUDY_ID = "study_tcga_pub";
    static final String EXISTING_PATIENT_ID = "TEST-INC-TCGA-P1";
    static final String INSERT_SAMPLE_ID_FOR_EXISTING_PATIENT = "TEST-INC-TCGA-P1-S1";
    static final String NON_EXISTING_PATIENT_ID = "TEST-INC-TCGA-P2";
    static final String INSERT_SAMPLE_ID_FOR_NON_EXISTING_PATIENT = "TEST-INC-TCGA-P2-S1";
    static final String UPDATE_SAMPLE_ID = "TCGA-A1-A0SH-01";
    static final File STUDY_FOLDER = new File("src/test/resources/incremental/study_tcga_pub");
    static final File META_FILE = new File(STUDY_FOLDER, "meta_clinical_samples.txt");
    static final File DATA_FILE = new File(STUDY_FOLDER, "data_clinical_samples.txt");
    CancerStudy cancerStudy;

    @Before
    public void setUp() throws DaoException {
        DaoCancerStudy.reCacheAll();
        cancerStudy = DaoCancerStudy.getCancerStudyByStableId(STUDY_ID);
    }

    /**
     * Test inserting new sample for existing patient
     */
    @Test
    public void testInsertNewSampleForExistingPatient() throws DaoException {
        /**
         * prepare a new patient without samples
         */
        Patient patient = new Patient(cancerStudy, EXISTING_PATIENT_ID);
        int internalPatientId = DaoPatient.addPatient(patient);
        DaoClinicalData.addPatientDatum(internalPatientId, "OS_STATUS", "0:LIVING");

        new ImportClinicalData(new String[]{
                "--meta", META_FILE.getAbsolutePath(),
                "--data", DATA_FILE.getAbsolutePath(),
                "--sample-ids-only", INSERT_SAMPLE_ID_FOR_EXISTING_PATIENT
        }).run();

        List<Sample> samples = DaoSample.getSamplesByPatientId(internalPatientId);
        assertEquals("A new sample has to be attached to the patient", 1, samples.size());
        Sample sample = samples.get(0);
        assertEquals(INSERT_SAMPLE_ID_FOR_EXISTING_PATIENT, sample.getStableId());

        List<ClinicalData> sampleClinicalData = DaoClinicalData.getSampleData(cancerStudy.getInternalId(), List.of(INSERT_SAMPLE_ID_FOR_EXISTING_PATIENT));
        Map<String, String> sampleAttrs = sampleClinicalData.stream().collect(Collectors.toMap(ClinicalData::getAttrId, ClinicalData::getAttrVal));
        assertEquals(Map.of(
                "SUBTYPE", "basal-like",
                "OS_STATUS", "1:DECEASED",
                "OS_MONTHS", "12.34",
                "DFS_STATUS", "1:Recurred/Progressed"), sampleAttrs);
    }

    @Test
    public void testSampleCountPatientAttribute() throws DaoException {
        /**
         * prepare a new patient without samples
         */
        Patient patient = new Patient(cancerStudy, EXISTING_PATIENT_ID);
        DaoPatient.addPatient(patient);

        new ImportClinicalData(new String[]{
                "--meta", META_FILE.getAbsolutePath(),
                "--data", DATA_FILE.getAbsolutePath(),
                "--sample-ids-only", INSERT_SAMPLE_ID_FOR_EXISTING_PATIENT
        }).run();

        // Patient attributes get SAMPLE_COUNT
        List<ClinicalData> patientClinicalData = DaoClinicalData.getData(cancerStudy.getInternalId(), List.of(EXISTING_PATIENT_ID));
        Map<String, String> patientAttrs = patientClinicalData.stream().collect(Collectors.toMap(ClinicalData::getAttrId, ClinicalData::getAttrVal));
        assertEquals(Map.of(
                "SAMPLE_COUNT", "1"), patientAttrs);
    }

    /**
     * Test inserting new sample for nonexistent patient.
     * EXPECTED RESULTS:
     * 1. The new patient entry has to be inserted
     * 2. Sample and all its clinical attributes have to be inserted
     */
    @Test
    public void testInsertNewSampleForNonexistentPatient() throws DaoException {
        new ImportClinicalData(new String[]{
                "--meta", META_FILE.getAbsolutePath(),
                "--data", DATA_FILE.getAbsolutePath(),
                "--sample-ids-only", INSERT_SAMPLE_ID_FOR_NON_EXISTING_PATIENT
        }).run();

        Patient newPatient = DaoPatient.getPatientByCancerStudyAndPatientId(cancerStudy.getInternalId(), NON_EXISTING_PATIENT_ID);
        assertNotNull("The new patient has to be created.", newPatient);

        List<Sample> samples = DaoSample.getSamplesByPatientId(newPatient.getInternalId());
        assertEquals("A new sample has to be attached to the patient", 1, samples.size());
        Sample sample = samples.get(0);
        assertEquals(INSERT_SAMPLE_ID_FOR_NON_EXISTING_PATIENT, sample.getStableId());

        List<ClinicalData> clinicalData = DaoClinicalData.getSampleData(cancerStudy.getInternalId(), List.of(INSERT_SAMPLE_ID_FOR_NON_EXISTING_PATIENT));
        Map<String, String> sampleAttrs = clinicalData.stream().collect(Collectors.toMap(ClinicalData::getAttrId, ClinicalData::getAttrVal));
        assertEquals(Map.of(
                "SUBTYPE", "Luminal A",
                "OS_STATUS", "0:LIVING",
                "OS_MONTHS", "23.45",
                "DFS_STATUS", "1:Recurred/Progressed",
                "DFS_MONTHS", "100"), sampleAttrs);
    }

    /**
     * Test reloading sample clinical attributes
     */
    @Test
    public void testReloadSampleClinicalAttributes() throws DaoException {
        /**
         * Add to a tcga sample some clinical attributes (test data sets doesn't have any)
         */
        Sample tcgaSample = DaoSample.getSampleByCancerStudyAndSampleId(cancerStudy.getInternalId(),
                UPDATE_SAMPLE_ID);
        DaoClinicalData.addSampleDatum(tcgaSample.getInternalId(), "SUBTYPE", "Luminal A");
        DaoClinicalData.addSampleDatum(tcgaSample.getInternalId(), "OS_STATUS", "0:LIVING");
        DaoClinicalData.addSampleDatum(tcgaSample.getInternalId(), "OS_MONTHS", "34.56");

        new ImportClinicalData(new String[]{
                "--meta", META_FILE.getAbsolutePath(),
                "--data", DATA_FILE.getAbsolutePath(),
                "--sample-ids-only", UPDATE_SAMPLE_ID
        }).run();

        List<ClinicalData> clinicalData = DaoClinicalData.getSampleData(cancerStudy.getInternalId(), List.of(UPDATE_SAMPLE_ID));
        Map<String, String> sampleAttrs = clinicalData.stream().collect(Collectors.toMap(ClinicalData::getAttrId, ClinicalData::getAttrVal));
        assertEquals(Map.of(
                "OS_STATUS", "1:DECEASED",
                "OS_MONTHS", "45.67",
                "DFS_STATUS", "1:Recurred/Progressed",
                "DFS_MONTHS", "123"), sampleAttrs);

        /**
         * Sub-entries stayed as they were, not removed.
         */
        GeneticProfile mutationsProfile = DaoGeneticProfile.getGeneticProfileByStableId("study_tcga_pub_mutations");
        assertNotNull(mutationsProfile);
        ArrayList<ExtendedMutation> mutations = DaoMutation.getMutations(mutationsProfile.getGeneticProfileId(), tcgaSample.getInternalId());
        assertEquals(2, mutations.size());
    }

    @Test
    public void testSampleIdSelection() {
        new ImportClinicalData(new String[]{
                "--meta", META_FILE.getAbsolutePath(),
                "--data", DATA_FILE.getAbsolutePath(),
                "--sample-ids-only", String.join(",", UPDATE_SAMPLE_ID, INSERT_SAMPLE_ID_FOR_NON_EXISTING_PATIENT)
        }).run();

        Set<String> studySampleIds = DaoSample
                .getAllSamples()
                .stream()
                .map(Sample::getStableId)
                .collect(Collectors.toSet());

        assertTrue(studySampleIds.contains(UPDATE_SAMPLE_ID));
        assertTrue(studySampleIds.contains(INSERT_SAMPLE_ID_FOR_NON_EXISTING_PATIENT));
        assertFalse(INSERT_SAMPLE_ID_FOR_EXISTING_PATIENT + " sample is not selected and must not be inserted to the database.",
                studySampleIds.contains(INSERT_SAMPLE_ID_FOR_EXISTING_PATIENT));
    }
    @Test
    public void testSampleIdsValueHasToBeSpecified() {
        assertThrows("'--sample-ids-only' argument has to come with a value (e.g. --sample-ids-only SAMPLE_ID1,SAMPLE_ID2)",
                UsageException.class,
                new ImportClinicalData(new String[]{
                        "--meta", META_FILE.getAbsolutePath(),
                        "--data", DATA_FILE.getAbsolutePath(),
                        "--sample-ids-only"
                })::run);
    }

    @Test
    public void testSelectNonExistingSample() {
        assertThrows("The following sample ids are selected to be uploaded, but never found in the study files: NON_EXISTING_SAMPLE_ID",
                RuntimeException.class,
                new ImportClinicalData(new String[]{
                        "--meta", META_FILE.getAbsolutePath(),
                        "--data", DATA_FILE.getAbsolutePath(),
                        "--sample-ids-only", String.join(",", INSERT_SAMPLE_ID_FOR_NON_EXISTING_PATIENT, "NON_EXISTING_SAMPLE_ID")
                })::run);
    }
}
