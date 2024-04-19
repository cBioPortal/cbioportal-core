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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

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
public class TestIncrementalPatientsImport {

    static final String STUDY_ID = "study_tcga_pub";
    static final String INSERT_PATIENT_ID = "TEST-INC-TCGA-P2";
    static final String UPDATE_PATIENT_ID = "TCGA-A1-A0SB";
    static final File STUDY_FOLDER = new File("src/test/resources/incremental/study_tcga_pub");
    static final File META_FILE = new File(STUDY_FOLDER, "meta_clinical_patients.txt");
    static final File DATA_FILE = new File(STUDY_FOLDER, "data_clinical_patients.txt");
    CancerStudy cancerStudy;

    @Before
    public void setUp() throws DaoException {
        DaoCancerStudy.reCacheAll();
        cancerStudy = DaoCancerStudy.getCancerStudyByStableId(STUDY_ID);
    }

    @Test
    public void testInsertNewPatient() throws DaoException {
        new ImportClinicalData(new String[]{
                "--meta", META_FILE.getAbsolutePath(),
                "--data", DATA_FILE.getAbsolutePath(),
                "--patient-ids-only", INSERT_PATIENT_ID,
        }).run();

        Patient insertPatient = DaoPatient.getPatientByCancerStudyAndPatientId(cancerStudy.getInternalId(), INSERT_PATIENT_ID);
        assertNotNull("Patient with id " + INSERT_PATIENT_ID + " has to be injected to the DB.", insertPatient);
        List<ClinicalData> insertPatientClinicalData = DaoClinicalData.getData(cancerStudy.getInternalId(), List.of(INSERT_PATIENT_ID));
        Map<String, String> insertPatientAttributes = insertPatientClinicalData.stream().collect(Collectors.toMap(ClinicalData::getAttrId, ClinicalData::getAttrVal));
        assertEquals(Map.of(
                "SUBTYPE", "basal-like",
                "OS_STATUS", "0:LIVING",
                "OS_MONTHS", "45.6",
                "DFS_STATUS", "1:Recurred/Progressed"), insertPatientAttributes);
    }

    @Test
    public void testInsertPatientSelection() throws DaoException {
        new ImportClinicalData(new String[]{
                "--meta", META_FILE.getAbsolutePath(),
                "--data", DATA_FILE.getAbsolutePath(),
                "--patient-ids-only", INSERT_PATIENT_ID,
        }).run();

        List<ClinicalData> updatePatientClinicalData = DaoClinicalData.getData(cancerStudy.getInternalId(), List.of(UPDATE_PATIENT_ID));
        assertTrue("This patient should not get his clinical attributes uploaded as its id has not been selected", updatePatientClinicalData.isEmpty());
    }

    @Test
    public void testUpdatePatientAttributes() throws DaoException {
        //Prep some clinical attributes for the patient to be updated by the testee importer
        Patient tcgaPatient = DaoPatient.getPatientByCancerStudyAndPatientId(cancerStudy.getInternalId(),
                UPDATE_PATIENT_ID);
        DaoClinicalData.addPatientDatum(tcgaPatient.getInternalId(), "SUBTYPE", "Luminal A");
        DaoClinicalData.addPatientDatum(tcgaPatient.getInternalId(), "OS_STATUS", "0:LIVING");
        DaoClinicalData.addPatientDatum(tcgaPatient.getInternalId(), "OS_MONTHS", "34.56");

        new ImportClinicalData(new String[]{
                "--meta", META_FILE.getAbsolutePath(),
                "--data", DATA_FILE.getAbsolutePath(),
                "--patient-ids-only", UPDATE_PATIENT_ID,
        }).run();

        List<ClinicalData> updatePatientClinicalData = DaoClinicalData.getData(cancerStudy.getInternalId(), List.of(UPDATE_PATIENT_ID));
        Map<String, String> insertPatientAttributes = updatePatientClinicalData.stream().collect(Collectors.toMap(ClinicalData::getAttrId, ClinicalData::getAttrVal));
        assertEquals(Map.of(
                "SUBTYPE", "basal-like",
                "OS_MONTHS", "56.7",
                "DFS_STATUS", "1:Recurred/Progressed",
                "DFS_MONTHS", "100"), insertPatientAttributes);
    }

    @Test
    public void testUpdatePatientSelection() {
        new ImportClinicalData(new String[]{
                "--meta", META_FILE.getAbsolutePath(),
                "--data", DATA_FILE.getAbsolutePath(),
                "--patient-ids-only", UPDATE_PATIENT_ID,
        }).run();

        Patient insertPatient = DaoPatient.getPatientByCancerStudyAndPatientId(cancerStudy.getInternalId(), INSERT_PATIENT_ID);
        assertNull("Patient with id " + INSERT_PATIENT_ID + " has not to be injected to the DB as its id has not been selected.", insertPatient);
    }

    @Test
    public void testPatientIdsValueHasToBeSpecified() {
        assertThrows("'--patient-ids-only' argument has to come with a value (e.g. --patient-ids-only PATIENT_ID1,PATIENT_ID2)",
                UsageException.class,
                new ImportClinicalData(new String[]{
                        "--meta", META_FILE.getAbsolutePath(),
                        "--data", DATA_FILE.getAbsolutePath(),
                        "--patient-ids-only"
                })::run);
    }

    @Test
    public void testSelectNonExistingPatient() {
        assertThrows("The following patient ids are selected to be uploaded, but never found in the study files: NON_EXISTING_PATIENT_ID",
                RuntimeException.class,
                new ImportClinicalData(new String[]{
                        "--meta", META_FILE.getAbsolutePath(),
                        "--data", DATA_FILE.getAbsolutePath(),
                        "--patient-ids-only", String.join(",", INSERT_PATIENT_ID, "NON_EXISTING_PATIENT_ID")
                })::run);
    }
    @Test
    public void testSampleIdsCannotBeSpecified() {
        assertThrows("--sample-ids-only cannot be specified while loading patient clinical data.",
                UsageException.class,
                new ImportClinicalData(new String[]{
                        "--meta", META_FILE.getAbsolutePath(),
                        "--data", DATA_FILE.getAbsolutePath(),
                        "--sample-ids-only", "DOES_NOT_GET_TO_VERIFY_SAMPLE_ID"
                })::run);
    }
}
