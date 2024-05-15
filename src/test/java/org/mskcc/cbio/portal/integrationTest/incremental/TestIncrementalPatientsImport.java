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

/**
 * Tests Incremental Import of Sample Clinical Data.
 *
 * @author Ruslan Forostianov
 * @author Pieter Lukasse
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:/applicationContext-dao.xml" })
@Rollback
@Transactional
public class TestIncrementalPatientsImport {

    public static final String STUDY_ID = "study_tcga_pub";
    private CancerStudy cancerStudy;

    @Before
    public void setUp() throws DaoException {
        cancerStudy = DaoCancerStudy.getCancerStudyByStableId(STUDY_ID);
    }

    @Test
    public void testInsertNewPatient() throws DaoException {
        String newPatientId = "TEST-INC-TCGA-P2";
        File singleTcgaSampleFolder = new File("src/test/resources/incremental/insert_single_tcga_patient/");
        File metaFile = new File(singleTcgaSampleFolder, "meta_clinical_patient.txt");
        File dataFile = new File(singleTcgaSampleFolder, "clinical_data_single_PATIENT.txt");

        ImportClinicalData importClinicalData = new ImportClinicalData(new String[] {
                "--meta", metaFile.getAbsolutePath(),
                "--data", dataFile.getAbsolutePath(),
                "--overwrite-existing",
        });
        importClinicalData.run();

        Patient newPatient = DaoPatient.getPatientByCancerStudyAndPatientId(cancerStudy.getInternalId(), newPatientId);
        assertNotNull("Patient with id " + newPatientId + " has to be injected to the DB.", newPatient);

        List<ClinicalData> clinicalData = DaoClinicalData.getData(cancerStudy.getInternalId(), List.of(newPatientId));
        Map<String, String> patientAttrs = clinicalData.stream().collect(Collectors.toMap(ClinicalData::getAttrId, ClinicalData::getAttrVal));
        assertEquals(Map.of(
                "SUBTYPE", "basal-like",
                "OS_STATUS", "0:LIVING",
                "OS_MONTHS", "45.6",
                "DFS_STATUS", "1:Recurred/Progressed"), patientAttrs);
    }

    @Test
    public void testUpdatePatientAttributes() throws DaoException {
        String updatedPatientId = "TCGA-A1-A0SB";

        Patient tcgaPatient = DaoPatient.getPatientByCancerStudyAndPatientId(cancerStudy.getInternalId(),
                updatedPatientId);
        DaoClinicalData.addPatientDatum(tcgaPatient.getInternalId(), "SUBTYPE", "Luminal A");
        DaoClinicalData.addPatientDatum(tcgaPatient.getInternalId(), "OS_STATUS", "0:LIVING");
        DaoClinicalData.addPatientDatum(tcgaPatient.getInternalId(), "OS_MONTHS", "34.56");

        File singleTcgaSampleFolder = new File("src/test/resources/incremental/update_single_tcga_patient/");
        File metaFile = new File(singleTcgaSampleFolder, "meta_clinical_patient.txt");
        File dataFile = new File(singleTcgaSampleFolder, "clinical_data_single_PATIENT.txt");

        ImportClinicalData importClinicalData = new ImportClinicalData(new String[] {
                "--meta", metaFile.getAbsolutePath(),
                "--data", dataFile.getAbsolutePath(),
                "--overwrite-existing",
        });
        importClinicalData.run();

        Patient newPatient = DaoPatient.getPatientByCancerStudyAndPatientId(cancerStudy.getInternalId(), updatedPatientId);
        assertNotNull("Patient with id " + updatedPatientId + " has to be injected to the DB.", newPatient);

        List<ClinicalData> clinicalData = DaoClinicalData.getData(cancerStudy.getInternalId(), List.of(updatedPatientId));
        Map<String, String> patientAttrs = clinicalData.stream().collect(Collectors.toMap(ClinicalData::getAttrId, ClinicalData::getAttrVal));
        assertEquals(Map.of(
                "SUBTYPE", "basal-like",
                "OS_MONTHS", "56.7",
                "DFS_STATUS", "1:Recurred/Progressed",
                "DFS_MONTHS", "100"), patientAttrs);
    }
}
