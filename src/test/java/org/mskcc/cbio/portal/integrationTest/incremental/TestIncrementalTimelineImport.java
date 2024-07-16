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
import org.mskcc.cbio.portal.dao.DaoClinicalEvent;
import org.mskcc.cbio.portal.dao.DaoException;
import org.mskcc.cbio.portal.dao.DaoPatient;
import org.mskcc.cbio.portal.dao.MySQLbulkLoader;
import org.mskcc.cbio.portal.model.CancerStudy;
import org.mskcc.cbio.portal.model.ClinicalEvent;
import org.mskcc.cbio.portal.model.Patient;
import org.mskcc.cbio.portal.scripts.ImportTimelineData;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.annotation.Transactional;

import java.io.File;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Tests Incremental Import of Timeline Data.
 *
 * @author Ruslan Forostianov
 * @author Pieter Lukasse
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:/applicationContext-dao.xml" })
@Rollback
@Transactional
public class TestIncrementalTimelineImport {

    public static final String STUDY_ID = "study_tcga_pub";
    private CancerStudy cancerStudy;

    @Before
    public void setUp() throws DaoException {
        cancerStudy = DaoCancerStudy.getCancerStudyByStableId(STUDY_ID);
    }

	@Test
    public void testTimelineDataReloading() throws DaoException {
        MySQLbulkLoader.bulkLoadOn();
        ClinicalEvent event = new ClinicalEvent();
        event.setClinicalEventId(1L);
        Patient sbPatient = DaoPatient.getPatientByCancerStudyAndPatientId(cancerStudy.getInternalId(), "TCGA-A1-A0SB");
        event.setPatientId(sbPatient.getInternalId());
        event.setStartDate(5L);
        event.setEventType("SPECIMEN");
        event.setEventData(Map.of("SPECIMEN_SITE", "specimen_site_to_erase"));
        DaoClinicalEvent.addClinicalEvent(event);
        MySQLbulkLoader.flushAll();

        File singleTcgaSampleFolder = new File("src/test/resources/incremental/clinical/");
        File metaFile = new File(singleTcgaSampleFolder, "meta_timeline.txt");
        File dataFile = new File(singleTcgaSampleFolder, "data_timeline.txt");

        ImportTimelineData importTimelineData = new ImportTimelineData(new String[] {
                "--meta", metaFile.getAbsolutePath(),
                "--data", dataFile.getAbsolutePath(),
                "--overwrite-existing",
        });
        importTimelineData.run();

        List<ClinicalEvent> sbClinicalEvents = DaoClinicalEvent.getClinicalEvent(sbPatient.getInternalId());
        assertEquals(2, sbClinicalEvents.size());
        ClinicalEvent sbSpecimen = sbClinicalEvents.stream().filter(ce -> ce.getEventType().equals("SPECIMEN")).findFirst().get();
        assertEquals(20L, sbSpecimen.getStartDate());
        assertEquals(60L, sbSpecimen.getStopDate());
        assertEquals(Map.of(
                "SPECIMEN_SITE", "test_specimen_site_1",
                "SPECIMEN_TYPE", "test_specimen_type",
                "SOURCE", "test_source_3"
        ), sbSpecimen.getEventData());
        ClinicalEvent sbStatus = sbClinicalEvents.stream().filter(ce -> ce.getEventType().equals("STATUS")).findFirst().get();
        assertEquals(10L, sbStatus.getStartDate());
        assertEquals(20L, sbStatus.getStopDate());
        assertEquals(Map.of("SOURCE", "test_source_4"), sbStatus.getEventData());

        Patient sdPatient = DaoPatient.getPatientByCancerStudyAndPatientId(cancerStudy.getInternalId(), "TCGA-A1-A0SD");
        List<ClinicalEvent> sdClinicalEvents = DaoClinicalEvent.getClinicalEvent(sdPatient.getInternalId());
        assertEquals(1, sdClinicalEvents.size());
        ClinicalEvent sdStatus = sdClinicalEvents.stream().filter(ce -> ce.getEventType().equals("STATUS")).findFirst().get();
        assertEquals(45L, sdStatus.getStartDate());
        assertNull(sdStatus.getStopDate());
        assertEquals(Map.of("SOURCE", "test_source_2"), sdStatus.getEventData());

        Patient nonexistentPatient = DaoPatient.getPatientByCancerStudyAndPatientId(cancerStudy.getInternalId(), "NONEXISTENT_PATIENT");
        assertNull(nonexistentPatient);
    }

}
