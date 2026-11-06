package org.mskcc.cbio.portal.dao;

import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mskcc.cbio.portal.model.ClinicalEvent;
import org.mskcc.cbio.portal.scripts.ImportTimelineData;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.ArgumentMatchers.any;
import org.mskcc.cbio.portal.model.CancerStudy;
import org.mskcc.cbio.portal.model.GeneticProfile;
import org.mskcc.cbio.portal.model.Patient;
import org.mskcc.cbio.portal.model.Sample;
import org.mskcc.cbio.portal.model.shared.GeneticAlterationType;
import static org.junit.Assert.*;

/** Database reads deliberately never see this import's writes. */
public class TestImportStateRetention {
    private final List<String> queries = new ArrayList<>();

    @Before
    public void setUp() throws Exception {
        ResultSet empty = EasyMock.createNiceMock(ResultSet.class);
        EasyMock.expect(empty.next()).andReturn(false).anyTimes();
        EasyMock.replay(empty);
        PreparedStatement statement = EasyMock.createNiceMock(PreparedStatement.class);
        EasyMock.expect(statement.executeQuery()).andReturn(empty).anyTimes();
        EasyMock.replay(statement);
        Connection connection = EasyMock.createNiceMock(Connection.class);
        EasyMock.expect(connection.prepareStatement(EasyMock.anyString())).andAnswer(() -> {
            queries.add((String) EasyMock.getCurrentArguments()[0]);
            return statement;
        }).anyTimes();
        EasyMock.replay(connection);
        DataSource source = EasyMock.createNiceMock(DataSource.class);
        EasyMock.expect(source.getConnection()).andReturn(connection).anyTimes();
        EasyMock.replay(source);
        JdbcUtil.setDataSource(source);
        ClickHouseAutoIncrement.resetCounters();
        ClickHouseBulkLoader.discardAll();
        DaoPatient.reCache();
        DaoSample.reCache();
        DaoGeneticProfile.reCache();
    }

    @After
    public void tearDown() {
        ClickHouseBulkLoader.discardAll();
        ClickHouseAutoIncrement.resetCounters();
    }

    private Patient patient(int studyId, int patientId) {
        CancerStudy study = new CancerStudy("test", "test", "study_" + studyId, "brca", true);
        study.setInternalId(studyId);
        Patient patient = new Patient(study, "patient", patientId);
        DaoPatient.cachePatient(patient, studyId);
        return patient;
    }

    private int sample(Patient patient, String stableId) throws Exception {
        ClickHouseBulkLoader.bulkLoadOn();
        return DaoSample.addSample(new Sample(-1, stableId, patient.getInternalId()));
    }

    @Test
    public void persistKeepsCounterWhenDatabaseCannotSeeEarlierWrites() throws Exception {
        assertEquals(1, ClickHouseAutoIncrement.nextId("seq_genetic_profile"));
        queries.clear();
        ClickHouseAutoIncrement.persistAllSequences();
        assertEquals(2, ClickHouseAutoIncrement.nextId("seq_genetic_profile"));
        ClickHouseAutoIncrement.persistAllSequences();
        assertEquals(3, ClickHouseAutoIncrement.nextId("seq_genetic_profile"));
        assertEquals(2, queries.size());
        assertTrue(queries.stream().allMatch(sql -> sql.startsWith("INSERT INTO cbioportal_sequence_state")));
    }

    @Test
    public void fileImportSequencesKeepIdsAcrossFilesDespiteStaleMaxReads() throws Exception {
        for (String sequence : List.of("seq_clinical_event", "seq_mutation_event",
                "seq_structural_variant", "seq_copy_number_seg")) {
            assertEquals(1, ClickHouseAutoIncrement.nextId(sequence));
            ClickHouseAutoIncrement.persistAllSequences();
            queries.clear();
            assertEquals(2, ClickHouseAutoIncrement.nextId(sequence));
            assertEquals(3, ClickHouseAutoIncrement.nextId(sequence));
            assertTrue("A later file must not reread a stale maximum: " + sequence, queries.isEmpty());
        }
    }

    @Test
    public void twoTimelineFilesDoNotReuseIdsWhenReadsCannotSeeFirstFile() throws Exception {
        patient(1, 10);
        Path file = Files.createTempFile("timeline", ".tsv");
        List<Long> insertedIds = new ArrayList<>();
        Method importFile = ImportTimelineData.class
            .getDeclaredMethod("importData", String.class, int.class, boolean.class);
        importFile.setAccessible(true);
        try (MockedStatic<DaoClinicalEvent> events = mockStatic(DaoClinicalEvent.class)) {
            Files.writeString(file, "PATIENT_ID\tSTART_DATE\tEVENT_TYPE\npatient\t1\tSTATUS\n");
            events.when(() -> DaoClinicalEvent.addClinicalEvent(any()))
                .thenAnswer(call -> {
                    insertedIds.add(((ClinicalEvent) call.getArgument(0)).getClinicalEventId());
                    return null;
                });
            importFile.invoke(null, file.toString(), 1, false);
            ClickHouseAutoIncrement.persistAllSequences();
            importFile.invoke(null, file.toString(), 1, false);
            assertEquals(List.of(1L, 2L), insertedIds);
        } finally {
            Files.deleteIfExists(file);
        }
    }

    @Test
    public void deletingSamplePreservesSurvivorsAndSharedStableIdsWithoutQueries() throws Exception {
        Patient first = patient(1, 10);
        Patient second = patient(2, 20);
        int deleted = sample(first, "shared");
        int survivor = sample(first, "survivor");
        int otherStudy = sample(second, "shared");
        queries.clear();
        DaoSample.removeSamplesFromCache(1, List.of(deleted));
        assertNull(DaoSample.getSampleById(deleted));
        assertNull(DaoSample.getSampleByPatientAndSampleId(10, "shared"));
        assertNull(DaoSample.getSampleByCancerStudyAndSampleId(1, "shared", false));
        assertEquals(survivor, DaoSample.getSampleByCancerStudyAndSampleId(1, "survivor").getInternalId());
        assertEquals(otherStudy, DaoSample.getSampleByCancerStudyAndSampleId(2, "shared").getInternalId());
        assertTrue(DaoSample.getAllSamples().stream().anyMatch(s -> s.getInternalId() == otherStudy));
        assertTrue(queries.isEmpty());
    }

    @Test
    public void partialPatientEvictionKeepsOtherPatientsInStudy() {
        Patient deleted = patient(1, 10);
        Patient survivor = new Patient(deleted.getCancerStudy(), "other", 11);
        DaoPatient.cachePatient(survivor, 1);
        queries.clear();
        DaoPatient.removePatientsFromCache(List.of(10));
        assertNull(DaoPatient.getPatientById(10));
        assertNull(DaoPatient.getPatientByCancerStudyAndPatientId(1, "patient"));
        assertSame(survivor, DaoPatient.getPatientById(11));
        assertSame(survivor, DaoPatient.getPatientByCancerStudyAndPatientId(1, "other"));
        assertEquals(java.util.Set.of(survivor), DaoPatient.getPatientsByCancerStudyId(1));
        assertTrue(queries.isEmpty());
    }

    @Test
    public void studyEvictionPreservesOtherStudyAndAllowsReinsertion() throws Exception {
        Patient first = patient(1, 10);
        Patient second = patient(2, 20);
        int deleted = sample(first, "shared");
        int survivor = sample(second, "shared");
        GeneticProfile profile = new GeneticProfile("study_1_mutations", 1,
            GeneticAlterationType.MUTATION_EXTENDED, "MAF", "mutations", "mutations", true);
        DaoGeneticProfile.addGeneticProfile(profile);
        GeneticProfile other = new GeneticProfile("study_2_mutations", 2,
            GeneticAlterationType.MUTATION_EXTENDED, "MAF", "mutations", "mutations", true);
        DaoGeneticProfile.addGeneticProfile(other);
        queries.clear();
        DaoSample.removeStudyFromCache(1);
        DaoPatient.removeStudyFromCache(1);
        DaoGeneticProfile.removeStudyFromCache(1);
        assertNull(DaoSample.getSampleById(deleted));
        assertTrue(DaoSample.getSamplesByCancerStudy(1).isEmpty());
        assertTrue(DaoSample.getSamplesByPatientId(10).isEmpty());
        assertNull(DaoPatient.getPatientById(10));
        assertNull(DaoPatient.getPatientByCancerStudyAndPatientId(1, "patient"));
        assertNull(DaoGeneticProfile.getGeneticProfileById(profile.getGeneticProfileId()));
        assertNull(DaoGeneticProfile.getGeneticProfileByStableId(profile.getStableId()));
        assertTrue(DaoGeneticProfile.getAllGeneticProfiles(1).isEmpty());
        assertEquals(List.of(other), DaoGeneticProfile.getAllGeneticProfiles(2));
        assertNotNull(DaoPatient.getPatientById(20));
        assertNotNull(DaoSample.getSampleById(survivor));
        assertSame(other, DaoGeneticProfile.getGeneticProfileById(other.getGeneticProfileId()));
        assertTrue(queries.isEmpty());
        Patient replacement = patient(1, 30);
        int newSample = sample(replacement, "shared");
        assertEquals(newSample, DaoSample.getSampleByCancerStudyAndSampleId(1, "shared").getInternalId());
        assertEquals(survivor, DaoSample.getSampleByCancerStudyAndSampleId(2, "shared").getInternalId());
    }
}
