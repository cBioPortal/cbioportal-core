/*
 * Copyright (c) 2015 Memorial Sloan-Kettering Cancer Center.
 *
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY, WITHOUT EVEN THE IMPLIED WARRANTY OF MERCHANTABILITY OR FITNESS
 * FOR A PARTICULAR PURPOSE. The software and documentation provided hereunder
 * is on an "as is" basis, and Memorial Sloan-Kettering Cancer Center has no
 * obligations to provide maintenance, support, updates, enhancements or
 * modifications. In no event shall Memorial Sloan-Kettering Cancer Center be
 * liable to any party for direct, indirect, special, incidental or
 * consequential damages, including lost profits, arising out of the use of this
 * software and its documentation, even if Memorial Sloan-Kettering Cancer
 * Center has been advised of the possibility of such damage.
 */

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

package org.mskcc.cbio.portal.integrationTest.scripts;

import java.io.*;
import java.util.*;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.Test;
import org.mskcc.cbio.portal.dao.ClickHouseBulkLoader;
import org.mskcc.cbio.portal.dao.DaoCancerStudy;
import org.mskcc.cbio.portal.dao.DaoException;
import org.mskcc.cbio.portal.dao.DaoGeneOptimized;
import org.mskcc.cbio.portal.dao.DaoGeneticAlteration;
import org.mskcc.cbio.portal.dao.DaoGeneticProfile;
import org.mskcc.cbio.portal.dao.DaoPatient;
import org.mskcc.cbio.portal.dao.DaoSample;
import org.mskcc.cbio.portal.dao.DaoSampleProfile;
import org.mskcc.cbio.portal.dao.JdbcUtil;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import org.mskcc.cbio.portal.model.CancerStudy;
import org.mskcc.cbio.portal.model.CanonicalGene;
import org.mskcc.cbio.portal.model.CopyNumberStatus;
import org.mskcc.cbio.portal.model.shared.GeneticAlterationType;
import org.mskcc.cbio.portal.model.GeneticProfile;
import org.mskcc.cbio.portal.model.Patient;
import org.mskcc.cbio.portal.model.Sample;
import org.mskcc.cbio.portal.scripts.ImportTabDelimData;
import org.mskcc.cbio.portal.util.ConsoleUtil;
import org.mskcc.cbio.portal.util.ProgressMonitor;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.mskcc.cbio.portal.integrationTest.IntegrationTestBase;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * JUnit tests for ImportTabDelimData class.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:/applicationContext-dao.xml" })
public class TestImportTabDelimData extends IntegrationTestBase {

    private final Set<String> NON_CASE_ID_COLS = new HashSet<>(Arrays.asList(
            "Gene Symbol",
            "Hugo_Symbol",
            "Entrez_Gene_Id",
            "Locus ID",
            "Cytoband",
            "Composite.Element.Ref",
            "geneset_id"
    ));

    private int studyId;
    private int geneticProfileId;
    private int sample1;
    private int sample2;
    private int sample3;
    private int sample4;
    private int sample5;
    private CancerStudy study;

    @Before
    public void setUp() throws DaoException {
        DaoCancerStudy.reCacheAll();
        DaoGeneOptimized.getInstance().reCache();
        ProgressMonitor.resetWarnings();

        study = DaoCancerStudy.getCancerStudyByStableId("study_tcga_pub");
        studyId =study.getInternalId();

        GeneticProfile newGeneticProfile = new GeneticProfile();
        newGeneticProfile.setCancerStudyId(studyId);
        newGeneticProfile.setGeneticAlterationType(GeneticAlterationType.COPY_NUMBER_ALTERATION);
        newGeneticProfile.setStableId("study_tcga_pub_test");
        newGeneticProfile.setProfileName("Barry CNA Results");
        newGeneticProfile.setDatatype("test");
        DaoGeneticProfile.addGeneticProfile(newGeneticProfile);

        geneticProfileId =  DaoGeneticProfile.getGeneticProfileByStableId("study_tcga_pub_test").getGeneticProfileId();

        sample1 = DaoSample.getSampleByCancerStudyAndSampleId(studyId, "TCGA-A1-A0SB-01").getInternalId();
        sample2 = DaoSample.getSampleByCancerStudyAndSampleId(studyId, "TCGA-A1-A0SD-01").getInternalId();
        sample3 = DaoSample.getSampleByCancerStudyAndSampleId(studyId, "TCGA-A1-A0SE-01").getInternalId();
        sample4 = DaoSample.getSampleByCancerStudyAndSampleId(studyId, "TCGA-A1-A0SF-01").getInternalId();
        sample5 = DaoSample.getSampleByCancerStudyAndSampleId(studyId, "TCGA-A1-A0SG-01").getInternalId();
    }

    /**
     * Test importing of cna_test.txt file.
     * @throws Exception All Errors.
     */
	@Test
    public void testImportCnaDataBulkLoadOff() throws Exception {

        ClickHouseBulkLoader.bulkLoadOff();
        runImportCnaData();
    }
    
    /**
     * Test importing of cna_test.txt file.
     * @throws Exception All Errors.
     */
	@Test
    public void testImportCnaDataBulkLoadOn() throws Exception {
		ClickHouseBulkLoader.bulkLoadOn();
        runImportCnaData();
    }
    
    private void runImportCnaData() throws Exception {

        DaoGeneticAlteration dao = DaoGeneticAlteration.getInstance();
        DaoGeneOptimized daoGene = DaoGeneOptimized.getInstance();

        // the largest current true Entrez gene ID counts 8 digits
        daoGene.addGene(new CanonicalGene(999999207, "TESTAKT1"));
        daoGene.addGene(new CanonicalGene(999999208, "TESTAKT2"));
        daoGene.addGene(new CanonicalGene(999910000, "TESTAKT3"));
        daoGene.addGene(new CanonicalGene(999999369, "TESTARAF"));
        daoGene.addGene(new CanonicalGene(999999472, "TESTATM"));
        daoGene.addGene(new CanonicalGene(999999673, "TESTBRAF"));
        daoGene.addGene(new CanonicalGene(999999672, "TESTBRCA1"));
        daoGene.addGene(new CanonicalGene(999999675, "TESTBRCA2"));

        ProgressMonitor.setConsoleMode(false);
		// TBD: change this to use getResourceAsStream()
        File file = new File("src/test/resources/cna_test.txt");
        ImportTabDelimData parser = new ImportTabDelimData(file, "Barry", geneticProfileId, null, false, DaoGeneOptimized.getInstance());
        parser.importData();

        String value = dao.getGeneticAlteration(geneticProfileId, sample1, 999999207);
        assertEquals ("0", value);
        value = dao.getGeneticAlteration(geneticProfileId, sample4, 999999207);
        assertEquals ("-1", value);
        value = dao.getGeneticAlteration(geneticProfileId, sample2, 999999207);
        assertEquals ("0", value);
        value = dao.getGeneticAlteration(geneticProfileId, sample2, 999910000);
        assertEquals ("2", value);
        value = dao.getGeneticAlteration(geneticProfileId, sample3, 999910000);
        assertEquals ("2", value);

        int cnaStatus = Integer.parseInt(dao.getGeneticAlteration(geneticProfileId, sample3, 999910000));
        assertEquals(CopyNumberStatus.COPY_NUMBER_AMPLIFICATION, cnaStatus);
        cnaStatus = Integer.parseInt(dao.getGeneticAlteration(geneticProfileId, sample2, 999910000));
        assertEquals(CopyNumberStatus.COPY_NUMBER_AMPLIFICATION, cnaStatus);
        cnaStatus = Integer.parseInt(dao.getGeneticAlteration(geneticProfileId, sample4, 999999207));
        assertEquals(CopyNumberStatus.HEMIZYGOUS_DELETION, cnaStatus);

        Patient patient = DaoPatient.getPatientByCancerStudyAndPatientId(studyId, "TCGA-A1-A0SB");
        Sample sample = DaoSample.getSampleByPatientAndSampleId(patient.getInternalId(), "TCGA-A1-A0SB-01");
        assertTrue(DaoSampleProfile.sampleExistsInGeneticProfile(sample.getInternalId(), geneticProfileId));
 
        patient = DaoPatient.getPatientByCancerStudyAndPatientId(studyId, "TCGA-A1-A0SJ");
        sample = DaoSample.getSampleByPatientAndSampleId(patient.getInternalId(), "TCGA-A1-A0SJ-01");
        assertTrue(DaoSampleProfile.sampleExistsInGeneticProfile(sample.getInternalId(), geneticProfileId));

        ArrayList caseIds = DaoSampleProfile.getAllSampleIdsInProfile(geneticProfileId);
        assertEquals(14, caseIds.size());
    }

    /**
     * Test importing of cna_test2.txt file.
     * This is identical to cna_test.txt, except there is no target line.
     * @throws Exception All Errors.
     */
    @Test
    public void testImportCnaData2BulkLoadOff() throws Exception {
        // test with both values of ClickHouseBulkLoader.isBulkLoad()
        ClickHouseBulkLoader.bulkLoadOff();
        runImportCnaData2();
    }
    
    /**
     * Test importing of cna_test2.txt file.
     * This is identical to cna_test.txt, except there is no target line.
     * @throws Exception All Errors.
     */
    @Test
    public void testImportCnaData2BulkLoadOn() throws Exception {
        // test with both values of ClickHouseBulkLoader.isBulkLoad()
    	ClickHouseBulkLoader.bulkLoadOn();
        runImportCnaData2();
    }
    
    private void runImportCnaData2() throws Exception {

        DaoGeneticAlteration dao = DaoGeneticAlteration.getInstance();

        ProgressMonitor.setConsoleMode(false);
		// TBD: change this to use getResourceAsStream()
        File file = new File("src/test/resources/cna_test2.txt");
        ImportTabDelimData parser = new ImportTabDelimData(file, geneticProfileId, null, false, DaoGeneOptimized.getInstance());
        parser.importData();

        String value = dao.getGeneticAlteration(geneticProfileId, sample1, 207);
        assertEquals (value, "0");
        value = dao.getGeneticAlteration(geneticProfileId, sample4, 207);
        assertEquals (value, "-1");
        value = dao.getGeneticAlteration(geneticProfileId, sample2, 207);
        assertEquals (value, "0");
        value = dao.getGeneticAlteration(geneticProfileId, sample2, 10000);
        assertEquals (value, "2");
        value = dao.getGeneticAlteration(geneticProfileId, sample3, 10000);
        assertEquals (value, "2");

        int cnaStatus = Integer.parseInt(dao.getGeneticAlteration(geneticProfileId, sample3, 10000));
        assertEquals(CopyNumberStatus.COPY_NUMBER_AMPLIFICATION, cnaStatus);
        cnaStatus = Integer.parseInt(dao.getGeneticAlteration(geneticProfileId, sample2, 10000));
        assertEquals(CopyNumberStatus.COPY_NUMBER_AMPLIFICATION, cnaStatus);
        cnaStatus = Integer.parseInt(dao.getGeneticAlteration(geneticProfileId, sample4, 207));
        assertEquals(CopyNumberStatus.HEMIZYGOUS_DELETION, cnaStatus);

        Patient patient = DaoPatient.getPatientByCancerStudyAndPatientId(studyId, "TCGA-A1-A0SB");
        Sample sample = DaoSample.getSampleByPatientAndSampleId(patient.getInternalId(), "TCGA-A1-A0SB-01");
        assertTrue(DaoSampleProfile.sampleExistsInGeneticProfile(sample.getInternalId(), geneticProfileId));

        patient = DaoPatient.getPatientByCancerStudyAndPatientId(studyId, "TCGA-A1-A0SJ");
        sample = DaoSample.getSampleByPatientAndSampleId(patient.getInternalId(), "TCGA-A1-A0SJ-01");
        assertTrue(DaoSampleProfile.sampleExistsInGeneticProfile(sample.getInternalId(), geneticProfileId));
        ArrayList sampleIds = DaoSampleProfile.getAllSampleIdsInProfile(geneticProfileId);
        assertEquals(14, sampleIds.size());
    }

    /**
     * Test importing of mrna_test file.
     * @throws Exception All Errors.
     */
    @Test
    public void testImportmRnaData1BulkLoadOff() throws Exception {
        // test with both values of ClickHouseBulkLoader.isBulkLoad()
        ClickHouseBulkLoader.bulkLoadOff();
        runImportRnaData1();
    }
    
    /**
     * Test importing of mrna_test file.
     * @throws Exception All Errors.
     */
    @Test
    public void testImportmRnaData1BulkLoadOn() throws Exception {
        // test with both values of ClickHouseBulkLoader.isBulkLoad()
      	ClickHouseBulkLoader.bulkLoadOn();
        runImportRnaData1();
    }
    
    private void runImportRnaData1() throws Exception {

        DaoGeneOptimized daoGene = DaoGeneOptimized.getInstance();
        DaoGeneticAlteration dao = DaoGeneticAlteration.getInstance();

        daoGene.addGene(new CanonicalGene(999999780, "A"));
        daoGene.addGene(new CanonicalGene(999995982, "B"));
        daoGene.addGene(new CanonicalGene(999993310, "C"));
        daoGene.addGene(new CanonicalGene(999997849, "D"));
        daoGene.addGene(new CanonicalGene(999992978, "E"));
        daoGene.addGene(new CanonicalGene(999997067, "F"));
        daoGene.addGene(new CanonicalGene(999911099, "G"));
        daoGene.addGene(new CanonicalGene(999999675, "6352"));

        GeneticProfile geneticProfile = new GeneticProfile();

        geneticProfile.setCancerStudyId(studyId);
        geneticProfile.setStableId("gbm_mrna");
        geneticProfile.setGeneticAlterationType(GeneticAlterationType.MRNA_EXPRESSION);
        geneticProfile.setDatatype("CONTINUOUS");
        geneticProfile.setProfileName("MRNA Data");
        geneticProfile.setProfileDescription("mRNA Data");
        DaoGeneticProfile.addGeneticProfile(geneticProfile);
        
        int newGeneticProfileId = DaoGeneticProfile.getGeneticProfileByStableId("gbm_mrna").getGeneticProfileId();

        ProgressMonitor.setConsoleMode(true);
		// TBD: change this to use getResourceAsStream()
        File file = new File("src/test/resources/mrna_test.txt");
        addTestPatientAndSampleRecords(file);
        ImportTabDelimData parser = new ImportTabDelimData(file, newGeneticProfileId, null, false, DaoGeneOptimized.getInstance());
        parser.importData();
        ConsoleUtil.showMessages();
        
        int sampleId = DaoSample.getSampleByCancerStudyAndSampleId(studyId, "DD639").getInternalId();
        String value = dao.getGeneticAlteration(newGeneticProfileId, sampleId, 999992978);
        assertEquals ("2.01", value );

        sampleId = DaoSample.getSampleByCancerStudyAndSampleId(studyId, "DD638").getInternalId();
        value = dao.getGeneticAlteration(newGeneticProfileId, sampleId, 999997849);
        assertEquals ("0.55", value );
    }

    
    /**
     * Test importing of data_expression file.
     * @throws Exception All Errors.
     */
    @Test
    public void testImportmRnaData2() throws Exception {
       	ClickHouseBulkLoader.bulkLoadOn();
        

        DaoGeneOptimized daoGene = DaoGeneOptimized.getInstance();
        DaoGeneticAlteration dao = DaoGeneticAlteration.getInstance();

        //Gene with alias:
        daoGene.addGene(makeGeneWithAlias(999997504, "TESTXK", "NA"));
        //Other genes:
        daoGene.addGene(new CanonicalGene(999999999, "TESTNAT1"));

        daoGene.addGene(new CanonicalGene(999997124, "TESTTNF"));
        daoGene.addGene(new CanonicalGene(999991111, "TESTCHEK1"));
        daoGene.addGene(new CanonicalGene(999999919, "TESTABCA1"));
        // will get generated negative id:
        daoGene.addGene(new CanonicalGene(-1, "TESTphosphoprotein"));
        		
        GeneticProfile geneticProfile = new GeneticProfile();

        geneticProfile.setCancerStudyId(studyId);
        geneticProfile.setStableId("gbm_mrna");
        geneticProfile.setGeneticAlterationType(GeneticAlterationType.MRNA_EXPRESSION);
        geneticProfile.setDatatype("CONTINUOUS");
        geneticProfile.setProfileName("MRNA Data");
        geneticProfile.setProfileDescription("mRNA Data");
        DaoGeneticProfile.addGeneticProfile(geneticProfile);
        
        int newGeneticProfileId = DaoGeneticProfile.getGeneticProfileByStableId("gbm_mrna").getGeneticProfileId();

        ProgressMonitor.setConsoleMode(true);
		// TBD: change this to use getResourceAsStream()
        File file = new File("src/test/resources/tabDelimitedData/data_expression2.txt");
        addTestPatientAndSampleRecords(file);
        ImportTabDelimData parser = new ImportTabDelimData(file, newGeneticProfileId, null, false, DaoGeneOptimized.getInstance());
        parser.importData();
        
        // check if expected warnings are given:
        ArrayList<String> warnings = ProgressMonitor.getWarnings();
        int countDuplicatedRowWarnings = 0;
        int countInvalidEntrez = 0;
        int countSkippedWarnings = 0;
        for (String warning: warnings) {
            if (warning.contains("Duplicated row")) {
                countDuplicatedRowWarnings++;
            }
            if (warning.contains("invalid Entrez_Id")) {
                //invalid Entrez
                countInvalidEntrez++;
            }
            if (warning.contains("Record will be skipped")) {
                //Entrez is a valid number, but not found
                countSkippedWarnings++;
            }
        }
        //check that we have 11 warning messages:
        assertEquals(4, countDuplicatedRowWarnings);
        assertEquals(3, countInvalidEntrez);
        assertEquals(4, countSkippedWarnings);
        
        Set<Integer> geneticEntityIds = DaoGeneticAlteration.getEntityIdsInProfile(newGeneticProfileId);
        // data will be loaded for 5 of the genes
        assertEquals(5, geneticEntityIds.size());
        HashMap<Integer, HashMap<Integer, String>> dataMap = dao.getGeneticAlterationMapForEntityIds(newGeneticProfileId, geneticEntityIds);
        assertEquals(5, dataMap.entrySet().size());
        
        int sampleId = DaoSample.getSampleByCancerStudyAndSampleId(studyId, "SAMPLE1").getInternalId();
        String value = dao.getGeneticAlteration(newGeneticProfileId, sampleId, 999997124);
        assertEquals ("770", value );
        
        sampleId = DaoSample.getSampleByCancerStudyAndSampleId(studyId, "SAMPLE3").getInternalId();
        value = dao.getGeneticAlteration(newGeneticProfileId, sampleId, 999997124);
        assertEquals ("220", value );

        //gene should also be loaded via its alias "NA" as defined above:
        sampleId = DaoSample.getSampleByCancerStudyAndSampleId(studyId, "SAMPLE3").getInternalId();
        value = dao.getGeneticAlteration(newGeneticProfileId, sampleId, 999997504);
        assertEquals ("9940", value );
    }
    
    
    /**
     * Test importing of data_rppa file.
     * @throws Exception All Errors.
     */
    @Test
    public void testImportRppaData() throws Exception {
       	ClickHouseBulkLoader.bulkLoadOn();
        
        DaoGeneOptimized daoGene = DaoGeneOptimized.getInstance();
        DaoGeneticAlteration dao = DaoGeneticAlteration.getInstance();

        //Genes with alias:
        daoGene.addGene(makeGeneWithAlias(999999931,"TESTACACA", "TESTACC1"));
        daoGene.addGene(makeGeneWithAlias(999999207,"TESTAKT1", "TESTAKT"));
        daoGene.addGene(makeGeneWithAlias(999999597,"TESTSANDER", "TESTACC1"));
        daoGene.addGene(makeGeneWithAlias(999997158,"TESTTP53BP1", "TEST53BP1"));
        // test for NA being a special case in RPPA, and not the usual alias
        daoGene.addGene(makeGeneWithAlias(999997504, "XK", "NA"));
        //Other genes:
        daoGene.addGene(new CanonicalGene(999999932,"TESTACACB"));
        daoGene.addGene(new CanonicalGene(999999208,"TESTAKT2"));
        daoGene.addGene(new CanonicalGene(999999369,"TESTARAF"));
        daoGene.addGene(new CanonicalGene(999991978, "TESTEIF4EBP1"));
        daoGene.addGene(new CanonicalGene(999995562,"TESTPRKAA1"));
        daoGene.addGene(new CanonicalGene(999997531,"TESTYWHAE"));
        daoGene.addGene(new CanonicalGene(999910000,"TESTAKT3"));
        daoGene.addGene(new CanonicalGene(999995578,"TESTPRKCA"));
        
        
        GeneticProfile geneticProfile = new GeneticProfile();

        geneticProfile.setCancerStudyId(studyId);
        geneticProfile.setStableId("gbm_rppa");
        geneticProfile.setGeneticAlterationType(GeneticAlterationType.PROTEIN_LEVEL);
        geneticProfile.setDatatype("LOG2-VALUE");
        geneticProfile.setProfileName("RPPA Data");
        geneticProfile.setProfileDescription("RPPA Data");
        DaoGeneticProfile.addGeneticProfile(geneticProfile);
        
        int newGeneticProfileId = DaoGeneticProfile.getGeneticProfileByStableId("gbm_rppa").getGeneticProfileId();

        ProgressMonitor.setConsoleMode(true);
		// TBD: change this to use getResourceAsStream()
        File file = new File("src/test/resources/tabDelimitedData/data_rppa.txt");
        addTestPatientAndSampleRecords(file);
        ImportTabDelimData parser = new ImportTabDelimData(file, newGeneticProfileId, null, false, DaoGeneOptimized.getInstance());
        parser.importData();
        ConsoleUtil.showMessages();
        
        int sampleId = DaoSample.getSampleByCancerStudyAndSampleId(studyId, "SAMPLE1").getInternalId();
        String value = dao.getGeneticAlteration(newGeneticProfileId, sampleId, 999997531);
        assertEquals ("1.5", value );
        
        sampleId = DaoSample.getSampleByCancerStudyAndSampleId(studyId, "SAMPLE4").getInternalId();
        value = dao.getGeneticAlteration(newGeneticProfileId, sampleId, 999997531);
        assertEquals ("2", value );
        
        sampleId = DaoSample.getSampleByCancerStudyAndSampleId(studyId, "SAMPLE4").getInternalId();
        value = dao.getGeneticAlteration(newGeneticProfileId, sampleId, 999997504);
        assertEquals ("NaN", value ); //"NA" is not expected to be stored because of workaround for bug in firehose. See also https://github.com/cBioPortal/cbioportal/issues/839#issuecomment-203523078
        
        sampleId = DaoSample.getSampleByCancerStudyAndSampleId(studyId, "SAMPLE1").getInternalId();
        value = dao.getGeneticAlteration(newGeneticProfileId, sampleId, 999995578);
        assertEquals ("1.5", value );
    }

	private CanonicalGene makeGeneWithAlias(int entrez, String symbol, String alias) {
		CanonicalGene gene = new CanonicalGene(entrez, symbol);
        Set<String> aliases = new HashSet<String>();
        aliases.add(alias);
        gene.setAliases(aliases);
        return gene;
	}

    private void addTestPatientAndSampleRecords(File file) throws FileNotFoundException, IOException, DaoException {
        // extract sample ids from header
        FileReader reader = new FileReader(file);
        BufferedReader buf = new BufferedReader(reader);
        String headerLine = buf.readLine();
        String parts[] = headerLine.split("\t");
        List<String> sampleIds = new ArrayList<>();
        for (int i=0; i<parts.length; i++) {
            boolean isSample = Boolean.TRUE;
            for (String nonCaseIdCol : NON_CASE_ID_COLS) {
                if (nonCaseIdCol.equalsIgnoreCase(parts[i])) {
                    isSample = Boolean.FALSE;
                    break;
                }
            }
            if (isSample) {
                sampleIds.add(parts[i]);
            }
        }
        reader.close();
        // add sample + patient records to db
        for (String sampleId : sampleIds) {
            // fetch patient from db or add new one if does not exist
            Patient p = DaoPatient.getPatientByCancerStudyAndPatientId(studyId, sampleId);
            Integer pId = (p == null) ? DaoPatient.addPatient(new Patient(study, sampleId)) : p.getInternalId();
            DaoSample.addSample(new Sample(sampleId, pId, study.getTypeOfCancerId()));
        }
        ClickHouseBulkLoader.flushAll();
    }

    /**
     * In no-explode mode the real importer writes exploded rows straight into
     * genetic_alteration_derived (no packed `values`, no ARRAY JOIN derive). Verify the
     * derived rows match the input matrix and that the packed table stays empty.
     */
    @Test
    public void testImportCnaDataNoExplode() throws Exception {
        ClickHouseBulkLoader.bulkLoadOn();
        ClickHouseBulkLoader.noExplodeOn();
        try {
            DaoGeneOptimized daoGene = DaoGeneOptimized.getInstance();
            daoGene.addGene(new CanonicalGene(999999207, "TESTAKT1"));
            daoGene.addGene(new CanonicalGene(999999208, "TESTAKT2"));
            daoGene.addGene(new CanonicalGene(999910000, "TESTAKT3"));
            daoGene.addGene(new CanonicalGene(999999369, "TESTARAF"));
            daoGene.addGene(new CanonicalGene(999999472, "TESTATM"));
            daoGene.addGene(new CanonicalGene(999999673, "TESTBRAF"));
            daoGene.addGene(new CanonicalGene(999999672, "TESTBRCA1"));
            daoGene.addGene(new CanonicalGene(999999675, "TESTBRCA2"));

            ProgressMonitor.setConsoleMode(false);
            File file = new File("src/test/resources/cna_test.txt");
            ImportTabDelimData parser = new ImportTabDelimData(file, "Barry", geneticProfileId, null, false, DaoGeneOptimized.getInstance());
            parser.importData();

            // derived rows: sample_unique_id = <study>_<sample>, profile_type = stableId minus study prefix ("test")
            String p = "study_tcga_pub_";
            assertEquals("0",  derivedValue(p + "TCGA-A1-A0SB-01", "TESTAKT1", "test"));
            assertEquals("-1", derivedValue(p + "TCGA-A1-A0SF-01", "TESTAKT1", "test"));
            assertEquals("0",  derivedValue(p + "TCGA-A1-A0SD-01", "TESTAKT1", "test"));
            assertEquals("2",  derivedValue(p + "TCGA-A1-A0SD-01", "TESTAKT3", "test"));
            assertEquals("2",  derivedValue(p + "TCGA-A1-A0SE-01", "TESTAKT3", "test"));

            // packed genetic_alteration must stay empty for this profile in no-explode mode
            assertEquals(0, packedRowCount(geneticProfileId));
        } finally {
            ClickHouseBulkLoader.noExplodeOff();
        }
    }

    private String derivedValue(String sampleUniqueId, String hugo, String profileType) throws Exception {
        Connection con = null; PreparedStatement ps = null; ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(TestImportTabDelimData.class);
            ps = con.prepareStatement("SELECT alteration_value FROM genetic_alteration_derived "
                + "WHERE sample_unique_id = ? AND hugo_gene_symbol = ? AND profile_type = ?");
            ps.setString(1, sampleUniqueId);
            ps.setString(2, hugo);
            ps.setString(3, profileType);
            rs = ps.executeQuery();
            return rs.next() ? rs.getString(1) : null;
        } finally {
            JdbcUtil.closeAll(TestImportTabDelimData.class, con, ps, rs);
        }
    }

    private int packedRowCount(int geneticProfileId) throws Exception {
        Connection con = null; PreparedStatement ps = null; ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(TestImportTabDelimData.class);
            ps = con.prepareStatement("SELECT count() FROM genetic_alteration WHERE genetic_profile_id = ?");
            ps.setInt(1, geneticProfileId);
            rs = ps.executeQuery();
            return rs.next() ? rs.getInt(1) : -1;
        } finally {
            JdbcUtil.closeAll(TestImportTabDelimData.class, con, ps, rs);
        }
    }

    /**
     * Performance benchmark of the REAL importer: legacy (packed genetic_alteration + ARRAY JOIN
     * derive) vs. no-explode (direct write to genetic_alteration_derived), on a synthetic large
     * matrix loaded twice into the same testcontainers ClickHouse. Run explicitly, e.g.:
     *   mvn test -Dtest=TestImportTabDelimData#benchmarkNoExplodeVsLegacy -Dbench.genes=8000 -Dbench.samples=1000
     */
    @Test
    public void benchmarkNoExplodeVsLegacy() throws Exception {
        // Performance benchmark (not a CI assertion). Run explicitly with -Dbench.run=true.
        org.junit.Assume.assumeTrue("performance benchmark; run with -Dbench.run=true", Boolean.getBoolean("bench.run"));
        final int nGenes = Integer.getInteger("bench.genes", 8000);
        final int nSamples = Integer.getInteger("bench.samples", 1000);
        ProgressMonitor.setConsoleMode(false);

        // --- setup: samples + genes under study_tcga_pub ---
        ClickHouseBulkLoader.bulkLoadOn();
        ClickHouseBulkLoader.noExplodeOff();
        DaoGeneOptimized daoGene = DaoGeneOptimized.getInstance();
        String[] sampleStableIds = new String[nSamples];
        for (int i = 0; i < nSamples; i++) {
            String sid = String.format("BENCH-%05d", i);
            sampleStableIds[i] = sid;
            int pid = DaoPatient.addPatient(new Patient(study, sid));
            DaoSample.addSample(new Sample(sid, pid, study.getTypeOfCancerId()));
        }
        for (int g = 0; g < nGenes; g++) {
            daoGene.addGene(new CanonicalGene(800000000L + g, "BENCHGENE" + g));
        }
        ClickHouseBulkLoader.flushAll();

        int legacyId = addBenchProfile("study_tcga_pub_benchlegacy");
        int noExplodeId = addBenchProfile("study_tcga_pub_benchnoexplode");
        File matrix = generateBenchMatrix(sampleStableIds, nGenes);
        long cells = (long) nGenes * nSamples;

        // --- LEGACY: real importer writes packed, then ARRAY JOIN derive ---
        ClickHouseBulkLoader.bulkLoadOn();
        ClickHouseBulkLoader.noExplodeOff();
        long t0 = System.nanoTime();
        new ImportTabDelimData(matrix, legacyId, null, false, DaoGeneOptimized.getInstance()).importData();
        double secPacked = nanosToSec(System.nanoTime() - t0);
        long t1 = System.nanoTime();
        runDerive(legacyId);
        double secDerive = nanosToSec(System.nanoTime() - t1);

        // --- NO-EXPLODE: real importer writes genetic_alteration_derived directly ---
        ClickHouseBulkLoader.bulkLoadOn();
        ClickHouseBulkLoader.noExplodeOn();
        long t2 = System.nanoTime();
        try {
            new ImportTabDelimData(matrix, noExplodeId, null, false, DaoGeneOptimized.getInstance()).importData();
        } finally {
            ClickHouseBulkLoader.noExplodeOff();
        }
        double secNoExplode = nanosToSec(System.nanoTime() - t2);

        // --- correctness + sizes ---
        String[] legacy = derivedStats("benchlegacy");
        String[] noexp = derivedStats("benchnoexplode");
        boolean match = legacy[0].equals(noexp[0]) && legacy[1].equals(noexp[1]);
        long packedBytes = tableBytes("genetic_alteration");
        long derivedBytes = tableBytes("genetic_alteration_derived");
        long deriveMemMiB = deriveServerPeakMiB();

        System.out.println("\nBENCH_RESULT ================================================");
        System.out.printf("BENCH_RESULT study=ccle-like genes=%d samples=%d cells=%,d%n", nGenes, nSamples, cells);
        System.out.printf("BENCH_RESULT legacy:     packed_import=%.2fs  derive=%.2fs  total=%.2fs%n",
            secPacked, secDerive, secPacked + secDerive);
        System.out.printf("BENCH_RESULT no_explode: direct_import=%.2fs  total=%.2fs%n", secNoExplode, secNoExplode);
        System.out.printf("BENCH_RESULT derive_server_peak=%dMiB%n", deriveMemMiB);
        System.out.printf("BENCH_RESULT storage: packed=%.1fMiB derived(all)=%.1fMiB%n",
            packedBytes / 1048576.0, derivedBytes / 1048576.0);
        System.out.printf("BENCH_RESULT correctness: legacy rows/chk=%s/%s  noexplode rows/chk=%s/%s  MATCH=%b%n",
            legacy[0], legacy[1], noexp[0], noexp[1], match);
        System.out.println("BENCH_RESULT ================================================\n");
        assertTrue("no-explode output must match the derive", match);
    }

    private int addBenchProfile(String stableId) throws DaoException {
        GeneticProfile gp = new GeneticProfile();
        gp.setCancerStudyId(studyId);
        gp.setStableId(stableId);
        gp.setGeneticAlterationType(GeneticAlterationType.MRNA_EXPRESSION);
        gp.setDatatype("CONTINUOUS");
        gp.setProfileName(stableId);
        DaoGeneticProfile.addGeneticProfile(gp);
        return DaoGeneticProfile.getGeneticProfileByStableId(stableId).getGeneticProfileId();
    }

    private File generateBenchMatrix(String[] sampleStableIds, int nGenes) throws Exception {
        File f = File.createTempFile("bench_matrix_", ".txt");
        f.deleteOnExit();
        try (BufferedWriter w = new BufferedWriter(new FileWriter(f), 1 << 20)) {
            w.write("Hugo_Symbol");
            for (String s : sampleStableIds) { w.write('\t'); w.write(s); }
            w.write('\n');
            for (int g = 0; g < nGenes; g++) {
                w.write("BENCHGENE" + g);
                for (int s = 0; s < sampleStableIds.length; s++) {
                    w.write('\t');
                    w.write(Integer.toString(((g * 7 + s * 3) % 5) - 2));
                }
                w.write('\n');
            }
        }
        return f;
    }

    private void runDerive(int geneticProfileId) throws Exception {
        // Mirrors clickhouse.sql genetic_alteration_derived build, scoped to one profile, joining
        // base tables (no sample_derived) to compute sample_unique_id = <study>_<sample>.
        String sql = """
            INSERT INTO genetic_alteration_derived
            SELECT concat(sub.csi, '_', s.stable_id) AS sample_unique_id,
                   sub.csi AS cancer_study_identifier,
                   sub.hugo AS hugo_gene_symbol,
                   replaceOne(sub.stable_id, concat(sub.csi, '_'), '') AS profile_type,
                   sub.av AS alteration_value
            FROM (
                SELECT hugo, stable_id, csi, av, isid
                FROM (
                    SELECT g.hugo_gene_symbol AS hugo, gp.stable_id AS stable_id,
                           cs.cancer_study_identifier AS csi,
                           arrayMap(x -> (x = '' ? NULL : x), splitByString(',', assumeNotNull(substring(ga.`values`, 1, -1)))) AS av,
                           arrayMap(x -> toInt64(x), splitByString(',', assumeNotNull(substring(gps.ordered_sample_list, 1, -1)))) AS isid
                    FROM genetic_alteration ga
                    JOIN genetic_profile gp ON ga.genetic_profile_id = gp.genetic_profile_id
                    JOIN cancer_study cs ON gp.cancer_study_id = cs.cancer_study_id
                    JOIN gene g ON ga.genetic_entity_id = g.genetic_entity_id
                    JOIN genetic_profile_samples gps ON gps.genetic_profile_id = gp.genetic_profile_id
                    WHERE gp.genetic_profile_id = %d
                ) ARRAY JOIN av, isid
                WHERE av != 'NA'
            ) AS sub
            JOIN sample s ON s.internal_id = sub.isid
            SETTINGS log_comment = 'bench_derive'
            """.formatted(geneticProfileId);
        Connection con = null; PreparedStatement st = null;
        try {
            con = JdbcUtil.getDbConnection(TestImportTabDelimData.class);
            st = con.prepareStatement(sql);
            st.execute();
        } finally {
            JdbcUtil.closeAll(TestImportTabDelimData.class, con, st, null);
        }
    }

    private String[] derivedStats(String profileType) throws Exception {
        Connection con = null; PreparedStatement ps = null; ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(TestImportTabDelimData.class);
            ps = con.prepareStatement("SELECT count(), sum(cityHash64(sample_unique_id, hugo_gene_symbol, "
                + "ifNull(alteration_value,'N'))) FROM genetic_alteration_derived WHERE profile_type = ?");
            ps.setString(1, profileType);
            rs = ps.executeQuery();
            rs.next();
            return new String[]{rs.getString(1), rs.getString(2)};
        } finally {
            JdbcUtil.closeAll(TestImportTabDelimData.class, con, ps, rs);
        }
    }

    private long tableBytes(String table) throws Exception {
        Connection con = null; PreparedStatement opt = null; PreparedStatement sel = null; ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(TestImportTabDelimData.class);
            opt = con.prepareStatement("OPTIMIZE TABLE " + table + " FINAL");
            opt.execute();
            opt.close();
            sel = con.prepareStatement("SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND table = ?");
            sel.setString(1, table);
            rs = sel.executeQuery();
            return rs.next() ? rs.getLong(1) : -1;
        } catch (Exception e) {
            return -1;
        } finally {
            JdbcUtil.closeAll(TestImportTabDelimData.class, con, sel, rs);
        }
    }

    private long deriveServerPeakMiB() {
        Connection con = null; PreparedStatement flush = null; PreparedStatement sel = null; ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(TestImportTabDelimData.class);
            flush = con.prepareStatement("SYSTEM FLUSH LOGS");
            flush.execute();
            flush.close();
            sel = con.prepareStatement("SELECT max(memory_usage) FROM system.query_log "
                + "WHERE log_comment = 'bench_derive' AND type = 'QueryFinish'");
            rs = sel.executeQuery();
            return rs.next() ? rs.getLong(1) / 1048576 : -1;
        } catch (Exception e) {
            return -1;
        } finally {
            JdbcUtil.closeAll(TestImportTabDelimData.class, con, sel, rs);
        }
    }

    private static double nanosToSec(long nanos) {
        return nanos / 1_000_000_000.0;
    }
}
