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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mskcc.cbio.portal.dao.ClickHouseBulkLoader;
import org.mskcc.cbio.portal.dao.DaoCancerStudy;
import org.mskcc.cbio.portal.dao.DaoGeneOptimized;
import org.mskcc.cbio.portal.dao.DaoGeneticAlteration;
import org.mskcc.cbio.portal.dao.DaoGeneticProfile;
import org.mskcc.cbio.portal.dao.DaoMutation;
import org.mskcc.cbio.portal.dao.DaoSample;
import org.mskcc.cbio.portal.dao.DaoSampleList;
import org.mskcc.cbio.portal.dao.DaoStructuralVariant;
import org.mskcc.cbio.portal.integrationTest.IntegrationTestBase;
import org.mskcc.cbio.portal.model.CancerStudy;
import org.mskcc.cbio.portal.model.GeneticProfile;
import org.mskcc.cbio.portal.scripts.ImportGenesetData;
import org.mskcc.cbio.portal.scripts.ImportStepRunner;
import org.mskcc.cbio.portal.util.ProgressMonitor;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Runs the import steps cbioportalImporter.py writes for study_es_0 in one JVM
 * with {@link ImportStepRunner}.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:/applicationContext-dao.xml" })
public class TestImportStepRunner extends IntegrationTestBase {

    // written by cbioportalImporter.py for --study_directory tests/test_data/study_es_0
    private static final File STUDY_ES_0_STEPS = new File("src/test/resources/import_step_runner/study_es_0_steps.json");

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private final ObjectMapper mapper = new ObjectMapper();

    @Before
    public void setUp() throws Exception {
        ProgressMonitor.setConsoleMode(true);
        ProgressMonitor.resetWarnings();
        DaoCancerStudy.reCacheAll();
        DaoGeneOptimized.getInstance().reCache();
        TestIntegrationTest.loadGenes();
        TestIntegrationTest.loadGenePanel();
        // the GSVA steps need these gene sets, which a study import does not load
        int status = new ImportGenesetData(new String[] {
            "--data", "src/test/resources/genesets/study_es_0_genesets.gmt",
            "--new-version", "msigdb_7.5.1",
            "--confirm-delete-all-genesets-hierarchy-genesetprofiles"}).runInProcess();
        assertEquals(0, status);
    }

    @Test
    public void testStudyEs0LoadsInOneJvm() throws Exception {
        File result = temporaryFolder.newFile("result.json");
        int mutationsBefore = DaoMutation.getAllMutations().size();

        int status = ImportStepRunner.run(new String[] {
            "--manifest", studyEs0Manifest().getPath(), "--result", result.getPath()});

        assertEquals(0, status);
        assertEquals(Map.of("status", 0), withoutNulls(readResult(result)));
        assertStudyEs0Loaded(mutationsBefore);
    }

    @Test
    public void testStudyEs0LoadsTwiceInTheSameJvm() throws Exception {
        int mutationsBefore = DaoMutation.getAllMutations().size();
        String[] args = {"--manifest", studyEs0Manifest().getPath()};

        assertEquals(0, ImportStepRunner.run(args));
        // the second run removes the study and imports it again, using caches left by the first
        assertEquals(0, ImportStepRunner.run(args));

        assertStudyEs0Loaded(mutationsBefore);
    }

    @Test
    public void testFailedStepStopsTheRun() throws Exception {
        List<Map<String, Object>> steps = studyEs0Steps();
        // keep up to and including the sample clinical data, then add a step that fails
        int clinicalSamplesStep = indexOfStepWithArg(steps, "tests/test_data/study_es_0/data_clinical_samples.txt");
        List<Map<String, Object>> failingSteps = new java.util.ArrayList<>(steps.subList(0, clinicalSamplesStep + 1));
        failingSteps.add(step("org.mskcc.cbio.portal.scripts.ImportClinicalData",
            "--meta", "tests/test_data/study_es_0/meta_clinical_patients.txt",
            "--loadMode", "bulkload",
            "--data", "tests/test_data/study_es_0/no_such_file.txt",
            "--noprogress"));
        failingSteps.add(step("org.mskcc.cbio.portal.scripts.AddCaseList", "study_es_0", "all", "--noprogress"));
        File manifest = writeManifest(failingSteps);
        File result = temporaryFolder.newFile("result.json");

        int status = ImportStepRunner.run(new String[] {
            "--manifest", manifest.getPath(), "--result", result.getPath()});

        assertFalse(status == 0);
        Map<String, Object> resultJson = readResult(result);
        assertEquals(status, resultJson.get("status"));
        assertEquals(clinicalSamplesStep + 1, resultJson.get("failed_step"));
        // the step after the failure did not run
        assertNull(new DaoSampleList().getSampleListByStableId("study_es_0_all"));
        // and the failed step's bulk load state was reset
        assertFalse(ClickHouseBulkLoader.isBulkLoad());

        // the JVM can still run the whole study afterwards
        int mutationsBefore = DaoMutation.getAllMutations().size();
        assertEquals(0, ImportStepRunner.run(new String[] {"--manifest", studyEs0Manifest().getPath()}));
        assertStudyEs0Loaded(mutationsBefore);
    }

    @Test
    public void testStepAfterRemoveSamplesSeesReloadedCaches() throws Exception {
        assertEquals(0, ImportStepRunner.run(new String[] {"--manifest", studyEs0Manifest().getPath()}));
        CancerStudy cancerStudy = DaoCancerStudy.getCancerStudyByStableId("study_es_0");
        int samplesBefore = DaoSample.getSamplesByCancerStudy(cancerStudy.getInternalId()).size();

        // RemoveSamples empties the sample cache; AddCaseList needs it reloaded to find the remaining samples
        File manifest = writeManifest(List.of(
            step("org.mskcc.cbio.portal.scripts.RemoveSamples",
                "--study_ids", "study_es_0", "--sample_ids", "TCGA-A2-A04P-01"),
            step("org.mskcc.cbio.portal.scripts.AddCaseList", "study_es_0", "all", "--noprogress")));
        assertEquals(0, ImportStepRunner.run(new String[] {"--manifest", manifest.getPath()}));

        DaoCancerStudy.reCacheAll();
        assertEquals(samplesBefore - 1, DaoSample.getSamplesByCancerStudy(cancerStudy.getInternalId()).size());
        assertEquals(samplesBefore - 1,
            new DaoSampleList().getSampleListByStableId("study_es_0_all").getSampleList().size());
    }

    @Test
    public void testUnknownClassIsRejectedBeforeAnyStepRuns() throws Exception {
        List<Map<String, Object>> steps = studyEs0Steps();
        steps.add(step("java.lang.Runtime", "--noprogress"));
        File result = temporaryFolder.newFile("result.json");

        int status = ImportStepRunner.run(new String[] {
            "--manifest", writeManifest(steps).getPath(), "--result", result.getPath()});

        assertEquals(64, status);
        assertNull(DaoCancerStudy.getCancerStudyByStableId("study_es_0"));
    }

    private void assertStudyEs0Loaded(int mutationsBefore) throws Exception {
        DaoCancerStudy.reCacheAll();
        CancerStudy cancerStudy = DaoCancerStudy.getCancerStudyByStableId("study_es_0");
        assertNotNull(cancerStudy);
        assertEquals("Test study es_0", cancerStudy.getName());

        // the same count TestIntegrationTest expects from study_es_0
        assertEquals(mutationsBefore + 34, DaoMutation.getAllMutations().size());

        GeneticProfile svProfile = DaoGeneticProfile.getGeneticProfileByStableId("study_es_0_structural_variants");
        assertEquals(48, DaoStructuralVariant.getAllStructuralVariants().stream()
            .filter(sv -> sv.getGeneticProfileId() == svProfile.getGeneticProfileId())
            .collect(Collectors.toList()).size());

        GeneticProfile gisticProfile = DaoGeneticProfile.getGeneticProfileByStableId("study_es_0_gistic");
        int profileDataSize = 0;
        int ampOrDel = 0;
        for (Long entrezId : Arrays.asList(116983L, 375790L, 55210L, 83858L, 219293L, 54998L, 2073L)) {
            HashMap<Integer, String> data = DaoGeneticAlteration.getInstance()
                .getGeneticAlterationMap(gisticProfile.getGeneticProfileId(), entrezId);
            profileDataSize += data.size();
            ampOrDel += (int) data.values().stream().filter(v -> v.equals("2") || v.equals("-2")).count();
        }
        assertEquals(7 * 788, profileDataSize);
        assertEquals(63, ampOrDel);

        DaoSampleList daoSampleList = new DaoSampleList();
        assertEquals("All cases in study", daoSampleList.getSampleListByStableId("study_es_0_all").getName());
        assertEquals("this is an optional custom case list",
            daoSampleList.getSampleListByStableId("study_es_0_custom").getName());
        assertEquals(DaoSample.getSamplesByCancerStudy(cancerStudy.getInternalId()).size(),
            daoSampleList.getSampleListByStableId("study_es_0_all").getSampleList().size());
    }

    /**
     * The study_es_0 steps without the version check: the test classpath has no
     * db.version, so VersionUtil fails here as it would against a mismatched database.
     */
    private List<Map<String, Object>> studyEs0Steps() throws IOException {
        List<Map<String, Object>> steps = readSteps(STUDY_ES_0_STEPS);
        assertEquals("org.mskcc.cbio.portal.util.VersionUtil", steps.get(0).get("class"));
        return new java.util.ArrayList<>(steps.subList(1, steps.size()));
    }

    private File studyEs0Manifest() throws IOException {
        return writeManifest(studyEs0Steps());
    }

    private static Map<String, Object> step(String className, String... args) {
        Map<String, Object> step = new HashMap<>();
        step.put("class", className);
        step.put("args", Arrays.asList(args));
        return step;
    }

    private static int indexOfStepWithArg(List<Map<String, Object>> steps, String arg) {
        for (int i = 0; i < steps.size(); i++) {
            if (((List<?>) steps.get(i).get("args")).contains(arg)) {
                return i;
            }
        }
        throw new AssertionError("no step with argument " + arg);
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> readSteps(File manifest) throws IOException {
        return (List<Map<String, Object>>) mapper.readValue(manifest, Map.class).get("steps");
    }

    private File writeManifest(List<Map<String, Object>> steps) throws IOException {
        File manifest = temporaryFolder.newFile();
        mapper.writeValue(manifest, Map.of("version", 1, "steps", steps));
        return manifest;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> readResult(File result) throws IOException {
        return mapper.readValue(result, Map.class);
    }

    private static Map<String, Object> withoutNulls(Map<String, Object> map) {
        return map.entrySet().stream()
            .filter(e -> e.getValue() != null)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
