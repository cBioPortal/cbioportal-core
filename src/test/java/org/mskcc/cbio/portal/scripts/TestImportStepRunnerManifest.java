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

package org.mskcc.cbio.portal.scripts;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mskcc.cbio.portal.dao.ClickHouseBulkLoader;
import org.mskcc.cbio.portal.util.ProgressMonitor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests of {@link ImportStepRunner} that need no database.
 */
public class TestImportStepRunnerManifest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testMissingManifestOptionIsAUsageError() {
        assertEquals(64, ImportStepRunner.run(new String[0]));
    }

    @Test
    public void testEmptyManifestSucceeds() throws IOException {
        File result = temporaryFolder.newFile();
        int status = ImportStepRunner.run(new String[] {
            "--manifest", manifest("{\"version\": 1, \"steps\": []}").getPath(),
            "--result", result.getPath()});
        assertEquals(0, status);
        assertEquals(resultJson(0, null), readResult(result));
    }

    @Test
    public void testUnknownClassIsAUsageError() throws IOException {
        File result = temporaryFolder.newFile();
        int status = ImportStepRunner.run(new String[] {
            "--manifest", manifest("{\"version\": 1, \"steps\": ["
                + "{\"class\": \"org.mskcc.cbio.portal.scripts.ImportCancerStudy\", \"args\": [\"meta_study.txt\"]},"
                + "{\"class\": \"java.lang.Runtime\", \"args\": []}]}").getPath(),
            "--result", result.getPath()});
        assertEquals(64, status);
        assertEquals(resultJson(64, null), readResult(result));
    }

    @Test
    public void testInvalidManifestsAreUsageErrors() throws IOException {
        for (String json : new String[] {
                "not json",
                "{\"version\": 2, \"steps\": []}",
                "{\"version\": 1}",
                "{\"version\": 1, \"steps\": [{\"class\": \"org.mskcc.cbio.portal.scripts.ImportCancerStudy\"}]}"}) {
            assertEquals(json, 64, ImportStepRunner.run(new String[] {"--manifest", manifest(json).getPath()}));
        }
        assertEquals(64, ImportStepRunner.run(new String[] {"--manifest", "no/such/manifest.json"}));
    }

    @Test
    public void testEveryStepClassCanBeRun() throws ClassNotFoundException {
        for (String className : ImportStepRunner.STEP_CLASSES) {
            Class<?> stepClass = Class.forName(className);
            if (!className.equals(ImportStepRunner.VERSION_UTIL_CLASS)) {
                assertTrue(className, ConsoleRunnable.class.isAssignableFrom(stepClass));
            }
        }
    }

    @Test
    public void testDiscardAllResetsBulkLoader() {
        ClickHouseBulkLoader.bulkLoadOn();
        ClickHouseBulkLoader.relaxedModeOn();
        ClickHouseBulkLoader.getClickHouseBulkLoader("sample").insertRecord("1", "2");
        ClickHouseBulkLoader.getClickHouseBulkLoader("patient").insertRecord("3");

        assertEquals(2, ClickHouseBulkLoader.discardAll());

        assertFalse(ClickHouseBulkLoader.isBulkLoad());
        assertEquals(0, ClickHouseBulkLoader.discardAll());
    }

    @Test
    public void testProgressMonitorReset() {
        ProgressMonitor.setConsoleMode(true);
        ProgressMonitor.logWarning("a warning");
        assertEquals(1, ProgressMonitor.getWarnings().size());

        ProgressMonitor.reset();

        assertEquals(0, ProgressMonitor.getWarnings().size());
        assertEquals(0, ProgressMonitor.getMessages().size());
    }

    private File manifest(String json) throws IOException {
        File manifest = temporaryFolder.newFile();
        Files.writeString(manifest.toPath(), json);
        return manifest;
    }

    private static Map<String, Object> resultJson(int status, Integer failedStep) {
        Map<String, Object> result = new HashMap<>();
        result.put("status", status);
        result.put("failed_step", failedStep);
        return result;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> readResult(File result) throws IOException {
        return new ObjectMapper().readValue(result, Map.class);
    }
}
