/* SPDX-License-Identifier: Apache-2.0 */
package org.mskcc.cbio.portal.scripts;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.MockedStatic;
import org.mskcc.cbio.portal.dao.ClickHouseAutoIncrement;
import org.mskcc.cbio.portal.dao.ClickHouseBulkLoader;
import org.mskcc.cbio.portal.dao.ClickHouseBulkDeleter;
import org.mskcc.cbio.portal.util.ProgressMonitor;
import static org.mockito.Mockito.mockStatic;
import static org.junit.Assert.*;

public class TestImportReviewResponses {
    @Rule public TemporaryFolder temp = new TemporaryFolder();

    @Test public void successfulStepsWithUnwritableResultFail() throws Exception {
        File manifest = temp.newFile();
        Files.writeString(manifest.toPath(), "{\"version\":1,\"steps\":[]}");
        assertEquals(74, ImportStepRunner.run(new String[] {
            "--manifest", manifest.getPath(), "--result", temp.newFolder().getPath()}));
        assertEquals(0, ImportStepRunner.run(new String[] {"--manifest", manifest.getPath()}));
    }

    @Test public void invalidManifestKeepsUsageStatusEvenIfResultCannotBeWritten() throws Exception {
        File manifest = temp.newFile();
        Files.writeString(manifest.toPath(), "not json");
        assertEquals(64, ImportStepRunner.run(new String[] {
            "--manifest", manifest.getPath(), "--result", temp.newFolder().getPath()}));
    }

    public static class SuccessfulStep extends ConsoleRunnable {
        public SuccessfulStep(String[] args) { super(args); }
        public void run() { }
    }

    public static class ThrowingConstructor extends ConsoleRunnable {
        public ThrowingConstructor(String[] args) {
            super(args);
            throw new IllegalArgumentException("constructor problem");
        }
        public void run() { fail("must not execute"); }
    }

    @Test public void constructorFailuresIdentifyStepArgumentsAndPreserveFailureStatus() throws Exception {
        PrintStream stderr = System.err;
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (MockedStatic<ClickHouseAutoIncrement> counters = mockStatic(ClickHouseAutoIncrement.class);
             MockedStatic<ClickHouseBulkLoader> loaders = mockStatic(ClickHouseBulkLoader.class);
             MockedStatic<ClickHouseBulkDeleter> deleters = mockStatic(ClickHouseBulkDeleter.class);
             PrintStream capture = new PrintStream(output)) {
            System.setErr(capture);
            for (String className : List.of("missing.Step", ThrowingConstructor.class.getName())) {
                output.reset();
                List<ImportStepRunner.Step> steps = List.of(
                    new ImportStepRunner.Step(SuccessfulStep.class.getName(), List.of()),
                    new ImportStepRunner.Step(className, List.of("--meta", "study file.txt")),
                    new ImportStepRunner.Step("must.not.run", List.of()));
                File result = temp.newFile();
                assertEquals(70, ImportStepRunner.runSteps(steps, result));
                Map<?, ?> report = new ObjectMapper().readValue(result, Map.class);
                assertEquals(70, report.get("status"));
                assertEquals(1, report.get("failed_step"));
                assertTrue(output.toString().contains("import step 2 (" + className + ")"));
                assertTrue(output.toString().contains("[--meta, study file.txt]"));
                assertFalse(output.toString().contains("must.not.run"));
                assertEquals(70, ImportStepRunner.runSteps(steps, temp.newFolder()));
            }
        } finally {
            System.setErr(stderr);
            ProgressMonitor.reset();
        }
    }

    @Test public void followingConsoleStepRestoresLoggingAfterReset() {
        PrintStream stdout = System.out;
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (PrintStream capture = new PrintStream(output)) {
            System.setOut(capture);
            for (int i = 0; i < 2; i++) {
                ProgressMonitor.reset();
                assertFalse(ProgressMonitor.isConsoleMode());
                int step = i;
                ConsoleRunnable runnable = new ConsoleRunnable(new String[0]) {
                    public void run() {
                        assertTrue(ProgressMonitor.isConsoleMode());
                        ProgressMonitor.setCurrentMessage("visible step " + step);
                    }
                };
                assertEquals(0, runnable.runInProcess());
            }
            assertTrue(output.toString().contains("visible step 0"));
            assertTrue(output.toString().contains("visible step 1"));
        } finally {
            System.setOut(stdout);
            ProgressMonitor.reset();
        }
    }
}
