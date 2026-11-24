/*
 * Copyright 2026 Memorial Sloan Kettering Cancer Center
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.mskcc.cbio.portal.scripts;

import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.Connection;
import javax.sql.DataSource;
import org.easymock.EasyMock;
import org.mskcc.cbio.portal.dao.JdbcUtil;
import org.mskcc.cbio.portal.dao.ClickHouseAutoIncrement;
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
    public void testEveryStepClassCanBeRun() throws ReflectiveOperationException {
        for (String className : ImportStepRunner.STEP_CLASSES) {
            Class<?> stepClass = Class.forName(className);
            if (!className.equals(ImportStepRunner.VERSION_UTIL_CLASS)) {
                assertTrue(className, ConsoleRunnable.class.isAssignableFrom(stepClass));
                stepClass.getConstructor(String[].class);
            }
        }
    }

    @Test
    public void testStepCleanupKeepsCountersDespiteStaleDatabaseReads() throws Exception {
        ResultSet empty = EasyMock.createNiceMock(ResultSet.class);
        EasyMock.expect(empty.next()).andReturn(false).anyTimes();
        EasyMock.replay(empty);
        PreparedStatement statement = EasyMock.createNiceMock(PreparedStatement.class);
        EasyMock.expect(statement.executeQuery()).andReturn(empty).anyTimes();
        EasyMock.replay(statement);
        Connection connection = EasyMock.createNiceMock(Connection.class);
        EasyMock.expect(connection.prepareStatement(EasyMock.anyString()))
            .andReturn(statement).anyTimes();
        EasyMock.replay(connection);
        DataSource source = EasyMock.createNiceMock(DataSource.class);
        EasyMock.expect(source.getConnection()).andReturn(connection).anyTimes();
        EasyMock.replay(source);
        JdbcUtil.setDataSource(source);
        ClickHouseAutoIncrement.resetCounters();
        try {
            assertEquals(1, ClickHouseAutoIncrement.nextId("seq_genetic_profile"));
            ImportStepRunner.resetAfterStep(0);
            assertEquals(2, ClickHouseAutoIncrement.nextId("seq_genetic_profile"));
            ImportStepRunner.resetAfterStep(1);
            assertEquals(3, ClickHouseAutoIncrement.nextId("seq_genetic_profile"));
        } finally {
            ClickHouseAutoIncrement.resetCounters();
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
