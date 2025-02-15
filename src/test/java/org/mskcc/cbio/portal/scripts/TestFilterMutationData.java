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

package org.mskcc.cbio.portal.scripts;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * JUnit tests for FilterMutationData step
 */
public class TestFilterMutationData {

    public static final String SRC_MAF_DATA_FILE_PATH = "src/test/resources/data_mutations_extended.txt";
    private Path tempDir;

    @BeforeEach
    public void setUp() throws IOException {
        // Create a temporary directory for each test
        tempDir = Files.createTempDirectory("tempTestDir");

        // Copy files to the temporary directory
        Path dataFile = Paths.get(SRC_MAF_DATA_FILE_PATH);
        Path copiedDataFile = tempDir.resolve(dataFile.getFileName());
        Files.copy(dataFile, copiedDataFile, StandardCopyOption.REPLACE_EXISTING);

        Path metaFile = Paths.get("src/test/resources/meta_mutations_extended.txt");
        Path copiedMetaFile = tempDir.resolve(metaFile.getFileName());
        Files.copy(metaFile, copiedMetaFile, StandardCopyOption.REPLACE_EXISTING);
    }

  @AfterEach
    public void tearDown() throws IOException {
        // Delete the temporary directory and files after each test
        Files.walk(tempDir)
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
    }

    @Test
    public void testFilterMutationData() throws IOException {
        String mafFile = tempDir + "/data_mutations_extended.txt";
        String[] args = {
                "--data", mafFile,
                "--meta", tempDir + "/meta_mutations_extended.txt"
        };
        FilterMutationData runner = new FilterMutationData(args);
        runner.run();

        List<String> filteredDataFileLines = Files.readAllLines(Paths.get(mafFile));
        List<String> backedUpDataFileLines = Files.readAllLines(Paths.get(mafFile + "_backup"));
        List<String> originalDataFileLines = Files.readAllLines(Paths.get(SRC_MAF_DATA_FILE_PATH));
        assertEquals(originalDataFileLines, backedUpDataFileLines);
        assertFalse(filteredDataFileLines.isEmpty());
        assertTrue(originalDataFileLines.size() > filteredDataFileLines.size());
        assertTrue(originalDataFileLines.containsAll(filteredDataFileLines));
    }
}
