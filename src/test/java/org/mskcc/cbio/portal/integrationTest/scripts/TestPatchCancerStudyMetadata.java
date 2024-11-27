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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mskcc.cbio.portal.dao.DaoCancerStudy;
import org.mskcc.cbio.portal.dao.DaoException;
import org.mskcc.cbio.portal.model.CancerStudy;
import org.mskcc.cbio.portal.scripts.PatchCancerStudyMetadata;
import org.mskcc.cbio.portal.scripts.UsageException;
import org.mskcc.cbio.portal.util.ProgressMonitor;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.annotation.Transactional;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * Tests Patching Study Metadata
 *
 * @author Ruslan Forostianov
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:/applicationContext-dao.xml" })
@Rollback
@Transactional
public class TestPatchCancerStudyMetadata {

    private int cancerStudyInternalId;

    @Rule
    public ExpectedException exception = ExpectedException.none();

    /**
	 * This is executed n times, for each of the n test methods below:
	 * @throws DaoException
	 */
    @Before 
    public void setUp() throws DaoException
    {
        DaoCancerStudy.reCacheAll();
        CancerStudy cancerStudy = new CancerStudy("testnew","testnew","testnew","brca",true);
        cancerStudy.setReferenceGenome("hg19");
        cancerStudy.setCitation("citation");
        cancerStudy.setPmid("0000");
        cancerStudy.setGroupsInUpperCase("XYZ");
        DaoCancerStudy.addCancerStudy(cancerStudy);
        this.cancerStudyInternalId = cancerStudy.getInternalId();
	}

    @Test
    public void testPatchCommand() throws DaoException, SQLException, IOException {
        Path tempFile = Files.createTempFile("tempFile_", ".txt");
        Files.write(tempFile, """
        cancer_study_identifier: testnew
        name: testnew name updated
        description: testnew description updated
        citation: testnew citation updated
        pmid: 12345
        """.getBytes());
        tempFile.toFile().deleteOnExit();

        new PatchCancerStudyMetadata(new String[] {
                tempFile.toString()
        }).run();

        DaoCancerStudy.reCacheAll();
        CancerStudy cancerStudy = DaoCancerStudy.getCancerStudyByInternalId(cancerStudyInternalId);
        assertNotNull(cancerStudy);
        // Remains
        assertEquals("brca", cancerStudy.getTypeOfCancerId());
        assertEquals(Set.of("XYZ"), cancerStudy.getGroups());
        // Patched values
        assertEquals("testnew name updated", cancerStudy.getName());
        assertEquals("testnew description updated", cancerStudy.getDescription());
        assertEquals("testnew citation updated", cancerStudy.getCitation());
        assertEquals("12345", cancerStudy.getPmid());
    }

    @Test
    public void testUsageException() {
        exception.expect(UsageException.class);
        exception.expectMessage("Invalid usage of the org.mskcc.cbio.portal.scripts.PatchCancerStudyMetadata script");
        new PatchCancerStudyMetadata(new String[] {
        }).run();
    }

	@Test
    public void testPartialPatch() throws DaoException, SQLException, IOException {
        InputStream inputStream = new ByteArrayInputStream("""
        cancer_study_identifier: testnew
        name: testnew name updated
        """.getBytes());

        PatchCancerStudyMetadata.run(inputStream);

        DaoCancerStudy.reCacheAll();
        CancerStudy cancerStudy = DaoCancerStudy.getCancerStudyByInternalId(cancerStudyInternalId);
        assertNotNull(cancerStudy);
        // Remains
        assertEquals("brca", cancerStudy.getTypeOfCancerId());
        assertEquals(Set.of("XYZ"), cancerStudy.getGroups());
        assertEquals("testnew", cancerStudy.getDescription());
        assertEquals("citation", cancerStudy.getCitation());
        assertEquals("0000", cancerStudy.getPmid());
        // Patched values
        assertEquals("testnew name updated", cancerStudy.getName());
        assertTrue(
                ProgressMonitor
                        .getLog()
                        .contains("--> name: testnew name updated"));
	}

    public void testEmptyDescription() throws DaoException, SQLException, IOException {
        InputStream inputStream = new ByteArrayInputStream("""
        cancer_study_identifier: testnew
        description:
        """.getBytes());

        PatchCancerStudyMetadata.run(inputStream);

        DaoCancerStudy.reCacheAll();
        CancerStudy cancerStudy = DaoCancerStudy.getCancerStudyByInternalId(cancerStudyInternalId);
        assertNotNull(cancerStudy);
        // Remains
        assertEquals("brca", cancerStudy.getTypeOfCancerId());
        assertEquals(Set.of("XYZ"), cancerStudy.getGroups());
        assertEquals("testnew name updated", cancerStudy.getName());
        assertEquals("citation", cancerStudy.getCitation());
        assertEquals("0000", cancerStudy.getPmid());
        // Patched values
        assertEquals("", cancerStudy.getDescription());
        assertTrue(
                ProgressMonitor
                        .getLog()
                        .contains("--> description: \n"));
    }

    @Test
    public void testEmptyContent() throws SQLException, IOException, DaoException {
        exception.expect(IllegalStateException.class);
        exception.expectMessage("No fields were found");
        InputStream inputStream = new ByteArrayInputStream("".getBytes());

        PatchCancerStudyMetadata.run(inputStream);
    }

    @Test
    public void testNoCancerStudyIdentifierField() throws SQLException, IOException, DaoException {
        exception.expect(IllegalStateException.class);
        exception.expectMessage("No cancer_study_identifier field has been found");
        InputStream inputStream = new ByteArrayInputStream("""
                name: name
                """.getBytes());

        PatchCancerStudyMetadata.run(inputStream);
    }

    @Test
    public void testNoFieldsToPatch() throws SQLException, IOException, DaoException {
        exception.expect(IllegalStateException.class);
        exception.expectMessage("No field to patch has been found");
        InputStream inputStream = new ByteArrayInputStream("""
                cancer_study_identifier: non_existing_id
                """.getBytes());

        PatchCancerStudyMetadata.run(inputStream);
    }

    public void testNoStudyToPatch() throws SQLException, IOException, DaoException {
        exception.expect(IllegalStateException.class);
        exception.expectMessage("No study has been patched");
        InputStream inputStream = new ByteArrayInputStream("""
                cancer_study_identifier: non_existing_id
                name: name
                """.getBytes());

        PatchCancerStudyMetadata.run(inputStream);
    }

    @Test
    public void testNotSupportedPatchFields() throws DaoException, SQLException, IOException {
        InputStream inputStream = new ByteArrayInputStream("""
        cancer_study_identifier: testnew
        name: testnew name updated
        some_field: some_value
        """.getBytes());

        PatchCancerStudyMetadata.run(inputStream);

        assertTrue(
                ProgressMonitor
                        .getWarnings()
                        .stream()
                        .anyMatch(
                                warn ->
                                        warn .contains("Patch functionality is not supported for 'some_field' field." +
                                                " Skipping it.")));
    }
}
