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

package org.mskcc.cbio.portal.util;

import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link CnaUtil}, focusing on the resolution of the
 * Entrez_Gene_Id column. The Entrez ID is optional as long as a Hugo_Symbol is
 * provided, so a missing or "NA" value must not raise a warning.
 *
 * @see <a href="https://docs.cbioportal.org/file-formats/#discrete-copy-number-data">file format docs</a>
 */
public class TestCnaUtil {

    @Before
    public void setUp() {
        ProgressMonitor.resetWarnings();
    }

    private CnaUtil cnaUtil(String[] header) {
        return new CnaUtil(header, Collections.emptySet());
    }

    private boolean hasInvalidEntrezWarning() {
        return ProgressMonitor.getWarnings().stream()
                .anyMatch(w -> w.contains("Ignoring line with invalid Entrez_Id"));
    }

    @Test
    public void getEntrezSymbolReturnsZeroWithoutWarningWhenEntrezColumnAbsent() {
        // No Entrez_Gene_Id column at all (only Hugo_Symbol provided).
        CnaUtil cnaUtil = cnaUtil(new String[]{"Hugo_Symbol", "Sample_Id", "Value"});
        long entrez = cnaUtil.getEntrezSymbol(new String[]{"KIAA1549", "TCGA-A1-A0SB-11", "2"});
        assertEquals(0L, entrez);
        assertFalse("No warning expected when the Entrez_Gene_Id column is absent",
                hasInvalidEntrezWarning());
    }

    @Test
    public void getEntrezSymbolReturnsZeroWithoutWarningWhenValueIsNa() {
        CnaUtil cnaUtil = cnaUtil(new String[]{"Hugo_Symbol", "Entrez_Gene_Id", "Sample_Id", "Value"});
        long entrez = cnaUtil.getEntrezSymbol(new String[]{"KIAA1549", "NA", "TCGA-A1-A0SB-11", "2"});
        assertEquals(0L, entrez);
        assertFalse("No warning expected when the Entrez_Gene_Id value is NA",
                hasInvalidEntrezWarning());
    }

    @Test
    public void getEntrezSymbolReturnsZeroWithoutWarningWhenValueIsEmpty() {
        CnaUtil cnaUtil = cnaUtil(new String[]{"Hugo_Symbol", "Entrez_Gene_Id", "Sample_Id", "Value"});
        long entrez = cnaUtil.getEntrezSymbol(new String[]{"KIAA1549", "", "TCGA-A1-A0SB-11", "2"});
        assertEquals(0L, entrez);
        assertFalse("No warning expected when the Entrez_Gene_Id value is empty",
                hasInvalidEntrezWarning());
    }

    @Test
    public void getEntrezSymbolReturnsParsedValueForValidEntrezId() {
        CnaUtil cnaUtil = cnaUtil(new String[]{"Hugo_Symbol", "Entrez_Gene_Id", "Sample_Id", "Value"});
        long entrez = cnaUtil.getEntrezSymbol(new String[]{"KIAA1549", "57670", "TCGA-A1-A0SB-11", "2"});
        assertEquals(57670L, entrez);
        assertFalse("No warning expected for a valid Entrez ID", hasInvalidEntrezWarning());
    }

    @Test
    public void getEntrezSymbolWarnsForGenuinelyInvalidEntrezId() {
        CnaUtil cnaUtil = cnaUtil(new String[]{"Hugo_Symbol", "Entrez_Gene_Id", "Sample_Id", "Value"});
        long entrez = cnaUtil.getEntrezSymbol(new String[]{"KIAA1549", "not-a-number", "TCGA-A1-A0SB-11", "2"});
        assertEquals(0L, entrez);
        assertTrue("A non-numeric, non-NA Entrez ID should still produce a warning",
                hasInvalidEntrezWarning());
    }
}
