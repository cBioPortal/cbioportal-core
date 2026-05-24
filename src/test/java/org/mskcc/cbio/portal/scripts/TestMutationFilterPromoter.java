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

import org.junit.Test;
import org.mskcc.cbio.portal.model.CanonicalGene;
import org.mskcc.cbio.portal.model.ExtendedMutation;

import java.util.Collections;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests covering the handling of {@code 5'Flank} (promoter) mutations by
 * {@link MutationFilter}, both with the default filter and with a custom list of
 * filtered mutation types.
 *
 * <p>The "Promoter" protein-change relabeling is reserved for genes on the
 * promoter whitelist (currently only TERT, Entrez 7015). This must hold
 * regardless of whether a custom filtered-mutations list is supplied.
 */
public class TestMutationFilterPromoter {

    private static final long TERT_ENTREZ_ID = 7015L;
    private static final long NON_WHITELISTED_ENTREZ_ID = 207L; // AKT1

    private ExtendedMutation fivePrimeFlankMutation(long entrezGeneId) {
        CanonicalGene gene = new CanonicalGene(entrezGeneId, "GENE" + entrezGeneId);
        return new ExtendedMutation(gene, "Valid", "Somatic", "5'Flank");
    }

    @Test
    public void defaultFilterLabelsWhitelistedFlankMutationAsPromoter() {
        MutationFilter filter = new MutationFilter();
        ExtendedMutation mutation = fivePrimeFlankMutation(TERT_ENTREZ_ID);

        assertTrue(filter.acceptMutation(mutation, null));
        assertEquals("Promoter", mutation.getProteinChange());
    }

    @Test
    public void defaultFilterRejectsNonWhitelistedFlankMutation() {
        MutationFilter filter = new MutationFilter();
        ExtendedMutation mutation = fivePrimeFlankMutation(NON_WHITELISTED_ENTREZ_ID);

        assertFalse(filter.acceptMutation(mutation, null));
    }

    @Test
    public void customFilterLabelsWhitelistedFlankMutationAsPromoter() {
        MutationFilter filter = new MutationFilter();
        Set<String> filteredMutations = Collections.singleton("Silent");
        ExtendedMutation mutation = fivePrimeFlankMutation(TERT_ENTREZ_ID);

        assertTrue(filter.acceptMutation(mutation, filteredMutations));
        assertEquals("Promoter", mutation.getProteinChange());
    }

    @Test
    public void customFilterKeepsNonWhitelistedFlankMutationWithoutPromoterLabel() {
        MutationFilter filter = new MutationFilter();
        Set<String> filteredMutations = Collections.singleton("Silent");
        ExtendedMutation mutation = fivePrimeFlankMutation(NON_WHITELISTED_ENTREZ_ID);
        mutation.setProteinChange("p.E17K");

        // A custom filtered-mutations list only excludes the listed types, so a
        // 5'Flank mutation that is not listed must still be accepted ...
        assertTrue(filter.acceptMutation(mutation, filteredMutations));
        // ... but it must not be relabeled as a promoter mutation, since the
        // gene is not on the promoter whitelist.
        assertNotEquals("Promoter", mutation.getProteinChange());
        assertEquals("p.E17K", mutation.getProteinChange());
    }

    @Test
    public void customFilterRejectsListedMutationType() {
        MutationFilter filter = new MutationFilter();
        Set<String> filteredMutations = Collections.singleton("5'Flank");
        ExtendedMutation mutation = fivePrimeFlankMutation(TERT_ENTREZ_ID);

        assertFalse(filter.acceptMutation(mutation, filteredMutations));
    }
}
