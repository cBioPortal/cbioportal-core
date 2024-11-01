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

import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mskcc.cbio.maf.MafRecord;

import java.util.Set;
import java.util.stream.Stream;

import static org.junit.Assert.*;

/**
 * JUnit tests for MutationFilter class.
 */
public class TestMutationFilter {

    MutationFilter testee = new MutationFilter();
    MafRecord mafRecord = new MafRecord();
    {
        mafRecord.setChr("17");
        mafRecord.setGivenEntrezGeneId("7157");
        mafRecord.setHugoGeneSymbol("TP53");
        mafRecord.setMutationStatus("Somatic");
        mafRecord.setValidationStatus("Valid");
        mafRecord.setVariantClassification("In_Frame_Ins");
    }

    @Test
    public void testInitialCounters() {
        assertEquals(0, testee.getDecisions());
        assertEquals(0, testee.getAccepts());
        assertEquals(0, testee.getRejects());
        assertEquals(0, testee.getInvalidGeneInfo());
        assertEquals(0, testee.getInvalidChromosome());
        assertEquals(0, testee.getLohOrWildTypeRejects());
        assertEquals(0, testee.getRedactedOrWildTypeRejects());
        assertEquals(0, testee.getMutationStatusNoneRejects());
    }

    @Test
    public void testAccept() {
        assertTrue(testee.acceptMutation(mafRecord, null));
        assertEquals(1, testee.getDecisions());
        assertEquals(1, testee.getAccepts());
        assertEquals(0, testee.getRejects());
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"", " ", "0"})
    public void testAcceptWhenEntrezGeneIdIsNotSpecifiedButHugoGeneSymbolIsKnown(String givenEntrezGeneId) {
        mafRecord.setGivenEntrezGeneId(givenEntrezGeneId);
        assertTrue(testee.acceptMutation(mafRecord, null));
        assertEquals(1, testee.getDecisions());
        assertEquals(1, testee.getAccepts());
        assertEquals(0, testee.getRejects());
    }

    static Stream<Arguments> provideAllEmptyEntrezGeneIdAndHugoGeneSymbolCombinations() {
        String[] entrezGeneIds = {null, "", " ", "0"};
        String[] hugoGeneSymbol = {null, "", " ", "Unknown"};

        return Stream.of(entrezGeneIds)
                .flatMap(a -> Stream.of(hugoGeneSymbol)
                        .map(b -> Arguments.of(a, b)));
    }
    @ParameterizedTest
    @MethodSource("provideAllEmptyEntrezGeneIdAndHugoGeneSymbolCombinations")
    public void testRejectWhenGeneInfoIsNotSpecified(String givenEntrezGeneId, String hugoGeneSymbol) {
        mafRecord.setGivenEntrezGeneId(givenEntrezGeneId);
        mafRecord.setHugoGeneSymbol(hugoGeneSymbol);
        assertFalse(testee.acceptMutation(mafRecord, null));
        assertEquals(1, testee.getDecisions());
        assertEquals(0, testee.getAccepts());
        assertEquals(1, testee.getRejects());
        assertEquals(1, testee.getInvalidGeneInfo());
    }

    @ParameterizedTest
    @ValueSource(strings = {"-1", "ABC"})
    public void testRejectWhenEntrezGeneIdIsNegativeOrNotNumeric(String givenEntrezGeneId) {
        mafRecord.setGivenEntrezGeneId(givenEntrezGeneId);
        assertFalse(testee.acceptMutation(mafRecord, null));
        assertEquals(1, testee.getDecisions());
        assertEquals(0, testee.getAccepts());
        assertEquals(1, testee.getRejects());
        assertEquals(1, testee.getInvalidGeneInfo());
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"", "-1", "0", "Z"})
    public void testRejectWhenChromosomeIsInvalid(String chr) {
        mafRecord.setChr(chr);
        assertFalse(testee.acceptMutation(mafRecord, null));
        assertEquals(1, testee.getDecisions());
        assertEquals(0, testee.getAccepts());
        assertEquals(1, testee.getRejects());
        assertEquals(1, testee.getInvalidChromosome());
    }

    @Test
    public void testRejectWhenMutationStatusIsNone() {
        mafRecord.setMutationStatus("None");
        assertFalse(testee.acceptMutation(mafRecord, null));
        assertEquals(1, testee.getDecisions());
        assertEquals(0, testee.getAccepts());
        assertEquals(1, testee.getRejects());
        assertEquals(1, testee.getMutationStatusNoneRejects());
    }

    @ParameterizedTest
    @ValueSource(strings = {"LOH", "Wildtype"})
    public void testRejectWhenMutationStatusIsLohOrWildtype(String mutationStatus) {
        mafRecord.setMutationStatus(mutationStatus);
        assertFalse(testee.acceptMutation(mafRecord, null));
        assertEquals(1, testee.getDecisions());
        assertEquals(0, testee.getAccepts());
        assertEquals(1, testee.getRejects());
        assertEquals(1, testee.getLohOrWildTypeRejects());
    }

    @ParameterizedTest
    @ValueSource(strings = {"Redacted", "Wildtype"})
    public void testRejectWhenValidationStatusIsRedactedOrWildtype(String validationStatus) {
        mafRecord.setValidationStatus(validationStatus);
        assertFalse(testee.acceptMutation(mafRecord, null));
        assertEquals(1, testee.getDecisions());
        assertEquals(0, testee.getAccepts());
        assertEquals(1, testee.getRejects());
        assertEquals(1, testee.getRedactedOrWildTypeRejects());
    }

    final static String[] FILTERED_OUT_MUTATION_TYPES = {"Silent", "Intron", "3'UTR", "3'Flank", "5'UTR", "IGR", "RNA"};
    static Stream<String> filteredOutMutationTypesProvider() {
        return Stream.of(FILTERED_OUT_MUTATION_TYPES);
    }
    @ParameterizedTest
    @MethodSource("filteredOutMutationTypesProvider")
    public void testRejectWhenVariantClassificationInTheDefaultBlackList(String mutationType) {
        mafRecord.setVariantClassification(mutationType);
        assertFalse(testee.acceptMutation(mafRecord, null));
        assertEquals(1, testee.getDecisions());
        assertEquals(0, testee.getAccepts());
        assertEquals(1, testee.getRejects());
        assertEquals(Set.of(mutationType), testee.getRejectionMap().keySet());
    }

    static Stream<Arguments> provideAllVariantClassificationEmptyValuesAndFilteredOutMutationTypeCombinations() {
        String[] variantClassificationEmptyValues = {null, "", "NULL", "NA"};

        return Stream.of(variantClassificationEmptyValues)
                .flatMap(a -> Stream.of(FILTERED_OUT_MUTATION_TYPES)
                        .map(b -> Arguments.of(a, b)));
    }
    @ParameterizedTest
    @MethodSource("provideAllVariantClassificationEmptyValuesAndFilteredOutMutationTypeCombinations")
    public void testRejectWhenMafVariantClassificationInTheDefaultBlackList(String varianClassificatinEmptyValue, String mutationType) {
        mafRecord.setVariantClassification(varianClassificatinEmptyValue);
        mafRecord.setMafVariantClassification(mutationType);
        assertFalse(testee.acceptMutation(mafRecord, null));
        assertEquals(1, testee.getDecisions());
        assertEquals(0, testee.getAccepts());
        assertEquals(1, testee.getRejects());
        assertEquals(Set.of(mutationType), testee.getRejectionMap().keySet());
    }

    @Test
    public void testRejectWhenVariantClassificationInTheCustomBlackList() {
        Set<String> filteredMutations = Set.of("In_Frame_Ins");
        assertFalse(testee.acceptMutation(mafRecord, filteredMutations));
        assertEquals(1, testee.getDecisions());
        assertEquals(0, testee.getAccepts());
        assertEquals(1, testee.getRejects());
        assertEquals(filteredMutations, testee.getRejectionMap().keySet());
    }

    @Test
    public void testAccept5FlankMutationWhenNotInTheCustomBlackList() {
        assertNull(mafRecord.getProteinChange());

        mafRecord.setVariantClassification("5'Flank");
        Set<String> filteredMutations = Set.of("In_Frame_Ins");
        assertTrue(testee.acceptMutation(mafRecord, filteredMutations));

        assertEquals("Promoter", mafRecord.getProteinChange());
        assertEquals(1, testee.getDecisions());
        assertEquals(1, testee.getAccepts());
        assertEquals(0, testee.getRejects());
        assertEquals(Set.of(), testee.getRejectionMap().keySet());
    }

    @Test
    public void testReject5FlankMutationGene() {
        mafRecord.setVariantClassification("5'Flank");

        assertFalse(testee.acceptMutation(mafRecord, null));

        assertEquals(1, testee.getDecisions());
        assertEquals(0, testee.getAccepts());
        assertEquals(1, testee.getRejects());
        assertEquals(Set.of("5'Flank"), testee.getRejectionMap().keySet());
    }

    @Test
    public void testAccept5FlankMutationGeneWhenTERTEntrezGeneIdIsSpecified() {
        mafRecord.setVariantClassification("5'Flank");
        mafRecord.setGivenEntrezGeneId("7015");

        assertTrue(testee.acceptMutation(mafRecord, null));

        assertEquals("Promoter", mafRecord.getProteinChange());
        assertEquals(1, testee.getDecisions());
        assertEquals(1, testee.getAccepts());
        assertEquals(0, testee.getRejects());
        assertEquals(Set.of(), testee.getRejectionMap().keySet());
    }
}
