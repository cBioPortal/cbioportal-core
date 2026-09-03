/*
 * Copyright (c) 2026 Memorial Sloan-Kettering Cancer Center.
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

import java.util.Map;
import java.util.Set;
import org.mskcc.cbio.portal.dao.DaoException;
import org.mskcc.cbio.portal.dao.DaoFacetsGenes;
import org.mskcc.cbio.portal.dao.DaoGeneOptimized;
import org.mskcc.cbio.portal.model.CancerStudy;
import org.mskcc.cbio.portal.model.CanonicalGene;
import org.mskcc.cbio.portal.model.FacetsGeneLevelRecord;
import org.mskcc.cbio.portal.model.Sample;
import org.mskcc.cbio.portal.util.ProgressMonitor;

/**
 * Imports FACETS gene-level data (derived from, but not directly mapped to,
 * FACETS CNCF segment calls) into the {@code facets_genes} table.
 *
 * Expected tab-delimited data file columns (any order):
 * SAMPLE_ID, HUGO_SYMBOL, CHROMOSOME, START_POSITION, END_POSITION, TCN, LCN,
 * CELLULAR_FRACTION, PURITY
 */
public class ImportFacetsGeneLevelData extends AbstractImportFacetsData {

    private static final String HUGO_SYMBOL_COLUMN = "HUGO_SYMBOL";

    // Loaded once and reused for every row: DaoGeneOptimized.getInstance() holds
    // the full gene table in memory, so per-row lookups are plain HashMap gets,
    // not per-row database round trips.
    private final DaoGeneOptimized daoGeneOptimized = DaoGeneOptimized.getInstance();

    public ImportFacetsGeneLevelData(String[] args) {
        super(args);
    }

    @Override
    protected String dataTypeLabel() {
        return "FACETS gene-level";
    }

    @Override
    protected boolean dataExistsForCancerStudy(int cancerStudyId) throws DaoException {
        return DaoFacetsGenes.facetsGenesDataExistForCancerStudy(cancerStudyId);
    }

    @Override
    protected void deleteDataForSamples(int cancerStudyId, Set<Integer> sampleIds) throws DaoException {
        DaoFacetsGenes.deleteFacetsGenesDataForSamples(cancerStudyId, sampleIds);
    }

    @Override
    protected String[] getAdditionalRequiredColumns() {
        return new String[] { HUGO_SYMBOL_COLUMN };
    }

    @Override
    protected boolean storeRow(CommonFacetsFields common, String[] parts, Map<String, Integer> colIndex,
            CancerStudy study, Sample sample) throws DaoException {
        String hugoGeneSymbol = getValue(parts, colIndex, HUGO_SYMBOL_COLUMN).trim();
        CanonicalGene gene = daoGeneOptimized.getGene(hugoGeneSymbol);
        if (gene == null) {
            ProgressMonitor.logWarning("Gene '" + hugoGeneSymbol + "' not found in DB. Record will be skipped.");
            return false;
        }

        FacetsGeneLevelRecord rec = new FacetsGeneLevelRecord(
                study.getInternalId(),
                sample.getInternalId(),
                gene.getHugoGeneSymbolAllCaps(),
                gene.getEntrezGeneId(),
                common.chr,
                common.start,
                common.end,
                common.tcn,
                common.lcn,
                common.cellularFraction,
                common.purity);
        rec.setId(DaoFacetsGenes.getNextId());
        DaoFacetsGenes.addFacetsGeneLevelRecord(rec);
        return true;
    }

    /**
     * Runs the command as a script and exits with an appropriate exit code.
     *
     * @param args the arguments given on the command line
     */
    public static void main(String[] args) {
        ConsoleRunnable runner = new ImportFacetsGeneLevelData(args);
        runner.runInConsole();
    }
}
