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
import org.mskcc.cbio.portal.dao.DaoAscnSeg;
import org.mskcc.cbio.portal.model.CancerStudy;
import org.mskcc.cbio.portal.model.AscnSegment;
import org.mskcc.cbio.portal.model.Sample;

/**
 * Imports ASCN Seg (allele-specific copy number segment) data into the
 * {@code ascn_seg} table.
 *
 * Expected tab-delimited data file columns (any order):
 * SAMPLE_ID, CHROMOSOME, LOC_START, LOC_END, LOG_RATIO, TCN, LCN,
 * CELLULAR_FRACTION, PURITY, PLOIDY, DIP_LOG_R
 *
 * Note: unlike ASCN gene-level data (and the {@code ascn_seg} table's own
 * {@code start}/{@code end} columns), the ASCN Seg data file uses LOC_START/
 * LOC_END for the segment coordinates.
 */
public class ImportAscnSegData extends AbstractImportAscnData {

    private static final String LOC_START_COLUMN = "LOC_START";
    private static final String LOC_END_COLUMN = "LOC_END";
    private static final String LOG_RATIO_COLUMN = "LOG_RATIO";

    public ImportAscnSegData(String[] args) {
        super(args);
    }

    @Override
    protected String dataTypeLabel() {
        return "ASCN Seg";
    }

    @Override
    protected String startPositionColumn() {
        return LOC_START_COLUMN;
    }

    @Override
    protected String endPositionColumn() {
        return LOC_END_COLUMN;
    }

    @Override
    protected boolean dataExistsForCancerStudy(int cancerStudyId) throws DaoException {
        return DaoAscnSeg.ascnSegDataExistForCancerStudy(cancerStudyId);
    }

    @Override
    protected void deleteDataForSamples(int cancerStudyId, Set<Integer> sampleIds) throws DaoException {
        DaoAscnSeg.deleteAscnSegDataForSamples(cancerStudyId, sampleIds);
    }

    @Override
    protected String[] getAdditionalRequiredColumns() {
        return new String[] { LOG_RATIO_COLUMN };
    }

    @Override
    protected boolean storeRow(CommonAscnFields common, String[] parts, Map<String, Integer> colIndex,
            CancerStudy study, Sample sample) throws DaoException {
        Double logRatio = parseNullableDouble(getValue(parts, colIndex, LOG_RATIO_COLUMN));
        AscnSegment seg = new AscnSegment(
                study.getInternalId(),
                sample.getInternalId(),
                common.chr,
                common.start,
                common.end,
                common.tcn,
                common.lcn,
                common.cellularFraction,
                common.purity,
                common.ploidy,
                common.dipLogR,
                logRatio);
        seg.setId(DaoAscnSeg.getNextId());
        DaoAscnSeg.addAscnSegment(seg);
        return true;
    }

    /**
     * Runs the command as a script and exits with an appropriate exit code.
     *
     * @param args the arguments given on the command line
     */
    public static void main(String[] args) {
        ConsoleRunnable runner = new ImportAscnSegData(args);
        runner.runInConsole();
    }
}
