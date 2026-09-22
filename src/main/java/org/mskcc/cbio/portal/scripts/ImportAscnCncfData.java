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
import org.mskcc.cbio.portal.dao.DaoAscnCncf;
import org.mskcc.cbio.portal.model.CancerStudy;
import org.mskcc.cbio.portal.model.AscnCncfSegment;
import org.mskcc.cbio.portal.model.Sample;

/**
 * Imports ASCN CNCF (allele-specific copy number segment) data into the
 * {@code ascn_cncf} table.
 *
 * Expected tab-delimited data file columns (any order):
 * SAMPLE_ID, CHROMOSOME, START_POSITION, END_POSITION, TCN, LCN,
 * CELLULAR_FRACTION, PURITY
 */
public class ImportAscnCncfData extends AbstractImportAscnData {

    public ImportAscnCncfData(String[] args) {
        super(args);
    }

    @Override
    protected String dataTypeLabel() {
        return "ASCN CNCF";
    }

    @Override
    protected boolean dataExistsForCancerStudy(int cancerStudyId) throws DaoException {
        return DaoAscnCncf.ascnCncfDataExistForCancerStudy(cancerStudyId);
    }

    @Override
    protected void deleteDataForSamples(int cancerStudyId, Set<Integer> sampleIds) throws DaoException {
        DaoAscnCncf.deleteAscnCncfDataForSamples(cancerStudyId, sampleIds);
    }

    @Override
    protected boolean storeRow(CommonAscnFields common, String[] parts, Map<String, Integer> colIndex,
            CancerStudy study, Sample sample) throws DaoException {
        AscnCncfSegment seg = new AscnCncfSegment(
                study.getInternalId(),
                sample.getInternalId(),
                common.chr,
                common.start,
                common.end,
                common.tcn,
                common.lcn,
                common.cellularFraction,
                common.purity);
        seg.setId(DaoAscnCncf.getNextId());
        DaoAscnCncf.addAscnCncfSegment(seg);
        return true;
    }

    /**
     * Runs the command as a script and exits with an appropriate exit code.
     *
     * @param args the arguments given on the command line
     */
    public static void main(String[] args) {
        ConsoleRunnable runner = new ImportAscnCncfData(args);
        runner.runInConsole();
    }
}
