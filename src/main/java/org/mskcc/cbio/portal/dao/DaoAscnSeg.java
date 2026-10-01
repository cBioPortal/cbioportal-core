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

package org.mskcc.cbio.portal.dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.mskcc.cbio.portal.model.AscnSegment;

/**
 * DAO for ASCN Seg (allele-specific copy number segment) data.
 *
 * Mirrors {@link DaoCopyNumberSegment}: rows are exclusively appended through
 * {@link ClickHouseBulkLoader}, and the `ascn_seg` table is ordered by
 * (cancer_study_id, sample_id, chr, start) to make genome-ordered, per-sample
 * segment reads (e.g. segment plots) efficient.
 */
public final class DaoAscnSeg {

    private static final String TABLE = "ascn_seg";

    private DaoAscnSeg() {}

    public static void addAscnSegment(AscnSegment seg) throws DaoException {
        AscnDaoUtil.requireBulkLoad("insert ASCN Seg data");
        ClickHouseBulkLoader.getClickHouseBulkLoader(TABLE).insertRecord(
                Long.toString(seg.getId()),
                Integer.toString(seg.getCancerStudyId()),
                Integer.toString(seg.getSampleId()),
                seg.getChr(),
                Long.toString(seg.getStart()),
                Long.toString(seg.getEnd()),
                seg.getTcn() == null ? null : Double.toString(seg.getTcn()),
                seg.getLcn() == null ? null : Double.toString(seg.getLcn()),
                seg.getCellularFraction() == null ? null : Double.toString(seg.getCellularFraction()),
                seg.getPurity() == null ? null : Double.toString(seg.getPurity())
        );
    }

    public static void addAscnSegments(List<AscnSegment> segs) throws DaoException {
        for (AscnSegment seg : segs) {
            addAscnSegment(seg);
        }
    }

    /**
     * Reserves and returns the next unique id for a new `ascn_seg` row.
     * Callers should invoke this once per row before {@link #addAscnSegment}.
     */
    public static long getNextId() throws DaoException {
        return ClickHouseAutoIncrement.nextId("seq_ascn_seg");
    }

    public static List<AscnSegment> getSegmentsForSample(int sampleId, int cancerStudyId) throws DaoException {
        return getSegmentsForSamples(Collections.singleton(sampleId), cancerStudyId);
    }

    public static List<AscnSegment> getSegmentsForSamples(Collection<Integer> sampleIds, int cancerStudyId) throws DaoException {
        return AscnDaoUtil.queryForSamples(TABLE, sampleIds, cancerStudyId, DaoAscnSeg::mapRow);
    }

    public static boolean ascnSegDataExistForCancerStudy(int cancerStudyId) throws DaoException {
        return AscnDaoUtil.dataExistsForCancerStudy(TABLE, cancerStudyId);
    }

    public static void deleteAscnSegDataForSamples(int cancerStudyId, Set<Integer> sampleIds) throws DaoException {
        AscnDaoUtil.deleteDataForSamples(TABLE, cancerStudyId, sampleIds);
    }

    private static AscnSegment mapRow(ResultSet rs) throws SQLException {
        AscnSegment seg = new AscnSegment(
                rs.getInt("cancer_study_id"),
                rs.getInt("sample_id"),
                rs.getString("chr"),
                rs.getLong("start"),
                rs.getLong("end"),
                getNullableDouble(rs, "tcn"),
                getNullableDouble(rs, "lcn"),
                getNullableDouble(rs, "cellular_fraction"),
                getNullableDouble(rs, "purity"));
        seg.setId(rs.getLong("seg_id"));
        return seg;
    }

    private static Double getNullableDouble(ResultSet rs, String column) throws SQLException {
        double value = rs.getDouble(column);
        return rs.wasNull() ? null : value;
    }
}
