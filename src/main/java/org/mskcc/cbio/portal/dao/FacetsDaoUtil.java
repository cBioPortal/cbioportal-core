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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

/**
 * Shared helper for the FACETS DAOs ({@link DaoFacetsCncf}, {@link DaoFacetsGenes}).
 * Both tables are plain, append-only ClickHouse MergeTree tables loaded exclusively
 * through {@link ClickHouseBulkLoader}, so the boilerplate for existence checks,
 * multi-sample queries and sample-scoped deletes is identical apart from the table
 * name and row-mapping logic.
 */
final class FacetsDaoUtil {

    private FacetsDaoUtil() {}

    static void requireBulkLoad(String operation) throws DaoException {
        if (!ClickHouseBulkLoader.isBulkLoad()) {
            throw new DaoException("You have to turn on ClickHouseBulkLoader in order to " + operation);
        }
    }

    static boolean dataExistsForCancerStudy(String table, int cancerStudyId) throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(FacetsDaoUtil.class);
            pstmt = con.prepareStatement("SELECT EXISTS (SELECT 1 FROM `" + table + "` WHERE `cancer_study_id`=?)");
            pstmt.setInt(1, cancerStudyId);
            rs = pstmt.executeQuery();
            return rs.next() && rs.getInt(1) == 1;
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(FacetsDaoUtil.class, con, pstmt, rs);
        }
    }

    static void deleteDataForSamples(String table, int cancerStudyId, Set<Integer> sampleIds) throws DaoException {
        if (sampleIds == null || sampleIds.isEmpty()) {
            return;
        }
        ClickHouseBulkUploader.upload(sampleIds, stagingTable -> {
            Connection con = null;
            try {
                con = JdbcUtil.getDbConnection(FacetsDaoUtil.class);
                try (PreparedStatement pstmt = con.prepareStatement(
                        "DELETE FROM `" + table + "` WHERE `cancer_study_id`=? AND `sample_id` IN (SELECT id FROM " + stagingTable + ")")) {
                    pstmt.setInt(1, cancerStudyId);
                    pstmt.executeUpdate();
                }
            } finally {
                JdbcUtil.closeAll(FacetsDaoUtil.class, con, null, null);
            }
            return null;
        });
    }

    @FunctionalInterface
    interface RowMapper<T> {
        T map(ResultSet rs) throws SQLException;
    }

    static <T> java.util.List<T> queryForSamples(String table, Collection<Integer> sampleIds, int cancerStudyId,
            RowMapper<T> mapper) throws DaoException {
        if (sampleIds == null || sampleIds.isEmpty()) {
            return java.util.Collections.emptyList();
        }
        return ClickHouseBulkUploader.upload(sampleIds, stagingTable -> {
            java.util.List<T> results = new java.util.ArrayList<>();
            Connection con = null;
            try {
                con = JdbcUtil.getDbConnection(FacetsDaoUtil.class);
                try (PreparedStatement pstmt = con.prepareStatement(
                        "SELECT * FROM `" + table + "`" +
                        " WHERE `sample_id` IN (SELECT id FROM " + stagingTable + ")" +
                        " AND `cancer_study_id`=" + cancerStudyId);
                     ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        results.add(mapper.map(rs));
                    }
                }
                return results;
            } finally {
                JdbcUtil.closeAll(FacetsDaoUtil.class, con, null, null);
            }
        });
    }
}
