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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.mskcc.cbio.portal.model.FacetsGeneLevelRecord;

/**
 * DAO for FACETS gene-level data, derived from (but not directly mapped to)
 * FACETS CNCF segment calls (see {@link DaoFacetsCncf}).
 *
 * Rows are exclusively appended through {@link ClickHouseBulkLoader}. The
 * `facets_genes` table is ordered by (cancer_study_id, sample_id,
 * hugo_gene_symbol) for efficient per-sample gene table reads, with a bloom
 * filter index on hugo_gene_symbol to keep cross-sample/cohort gene lookups
 * (e.g. "show gene X across all samples") cheap as well.
 */
public final class DaoFacetsGenes {

    private static final String TABLE = "facets_genes";

    private DaoFacetsGenes() {}

    public static void addFacetsGeneLevelRecord(FacetsGeneLevelRecord rec) throws DaoException {
        FacetsDaoUtil.requireBulkLoad("insert FACETS gene-level data");
        ClickHouseBulkLoader.getClickHouseBulkLoader(TABLE).insertRecord(
                Long.toString(rec.getId()),
                Integer.toString(rec.getCancerStudyId()),
                Integer.toString(rec.getSampleId()),
                rec.getHugoGeneSymbol(),
                Long.toString(rec.getEntrezGeneId()),
                rec.getChr(),
                Long.toString(rec.getStart()),
                Long.toString(rec.getEnd()),
                rec.getTcn() == null ? null : Double.toString(rec.getTcn()),
                rec.getLcn() == null ? null : Double.toString(rec.getLcn()),
                rec.getCellularFraction() == null ? null : Double.toString(rec.getCellularFraction()),
                rec.getPurity() == null ? null : Double.toString(rec.getPurity())
        );
    }

    public static void addFacetsGeneLevelRecords(List<FacetsGeneLevelRecord> recs) throws DaoException {
        for (FacetsGeneLevelRecord rec : recs) {
            addFacetsGeneLevelRecord(rec);
        }
    }

    /**
     * Reserves and returns the next unique id for a new `facets_genes` row.
     * Callers should invoke this once per row before {@link #addFacetsGeneLevelRecord}.
     */
    public static long getNextId() throws DaoException {
        return ClickHouseAutoIncrement.nextId("seq_facets_genes");
    }

    public static List<FacetsGeneLevelRecord> getGeneLevelDataForSample(int sampleId, int cancerStudyId) throws DaoException {
        return getGeneLevelDataForSamples(Collections.singleton(sampleId), cancerStudyId);
    }

    public static List<FacetsGeneLevelRecord> getGeneLevelDataForSamples(Collection<Integer> sampleIds, int cancerStudyId) throws DaoException {
        return FacetsDaoUtil.queryForSamples(TABLE, sampleIds, cancerStudyId, DaoFacetsGenes::mapRow);
    }

    /**
     * Fetches gene-level FACETS records for a single gene across a set of samples
     * (e.g. for an oncoprint-style, cohort-wide view). Relies on the bloom filter
     * index on `hugo_gene_symbol` for efficient filtering.
     */
    public static List<FacetsGeneLevelRecord> getGeneLevelDataForGene(String hugoGeneSymbol, Collection<Integer> sampleIds, int cancerStudyId) throws DaoException {
        if (sampleIds == null || sampleIds.isEmpty()) {
            return Collections.emptyList();
        }
        return ClickHouseBulkUploader.upload(sampleIds, stagingTable -> {
            List<FacetsGeneLevelRecord> results = new ArrayList<>();
            Connection con = null;
            try {
                con = JdbcUtil.getDbConnection(DaoFacetsGenes.class);
                try (PreparedStatement pstmt = con.prepareStatement(
                        "SELECT * FROM `" + TABLE + "`" +
                        " WHERE `hugo_gene_symbol`=?" +
                        " AND `cancer_study_id`=?" +
                        " AND `sample_id` IN (SELECT id FROM " + stagingTable + ")")) {
                    pstmt.setString(1, hugoGeneSymbol);
                    pstmt.setInt(2, cancerStudyId);
                    try (ResultSet rs = pstmt.executeQuery()) {
                        while (rs.next()) {
                            results.add(mapRow(rs));
                        }
                    }
                }
                return results;
            } finally {
                JdbcUtil.closeAll(DaoFacetsGenes.class, con, null, null);
            }
        });
    }

    public static boolean facetsGenesDataExistForCancerStudy(int cancerStudyId) throws DaoException {
        return FacetsDaoUtil.dataExistsForCancerStudy(TABLE, cancerStudyId);
    }

    public static void deleteFacetsGenesDataForSamples(int cancerStudyId, Set<Integer> sampleIds) throws DaoException {
        FacetsDaoUtil.deleteDataForSamples(TABLE, cancerStudyId, sampleIds);
    }

    private static FacetsGeneLevelRecord mapRow(ResultSet rs) throws SQLException {
        FacetsGeneLevelRecord rec = new FacetsGeneLevelRecord(
                rs.getInt("cancer_study_id"),
                rs.getInt("sample_id"),
                rs.getString("hugo_gene_symbol"),
                rs.getLong("entrez_gene_id"),
                rs.getString("chr"),
                rs.getLong("start"),
                rs.getLong("end"),
                getNullableDouble(rs, "tcn"),
                getNullableDouble(rs, "lcn"),
                getNullableDouble(rs, "cellular_fraction"),
                getNullableDouble(rs, "purity"));
        rec.setId(rs.getLong("gene_id"));
        return rec;
    }

    private static Double getNullableDouble(ResultSet rs, String column) throws SQLException {
        double value = rs.getDouble(column);
        return rs.wasNull() ? null : value;
    }
}
