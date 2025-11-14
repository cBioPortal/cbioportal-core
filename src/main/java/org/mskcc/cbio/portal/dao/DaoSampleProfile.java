/*
 * Copyright (c) 2015 - 2022 Memorial Sloan Kettering Cancer Center.
 *
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY, WITHOUT EVEN THE IMPLIED WARRANTY OF MERCHANTABILITY OR FITNESS
 * FOR A PARTICULAR PURPOSE. The software and documentation provided hereunder
 * is on an "as is" basis, and Memorial Sloan Kettering Cancer Center has no
 * obligations to provide maintenance, support, updates, enhancements or
 * modifications. In no event shall Memorial Sloan Kettering Cancer Center be
 * liable to any party for direct, indirect, special, incidental or
 * consequential damages, including lost profits, arising out of the use of this
 * software and its documentation, even if Memorial Sloan Kettering Cancer
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

import org.apache.commons.lang3.StringUtils;
import org.mskcc.cbio.portal.model.CancerStudy;
import org.mskcc.cbio.portal.model.GeneticProfile;
import org.mskcc.cbio.portal.model.Sample;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Data access object for sample_profile table
 */
public final class DaoSampleProfile {
    /**
     * Adds a record to the sample_profile table. This is part of adding records from genetic profiles.
     * Can use the bulk loader.
     */
    private DaoSampleProfile() {}

    private static final int NO_SUCH_PROFILE_ID = -1;

    public static void upsertSampleToProfileMapping(Collection<Integer> sampleIds, Integer geneticProfileId, Integer panelId) throws DaoException {
        upsertSampleToProfileMapping(
                sampleIds.stream()
                        .map(sampleId -> new SampleProfileTuple(geneticProfileId, sampleId, panelId)).toList());
    }

    public record SampleProfileTuple(int geneticProfileId, int sampleId, Integer panelId) {}

    public static void upsertSampleToProfileMapping(Collection<SampleProfileTuple> idTuples) throws DaoException {
        if (idTuples.isEmpty()) {
            return;
        }
        Connection con = null;
        PreparedStatement deleteStmt = null;
        PreparedStatement insertStmt = null;
        try {
            con = JdbcUtil.getDbConnection(DaoSampleProfile.class);
            String tuplePlaceholders = String.join(",", Collections.nCopies(idTuples.size(), "(?,?)"));
            deleteStmt = con.prepareStatement(
                "ALTER TABLE sample_profile "
                    + "DELETE WHERE (sample_id, genetic_profile_id) IN (" + tuplePlaceholders + ") "
                    + "SETTINGS mutations_sync=2"
            );
            int parameterIndex = 1;
            for (SampleProfileTuple idTuple : idTuples) {
                deleteStmt.setInt(parameterIndex++, idTuple.sampleId());
                deleteStmt.setInt(parameterIndex++, idTuple.geneticProfileId());
            }
            deleteStmt.executeUpdate();

            String insertSql = "INSERT INTO sample_profile (`sample_id`, `genetic_profile_id`, `panel_id`) VALUES "
                + String.join(",", Collections.nCopies(idTuples.size(), " (?,?,?)"));
            insertStmt = con.prepareStatement(insertSql);
            parameterIndex = 1;
            for (SampleProfileTuple idTuple : idTuples) {
                insertStmt.setInt(parameterIndex++, idTuple.sampleId());
                insertStmt.setInt(parameterIndex++, idTuple.geneticProfileId());
                if (idTuple.panelId() != null) {
                    insertStmt.setInt(parameterIndex, idTuple.panelId());
                } else {
                    insertStmt.setNull(parameterIndex, java.sql.Types.INTEGER);
                }
                parameterIndex++;
            }
            insertStmt.executeUpdate();
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoSampleProfile.class, null, deleteStmt, null);
            JdbcUtil.closeAll(DaoSampleProfile.class, con, insertStmt, null);
        }
    }

    public static boolean sampleExistsInGeneticProfile(int sampleId, int geneticProfileId)
            throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        try {
            con = JdbcUtil.getDbConnection(DaoSampleProfile.class);
            pstmt = con.prepareStatement
                    ("SELECT * FROM sample_profile WHERE sample_id = ? AND genetic_profile_id = ?");
            pstmt.setInt(1, sampleId);
            pstmt.setInt(2, geneticProfileId);
            rs = pstmt.executeQuery();
            return (rs.next());
        } catch (NullPointerException e) {
            throw new DaoException(e);
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoSampleProfile.class, con, pstmt, rs);
        }
    }

    public static Integer getPanelId(int sampleId, int geneticProfileId)
            throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        try {
            con = JdbcUtil.getDbConnection(DaoSampleProfile.class);
            pstmt = con.prepareStatement
                    ("SELECT panel_id FROM sample_profile WHERE sample_id = ? AND genetic_profile_id = ?");
            pstmt.setInt(1, sampleId);
            pstmt.setInt(2, geneticProfileId);
            rs = pstmt.executeQuery();
            if (rs.next()) {
                int panelId = rs.getInt(1);
                if (rs.wasNull()) {
                    return null;
                }
                return panelId;
            } else {
                throw new NoSuchElementException("No sample_profile with SAMPLE_ID=" + sampleId + " and genetic_profile_id=" + geneticProfileId);
            }
        } catch (NoSuchElementException | SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoSampleProfile.class, con, pstmt, rs);
        }
    }

    public static int countSamplesInProfile(int geneticProfileId) throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoSampleProfile.class);
            pstmt = con.prepareStatement
                    ("SELECT count(*) FROM sample_profile WHERE genetic_profile_id = ?");
            pstmt.setInt(1, geneticProfileId);
            rs = pstmt.executeQuery();
            if (rs.next()) {
                return rs.getInt(1);
            }
            return 0;
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoSampleProfile.class, con, pstmt, rs);
        }
    }

    public static int getProfileIdForSample(int sampleId) throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        try {
            con = JdbcUtil.getDbConnection(DaoSampleProfile.class);
            pstmt = con.prepareStatement("SELECT genetic_profile_id FROM sample_profile WHERE sample_id = ?");
            pstmt.setInt(1, sampleId);
            rs = pstmt.executeQuery();
            if( rs.next() ) {
               return rs.getInt("genetic_profile_id");
            }else{
               return NO_SUCH_PROFILE_ID;
            }
        } catch (NullPointerException e) {
            throw new DaoException(e);
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoSampleProfile.class, con, pstmt, rs);
        }
    }

    public static ArrayList<Integer> getAllSampleIdsInProfile(int geneticProfileId) throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoSampleProfile.class);
            pstmt = con.prepareStatement
                    ("SELECT * FROM sample_profile WHERE genetic_profile_id = ?");
            pstmt.setInt(1, geneticProfileId);
            rs = pstmt.executeQuery();
            ArrayList<Integer> sampleIds = new ArrayList<Integer>();
            while (rs.next()) {
                Sample sample = DaoSample.getSampleById(rs.getInt("sample_id"));
                sampleIds.add(rs.getInt("sample_id"));
            }
            return sampleIds;
        } catch (NullPointerException e) {
            throw new DaoException(e);
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoSampleProfile.class, con, pstmt, rs);
        }
    }

    public static ArrayList<Integer> getAllSamples() throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoSampleProfile.class);
            pstmt = con.prepareStatement
                    ("SELECT * FROM sample_profile");
            rs = pstmt.executeQuery();
            ArrayList<Integer> sampleIds = new ArrayList<Integer>();
            while (rs.next()) {
                sampleIds.add(rs.getInt("sample_id"));
            }
            return sampleIds;
        } catch (NullPointerException e) {
            throw new DaoException(e);
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoSampleProfile.class, con, pstmt, rs);
        }
    }

    /**
     * Counts the number of sequenced cases in each cancer study, returning a list of maps
     * containing the cancer study (full string name), the cancer type (e.g. `brca_tcga`), and the count
     *
     * @return  [ list of maps { cancer_study, cancer_type, num_sequenced_samples } ]
     * @throws DaoException
     * @author Gideon Dresdner <dresdnerg@cbio.mskcc.org>
     *
     */
    public static List<Map<String, Object>> metaData(List<CancerStudy> cancerStudies) throws DaoException {
        // collect all mutationProfileIds
        Map<Integer, GeneticProfile> id2MutationProfile = new HashMap<Integer, GeneticProfile>();
        for (CancerStudy cancerStudy : cancerStudies) {
            GeneticProfile mutationProfile = cancerStudy.getMutationProfile();

            if (mutationProfile != null) {
                // e.g. if cancerStudy == All Cancer Studies
                Integer mutationProfileId = mutationProfile.getGeneticProfileId();
                id2MutationProfile.put(mutationProfileId, mutationProfile);
            }
        }

        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
        try {
            con = JdbcUtil.getDbConnection(DaoMutation.class);

            String sql = "select `genetic_profile_id`, count(`sample_id`) from sample_profile " +
                    " where `genetic_profile_id` in ("+ StringUtils.join(id2MutationProfile.keySet(), ",") + ")" +
                    " group by `genetic_profile_id`";

            pstmt = con.prepareStatement(sql);
            rs = pstmt.executeQuery();

            while (rs.next()) {
                Map<String, Object> datum = new HashMap<String, Object>();

                Integer mutationProfileId = rs.getInt(1);
                Integer numSequencedSamples = rs.getInt(2);

                GeneticProfile mutationProfile = id2MutationProfile.get(mutationProfileId);
                Integer cancerStudyId = mutationProfile.getCancerStudyId();
                CancerStudy cancerStudy = DaoCancerStudy.getCancerStudyByInternalId(cancerStudyId);
                String cancerStudyName = cancerStudy.getName();
                String cancerType = cancerStudy.getTypeOfCancerId();

                datum.put("cancer_study", cancerStudyName);
                datum.put("cancer_type", cancerType);
                datum.put("color", DaoTypeOfCancer.getTypeOfCancerById(cancerType).getDedicatedColor());
                datum.put("num_sequenced_samples", numSequencedSamples);

                data.add(datum);
            }

        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoMutation.class, con, pstmt, rs);
        }

    return data;
    }

    public static void deleteAllRecords() throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoSampleProfile.class);
            pstmt = con.prepareStatement("TRUNCATE TABLE sample_profile");
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoSampleProfile.class, con, pstmt, rs);
        }
    }

    public static void deleteRecords(List<Integer> sampleIds, List<Integer> profileIds) throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoSampleProfile.class);
            for (int i = 0; i < sampleIds.size(); i++) {
                pstmt = con.prepareCall("DELETE FROM sample_profile WHERE sample_id = ? and genetic_profile_id = ?");
                pstmt.setInt(1, sampleIds.get(i));
                pstmt.setInt(2, profileIds.get(i));
                pstmt.executeUpdate();
            }
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoSampleProfile.class, con, pstmt, rs);
        }
    }

    public static boolean sampleProfileMappingExistsByPanel(Integer panelId) throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoSampleProfile.class);
            pstmt = con.prepareStatement("select count(*) from sample_profile where panel_id = ?");
            pstmt.setInt(1, panelId);
            rs = pstmt.executeQuery();
            if (rs.next()) {
                return (rs.getInt(1) > 0);
            }
            else {
                return false;
            }
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoSampleProfile.class, con, pstmt, rs);
        }
    }

}
