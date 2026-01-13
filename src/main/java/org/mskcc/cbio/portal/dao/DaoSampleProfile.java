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

import java.sql.*;
import java.util.*;
import org.mskcc.cbio.portal.model.Sample;

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
        PreparedStatement pstmt = null;
        try {
            con = JdbcUtil.getDbConnection(DaoSampleProfile.class);

            pstmt = con.prepareStatement
                    ("INSERT INTO sample_profile (`SAMPLE_ID`, `GENETIC_PROFILE_ID`, `PANEL_ID`)" +
                            " VALUES" +
                            String.join(",", Collections.nCopies(idTuples.size(), " (?,?,?)")) +
                            " ON DUPLICATE KEY UPDATE `PANEL_ID` = VALUES(`PANEL_ID`);");
            int parameterIndex = 1;
            for (SampleProfileTuple idTuple : idTuples) {
                pstmt.setInt(parameterIndex++, idTuple.sampleId());
                pstmt.setInt(parameterIndex++, idTuple.geneticProfileId());
                if (idTuple.panelId() != null) {
                    pstmt.setInt(parameterIndex, idTuple.panelId());
                } else {
                    pstmt.setNull(parameterIndex, java.sql.Types.INTEGER);
                }
                parameterIndex++;
            }
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoSampleProfile.class, con, pstmt, null);
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
                    ("SELECT * FROM sample_profile WHERE SAMPLE_ID = ? AND GENETIC_PROFILE_ID = ?");
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
                    ("SELECT PANEL_ID FROM sample_profile WHERE SAMPLE_ID = ? AND GENETIC_PROFILE_ID = ?");
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
                throw new NoSuchElementException("No sample_profile with SAMPLE_ID=" + sampleId + " and GENETIC_PROFILE_ID=" + geneticProfileId);
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
                    ("SELECT count(*) FROM sample_profile WHERE GENETIC_PROFILE_ID = ?");
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
            pstmt = con.prepareStatement("SELECT GENETIC_PROFILE_ID FROM sample_profile WHERE SAMPLE_ID = ?");
            pstmt.setInt(1, sampleId);
            rs = pstmt.executeQuery();
            if( rs.next() ) {
               return rs.getInt("GENETIC_PROFILE_ID");
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
                    ("SELECT * FROM sample_profile WHERE GENETIC_PROFILE_ID = ?");
            pstmt.setInt(1, geneticProfileId);
            rs = pstmt.executeQuery();
            ArrayList<Integer> sampleIds = new ArrayList<Integer>();
            while (rs.next()) {
                Sample sample = DaoSample.getSampleById(rs.getInt("SAMPLE_ID"));
                sampleIds.add(rs.getInt("SAMPLE_ID"));
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
                sampleIds.add(rs.getInt("SAMPLE_ID"));
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
            pstmt = con.prepareStatement("select count(*) from sample_profile where PANEL_ID = ?");
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
