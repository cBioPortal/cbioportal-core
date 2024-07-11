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

package org.mskcc.cbio.portal.dao;

import org.mskcc.cbio.portal.model.*;

import java.sql.*;
import java.util.*;

/**
 * Data access object for patient_List table
 */
public class DaoSampleList {

    private static final String DELETE_SAMPLE_LIST_LIST = "DELETE FROM sample_list_list WHERE `LIST_ID` = ?";

    /**
	 * Adds record to sample_list table.
	 */
    public int addSampleList(SampleList sampleList) throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        int rows;
        try {
            con = JdbcUtil.getDbConnection(DaoSampleList.class);

            pstmt = con.prepareStatement("INSERT INTO sample_list (`STABLE_ID`, `CANCER_STUDY_ID`, `NAME`, `CATEGORY`," +
                    "`DESCRIPTION`)" + " VALUES (?,?,?,?,?)", Statement.RETURN_GENERATED_KEYS);
            pstmt.setString(1, sampleList.getStableId());
            pstmt.setInt(2, sampleList.getCancerStudyId());
            pstmt.setString(3, sampleList.getName());
            pstmt.setString(4, sampleList.getSampleListCategory().getCategory());
            pstmt.setString(5, sampleList.getDescription());
            rows = pstmt.executeUpdate();
            try (ResultSet generatedKey = pstmt.getGeneratedKeys()) {
                if (generatedKey.next()) {
                    int listId = generatedKey.getInt(1);
                    rows += addSampleListList(sampleList.getCancerStudyId(), listId, sampleList.getSampleList(), con);
                } else {
                    throw new DaoException("Creating sample list failed, no ID obtained.");
                }
            }
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoSampleList.class, con, pstmt, null);
        }
        
        return rows;
    }

	/**
	 * Given a patient list by stable Id, returns a patient list.
	 */
    public SampleList getSampleListByStableId(String stableId) throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoSampleList.class);
            pstmt = con.prepareStatement
                    ("SELECT * FROM sample_list WHERE STABLE_ID = ?");
            pstmt.setString(1, stableId);
            rs = pstmt.executeQuery();
            if (rs.next()) {
                SampleList sampleList = extractSampleList(rs);
                sampleList.setSampleList(getSampleListList(sampleList, con));
                return sampleList;
            }
			return null;
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoSampleList.class, con, pstmt, rs);
        }
    }

	/**
	 * Given a patient list ID, returns a patient list.
	 */
    public SampleList getSampleListById(int id) throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoSampleList.class);
            pstmt = con.prepareStatement
                    ("SELECT * FROM sample_list WHERE LIST_ID = ?");
            pstmt.setInt(1, id);
            rs = pstmt.executeQuery();
            if (rs.next()) {
                SampleList sampleList = extractSampleList(rs);
				sampleList.setSampleList(getSampleListList(sampleList, con));
                return sampleList;
            }
			return null;
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoSampleList.class, con, pstmt, rs);
        }
    }

	/**
	 * Given a cancerStudyId, returns all patient list.
	 */
    public ArrayList<SampleList> getAllSampleLists( int cancerStudyId) throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoSampleList.class);

            pstmt = con.prepareStatement
                    ("SELECT * FROM sample_list WHERE CANCER_STUDY_ID = ? ORDER BY NAME");
            pstmt.setInt(1, cancerStudyId);
            rs = pstmt.executeQuery();
            ArrayList<SampleList> list = new ArrayList<SampleList>();
            while (rs.next()) {
                SampleList sampleList = extractSampleList(rs);
                list.add(sampleList);
            }
			// get patient list-list
			for (SampleList sampleList : list) {
				sampleList.setSampleList(getSampleListList(sampleList, con));
			}
            return list;
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoSampleList.class, con, pstmt, rs);
        }
    }

	/**
	 * Returns a list of all patient lists.
	 */
    public ArrayList<SampleList> getAllSampleLists() throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoSampleList.class);
            pstmt = con.prepareStatement
                    ("SELECT * FROM sample_list");
            rs = pstmt.executeQuery();
            ArrayList<SampleList> list = new ArrayList<SampleList>();
            while (rs.next()) {
                SampleList sampleList = extractSampleList(rs);
                list.add(sampleList);
            }
			// get patient list-list
			for (SampleList sampleList : list) {
				sampleList.setSampleList(getSampleListList(sampleList, con));
			}
            return list;
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoSampleList.class, con, pstmt, rs);
        }
    }

	/**
	 * Clears all records from patient list & sample_list_list.
	 */
    public void deleteAllRecords() throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoSampleList.class);
            pstmt = con.prepareStatement("TRUNCATE TABLE sample_list");
            pstmt.executeUpdate();
            pstmt = con.prepareStatement("TRUNCATE TABLE sample_list_list");
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoSampleList.class, con, pstmt, rs);
        }
    }

	/**
	 * Adds record to sample_list_list.
	 */
    private int addSampleListList(int cancerStudyId, int sampleListId, List<String> sampleList, Connection con) throws DaoException {
		
        if (sampleList.isEmpty()) {
            return 0;
        }

        PreparedStatement pstmt  ;
        ResultSet rs = null;
        int skippedPatients = 0;
        try {
            StringBuilder sql = new StringBuilder("INSERT INTO sample_list_list (`LIST_ID`, `SAMPLE_ID`) VALUES ");
            // NOTE - as of 12/12/14, patient lists contain sample ids
            for (String sampleId : sampleList) {
                Sample sample = DaoSample.getSampleByCancerStudyAndSampleId(cancerStudyId, sampleId);
                if (sample == null) {
                    System.out.println("null sample: " + sampleId);
                    ++skippedPatients;
                    continue;
                }
                sql.append("('").append(sampleListId).append("','").append(sample.getInternalId()).append("'),");
            }
            if (skippedPatients == sampleList.size()) {
                return 0;
            }
            sql.deleteCharAt(sql.length()-1);
            pstmt = con.prepareStatement(sql.toString());
            return pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(rs);
        }
    }

    public void updateSampleListList(SampleList sampleList) throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        try {
            con = JdbcUtil.getDbConnection(DaoSampleList.class);
            pstmt = con.prepareStatement(DELETE_SAMPLE_LIST_LIST);
            pstmt.setInt(1, sampleList.getSampleListId());
            pstmt.executeUpdate();

            addSampleListList(sampleList.getCancerStudyId(), sampleList.getSampleListId(), sampleList.getSampleList(), con);
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoSampleList.class, con, pstmt, null);
        }
    }


	/**
	 * Given a patient list object (thus patient list id) gets patient list list.
	 */
	private ArrayList<String> getSampleListList(SampleList sampleList, Connection con) throws DaoException {

        PreparedStatement pstmt  ;
        ResultSet rs = null;
        try {
            pstmt = con.prepareStatement
                    ("SELECT * FROM sample_list_list WHERE LIST_ID = ?");
            pstmt.setInt(1, sampleList.getSampleListId());
            rs = pstmt.executeQuery();
            ArrayList<String> patientIds = new ArrayList<String>();
            while (rs.next()) {
                // NOTE - as of 12/12/14, patient lists contain sample ids
                int sample_id = rs.getInt("SAMPLE_ID");
                Sample sample = DaoSample.getSampleById(sample_id);
				patientIds.add(sample.getStableId());
			}
            return patientIds;
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(rs);
        }
	}

	/**
	 * Given a result set, creates a patient list object.
	 */
    private SampleList extractSampleList(ResultSet rs) throws SQLException {
        SampleList sampleList = new SampleList();
        sampleList.setStableId(rs.getString("STABLE_ID"));
        sampleList.setCancerStudyId(rs.getInt("CANCER_STUDY_ID"));
        sampleList.setName(rs.getString("NAME"));
        sampleList.setSampleListCategory(SampleListCategory.get(rs.getString("CATEGORY")));
        sampleList.setDescription(rs.getString("DESCRIPTION"));
        sampleList.setSampleListId(rs.getInt("LIST_ID"));
        return sampleList;
    }
}
