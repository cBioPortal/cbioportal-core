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

import org.mskcc.cbio.portal.model.CopyNumberSegmentFile;

import org.apache.commons.lang3.StringUtils;

import java.sql.*;
import java.util.*;

public final class DaoCopyNumberSegmentFile {
    private static final String COPY_NUMBER_SEG_FILE_SEQUENCE = "seq_copy_number_seg_file";
    private DaoCopyNumberSegmentFile() {}
    
    public static int addCopyNumberSegmentFile(CopyNumberSegmentFile copySegFile) throws DaoException
    {
        Connection con = null;
        PreparedStatement pstmt = null;
        try {
            con = JdbcUtil.getDbConnection(DaoCopyNumberSegmentFile.class);
            long fileId = ClickHouseAutoIncrement.nextId(COPY_NUMBER_SEG_FILE_SEQUENCE);
            pstmt = con.prepareStatement
                    ("INSERT INTO copy_number_seg_file (`seg_file_id`, `cancer_study_id`, `reference_genome_id`, `description`,`filename`)"
                     + " VALUES (?,?,?,?,?)");
            pstmt.setLong(1, fileId);
            pstmt.setInt(2, copySegFile.cancerStudyId);
            pstmt.setString(3, copySegFile.referenceGenomeId.toString());
            pstmt.setString(4, copySegFile.description);
            pstmt.setString(5, copySegFile.filename);
            pstmt.executeUpdate();
            return (int) fileId;
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoCopyNumberSegmentFile.class, con, pstmt, null);
        }
    }

    public static CopyNumberSegmentFile getCopyNumberSegmentFile(int cancerStudyId) throws DaoException
    {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoCopyNumberSegmentFile.class);
            pstmt = con.prepareStatement("SELECT * from copy_number_seg_file WHERE `cancer_study_id` = ?");
            pstmt.setInt(1, cancerStudyId);
            rs = pstmt.executeQuery();
            if (rs.next()) {
                CopyNumberSegmentFile cnsf = new CopyNumberSegmentFile();
                cnsf.segFileId = rs.getInt("seg_file_id");
                cnsf.cancerStudyId = cancerStudyId;
                cnsf.referenceGenomeId = CopyNumberSegmentFile.ReferenceGenomeId.valueOf(rs.getString("reference_genome_id"));
                cnsf.description = rs.getString("description");
                cnsf.filename = rs.getString("filename");
                if (rs.next()) {
                    throw new SQLException("More than one row was returned.");
                }
                return cnsf;
            }
            return null;
        }
        catch(SQLException e) {
            throw new DaoException(e);
        }
        finally {
            JdbcUtil.closeAll(DaoCopyNumberSegmentFile.class, con, pstmt, rs);
        }
    }

    public static void deleteAllRecords() throws DaoException
    {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoCopyNumberSegmentFile.class);
            pstmt = con.prepareStatement("TRUNCATE TABLE copy_number_seg_file");
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoCopyNumberSegmentFile.class, con, pstmt, rs);
        }
    }
}
