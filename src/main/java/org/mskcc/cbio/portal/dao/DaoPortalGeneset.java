package org.mskcc.cbio.portal.dao;

import org.mskcc.cbio.portal.model.PortalGeneset;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class DaoPortalGeneset {

    private DaoPortalGeneset() {
    }

    public static List<PortalGeneset> getAllGenesets(Integer pageSize, Integer pageNumber) throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        List<PortalGeneset> genesets = new ArrayList<>();
        try {
            con = JdbcUtil.getDbConnection(DaoPortalGeneset.class);
            Integer offset = calculateOffset(pageSize, pageNumber);
            boolean applyLimit = pageSize != null && pageSize != 0 && offset != null;

            String sql = "SELECT geneset.id AS internal_id, geneset.external_id AS geneset_id, "
                + "geneset.name AS name, geneset.description AS description, geneset.ref_link AS ref_link "
                + "FROM geneset";
            if (applyLimit) {
                sql += " ORDER BY geneset.external_id ASC LIMIT ? OFFSET ?";
            }

            pstmt = con.prepareStatement(sql);
            if (applyLimit) {
                pstmt.setInt(1, pageSize);
                pstmt.setInt(2, offset);
            }
            rs = pstmt.executeQuery();
            while (rs.next()) {
                PortalGeneset geneset = new PortalGeneset();
                geneset.setInternalId(getNullableInteger(rs, "internal_id"));
                geneset.setGenesetId(rs.getString("geneset_id"));
                geneset.setName(rs.getString("name"));
                geneset.setDescription(rs.getString("description"));
                geneset.setRefLink(rs.getString("ref_link"));
                genesets.add(geneset);
            }
            return genesets;
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoPortalGeneset.class, con, pstmt, rs);
        }
    }

    public static String getGenesetVersion() throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoPortalGeneset.class);
            pstmt = con.prepareStatement("SELECT geneset_version FROM info");
            rs = pstmt.executeQuery();
            if (rs.next()) {
                String version = rs.getString(1);
                return version == null ? "" : version;
            }
            return "";
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoPortalGeneset.class, con, pstmt, rs);
        }
    }

    private static Integer calculateOffset(Integer pageSize, Integer pageNumber) {
        if (pageSize == null || pageNumber == null) {
            return null;
        }
        return pageSize * pageNumber;
    }

    private static Integer getNullableInteger(ResultSet rs, String column) throws SQLException {
        int value = rs.getInt(column);
        if (rs.wasNull()) {
            return null;
        }
        return value;
    }
}
