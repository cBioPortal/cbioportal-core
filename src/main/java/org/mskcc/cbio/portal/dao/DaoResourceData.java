package org.mskcc.cbio.portal.dao;

import java.sql.*;
import java.util.*;
import org.apache.commons.lang3.StringUtils;
import org.mskcc.cbio.portal.model.ResourceBaseData;

/**
 * Data Access Object for `resource_node` table
 */
public final class DaoResourceData {

    public static final String RESOURCE_NODE_TABLE = "resource_node";

    private static final String INSERT = "INSERT INTO " + RESOURCE_NODE_TABLE
            + "(`resource_id`,`cancer_study_id`,`entity_type`,`entity_internal_id`,`display_name`,`url`,`metadata`,`group_path`)"
            + " VALUES(?,?,?,?,?,?,?,?)";

    private DaoResourceData() {
    }

    public static int addSampleDatum(int internalSampleId, int cancerStudyId, String resourceId, String displayName, String url, String metadata, String groupPath) throws DaoException {
        return addDatum(internalSampleId, cancerStudyId, "SAMPLE", resourceId, displayName, url, metadata, groupPath);
    }

    public static int addPatientDatum(int internalPatientId, int cancerStudyId, String resourceId, String displayName, String url, String metadata, String groupPath) throws DaoException {
        return addDatum(internalPatientId, cancerStudyId, "PATIENT", resourceId, displayName, url, metadata, groupPath);
    }

    public static int addStudyDatum(int internalStudyId, int cancerStudyId, String resourceId, String displayName, String url, String metadata, String groupPath) throws DaoException {
        return addDatum(internalStudyId, cancerStudyId, "STUDY", resourceId, displayName, url, metadata, groupPath);
    }

    public static int addDatum(int internalId, int cancerStudyId, String entityType,
            String resourceId, String displayName, String url, String metadata, String groupPath) throws DaoException {
        if (ClickHouseBulkLoader.isBulkLoad()) {
            ClickHouseBulkLoader.getClickHouseBulkLoader(RESOURCE_NODE_TABLE).insertRecord(
                    resourceId,
                    Integer.toString(cancerStudyId),
                    entityType,
                    Integer.toString(internalId),
                    displayName,
                    url,
                    metadata,
                    groupPath);
            return 1;
        }

        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoResourceData.class);
            pstmt = con.prepareStatement(INSERT);
            pstmt.setString(1, resourceId);
            pstmt.setInt(2, cancerStudyId);
            pstmt.setString(3, entityType);
            pstmt.setInt(4, internalId);
            pstmt.setString(5, displayName);
            pstmt.setString(6, url);
            if (metadata != null) {
                pstmt.setString(7, metadata);
            } else {
                pstmt.setNull(7, Types.VARCHAR);
            }
            if (groupPath != null) {
                pstmt.setString(8, groupPath);
            } else {
                pstmt.setNull(8, Types.VARCHAR);
            }
            return pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoResourceData.class, con, pstmt, rs);
        }
    }

    public static List<ResourceBaseData> getDataByPatientId(int cancerStudyId, String patientId) throws DaoException {
        List<Integer> internalIds = new ArrayList<Integer>();
        internalIds.add(DaoPatient.getPatientByCancerStudyAndPatientId(cancerStudyId, patientId).getInternalId());
        return getDataByInternalIds(cancerStudyId, "PATIENT", internalIds);
    }

    private static List<ResourceBaseData> getDataByInternalIds(int internalCancerStudyId, String entityType, List<Integer> internalIds) throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        List<ResourceBaseData> resources = new ArrayList<ResourceBaseData>();
        String sql = "SELECT * FROM " + RESOURCE_NODE_TABLE
                + " WHERE `entity_type` = '" + entityType + "'"
                + " AND `entity_internal_id` IN (" + generateIdsSql(internalIds) + ")";

        try {
            con = JdbcUtil.getDbConnection(DaoResourceData.class);
            pstmt = con.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                resources.add(extract(internalCancerStudyId, entityType, rs));
            }
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoResourceData.class, con, pstmt, rs);
        }

        return resources;
    }

    private static String generateIdsSql(Collection<Integer> ids) {
        return "'" + StringUtils.join(ids, "','") + "'";
    }

    private static ResourceBaseData extract(int internalCancerStudyId, String entityType, ResultSet rs) throws SQLException {
        int internalId = rs.getInt("entity_internal_id");
        String stableId = getStableIdFromInternalId(entityType, internalId);
        return new ResourceBaseData(
                internalCancerStudyId,
                stableId,
                rs.getString("resource_id"),
                rs.getString("url"),
                rs.getString("metadata"),
                rs.getString("group_path"));
    }

    private static String getStableIdFromInternalId(String entityType, int internalId) {
        if ("SAMPLE".equals(entityType)) {
            return DaoSample.getSampleById(internalId).getStableId();
        } else {
            return DaoPatient.getPatientById(internalId).getStableId();
        }
    }
}
