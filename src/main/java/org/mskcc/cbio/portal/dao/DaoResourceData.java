package org.mskcc.cbio.portal.dao;

import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.StringUtils;
import org.mskcc.cbio.portal.model.ResourceBaseData;

/**
 * Data Access Object for `resource` tables (legacy split tables + unified resource_data table).
 */
public final class DaoResourceData {

    public static final String RESOURCE_SAMPLE_TABLE = "resource_sample";
    public static final String RESOURCE_PATIENT_TABLE = "resource_patient";
    public static final String RESOURCE_STUDY_TABLE = "resource_study";
    public static final String RESOURCE_DATA_TABLE = "resource_data";

    private static final String SAMPLE_INSERT = "INSERT INTO " + RESOURCE_SAMPLE_TABLE
            + "(`internal_id`,`resource_id`,`url` VALUES(?,?,?)";
    private static final String PATIENT_INSERT = "INSERT INTO " + RESOURCE_PATIENT_TABLE
            + "(`internal_id`,`resource_id`,`url` VALUES(?,?,?)";
    private static final String STUDY_INSERT = "INSERT INTO " + RESOURCE_STUDY_TABLE
            + "(`internal_id`,`resource_id`,`url` VALUES(?,?,?)";

    // Monotonically increasing ID for bulk-load inserts into resource_data
    // (ClickHouse has no AUTO_INCREMENT; uniqueness is not enforced by the engine)
    private static final AtomicLong resourceDataIdSeq = new AtomicLong(System.currentTimeMillis());

    private DaoResourceData() {
    }

    // ── Legacy table helpers ──────────────────────────────────────────────────

    public static int addSampleDatum(int internalSampleId, String resourceId, String url) throws DaoException {
        return addDatum(SAMPLE_INSERT, RESOURCE_SAMPLE_TABLE, internalSampleId, resourceId, url);
    }

    public static int addPatientDatum(int internalPatientId, String resourceId, String url) throws DaoException {
        return addDatum(PATIENT_INSERT, RESOURCE_PATIENT_TABLE, internalPatientId, resourceId, url);
    }

    public static int addStudyDatum(int internalStudyId, String resourceId, String url) throws DaoException {
        return addDatum(STUDY_INSERT, RESOURCE_STUDY_TABLE, internalStudyId, resourceId, url);
    }

    public static int addDatum(String query, String tableName, int internalId, String resourceId, String url)
            throws DaoException {
        if (ClickHouseBulkLoader.isBulkLoad()) {
            ClickHouseBulkLoader.getClickHouseBulkLoader(tableName).insertRecord(Integer.toString(internalId), resourceId, url);
            return 1;
        }

        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoResourceData.class);

            pstmt = con.prepareStatement(query);
            pstmt.setInt(1, internalId);
            pstmt.setString(2, resourceId);
            pstmt.setString(3, url);
            
            return pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoResourceData.class, con, pstmt, rs);
        }
    }

    // ── Unified resource_data table ───────────────────────────────────────────

    /**
     * Inserts a row into the unified {@code resource_data} table.
     *
     * @param cancerStudyId  internal cancer-study ID
     * @param resourceId     resource identifier (must match a resource_definition row)
     * @param entityType     one of "PATIENT", "SAMPLE", or "STUDY"
     * @param patientId      stable patient ID (null for STUDY-level records)
     * @param sampleId       stable sample ID (null for PATIENT/STUDY-level records)
     * @param url            URL of the resource
     * @param displayName    optional display name override (may be null)
     * @param type           optional type override (may be null)
     * @param priority       display priority
     * @param metadata       optional JSON metadata string (may be null)
     */
    public static int addResourceDatum(
            int cancerStudyId,
            String resourceId,
            String entityType,
            String patientId,
            String sampleId,
            String url,
            String displayName,
            String type,
            int priority,
            String metadata) throws DaoException {

        if (ClickHouseBulkLoader.isBulkLoad()) {
            // Column order matches DESCRIBE TABLE resource_data:
            // RESOURCE_DATA_ID, RESOURCE_ID, CANCER_STUDY_ID, ENTITY_TYPE,
            // PATIENT_ID, SAMPLE_ID, URL, DISPLAY_NAME, TYPE, METADATA, PRIORITY
            ClickHouseBulkLoader.getClickHouseBulkLoader(RESOURCE_DATA_TABLE).insertRecord(
                Long.toString(resourceDataIdSeq.incrementAndGet()),
                resourceId,
                Integer.toString(cancerStudyId),
                entityType,
                patientId != null ? patientId : "",
                sampleId != null ? sampleId : "",
                url,
                displayName != null ? displayName : "",
                type != null ? type : "",
                metadata != null ? metadata : "",
                Integer.toString(priority)
            );
            return 1;
        }

        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoResourceData.class);
            pstmt = con.prepareStatement(
                "INSERT INTO `" + RESOURCE_DATA_TABLE + "` "
                + "(`RESOURCE_ID`,`CANCER_STUDY_ID`,`ENTITY_TYPE`,"
                + "`PATIENT_ID`,`SAMPLE_ID`,`URL`,"
                + "`DISPLAY_NAME`,`TYPE`,`METADATA`,`PRIORITY`) "
                + "VALUES (?,?,?,?,?,?,?,?,?,?)"
            );
            pstmt.setString(1, resourceId);
            pstmt.setInt(2, cancerStudyId);
            pstmt.setString(3, entityType);
            pstmt.setString(4, patientId);
            pstmt.setString(5, sampleId);
            pstmt.setString(6, url);
            pstmt.setString(7, displayName);
            pstmt.setString(8, type);
            pstmt.setString(9, metadata);
            pstmt.setInt(10, priority);
            return pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoResourceData.class, con, pstmt, rs);
        }
    }

    // ── Read helpers ──────────────────────────────────────────────────────────

    public static List<ResourceBaseData> getDataByPatientId(int cancerStudyId, String patientId) throws DaoException
    {
        List<Integer> internalIds = new ArrayList<Integer>();
        internalIds.add(DaoPatient.getPatientByCancerStudyAndPatientId(cancerStudyId, patientId).getInternalId());
        return getDataByInternalIds(cancerStudyId, RESOURCE_PATIENT_TABLE, internalIds);
    }

    private static List<ResourceBaseData> getDataByInternalIds(int internalCancerStudyId, String table, List<Integer> internalIds) throws DaoException
    {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        List<ResourceBaseData> resources = new ArrayList<ResourceBaseData>();
        String sql = ("SELECT * FROM " + table + " WHERE `internal_id` IN " +
            "(" + generateIdsSql(internalIds) + ")");

        try {
            con = JdbcUtil.getDbConnection(DaoResourceData.class);
            pstmt = con.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                resources.add(extract(table, internalCancerStudyId, rs));
            }
        }
        catch (SQLException e) {
            throw new DaoException(e);
        }
        finally {
            JdbcUtil.closeAll(DaoResourceData.class, con, pstmt, rs);
        }

        return resources;
    }

    private static String generateIdsSql(Collection<Integer> ids) {
        return "'" + StringUtils.join(ids, "','") + "'";
    }

    private static ResourceBaseData extract(String table, int internalCancerStudyId, ResultSet rs) throws SQLException {
        String stableId = getStableIdFromInternalId(table, rs.getInt("internal_id"));
        return new ResourceBaseData(internalCancerStudyId, stableId, rs.getString("resource_id"), rs.getString("url"));
    }

    private static String getStableIdFromInternalId(String table, int internalId) {
        if (table.equals(RESOURCE_SAMPLE_TABLE)) {
            return DaoSample.getSampleById(internalId).getStableId();
        } else {
            return DaoPatient.getPatientById(internalId).getStableId();
        }
    }
}
