package org.mskcc.cbio.portal.dao;

import java.sql.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Data Access Object for the unified {@code resource_data} table.
 *
 * <p>Legacy split tables (resource_sample, resource_patient, resource_study) are no
 * longer written to by the importer.  Existing data in those tables can be migrated
 * to resource_data via the migration.sql script bundled with the backend.</p>
 */
public final class DaoResourceData {

    public static final String RESOURCE_DATA_TABLE = "resource_data";

    // Monotonically increasing ID for bulk-load inserts into resource_data.
    // ClickHouse has no AUTO_INCREMENT; uniqueness is not enforced by the engine.
    private static final AtomicLong resourceDataIdSeq = new AtomicLong(System.currentTimeMillis());

    private DaoResourceData() {
    }

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
     * @param type           optional type hint, e.g. IMAGE/LINK/PDF (may be null)
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
                patientId  != null ? patientId  : "",
                sampleId   != null ? sampleId   : "",
                url,
                displayName != null ? displayName : "",
                type       != null ? type       : "",
                metadata   != null ? metadata   : "",
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
}

