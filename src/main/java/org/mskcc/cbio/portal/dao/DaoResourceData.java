package org.mskcc.cbio.portal.dao;

import java.sql.*;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Data Access Object for the unified {@code resource_data} table.
 *
 * <p>Legacy split tables (resource_sample, resource_patient, resource_study) are no
 * longer written to by the importer.  Existing data in those tables can be migrated
 * to resource_data via the migration.sql script bundled with the backend.</p>
 *
 * <p>{@code resource_data} rows are never updated in place: ClickHouse's MergeTree
 * engine makes single-row updates expensive, and the importer has no way to know
 * which existing row (if any) corresponds to a given input line. Instead, a re-import
 * of a resource file for a study first deletes any existing rows for the resource IDs
 * present in that file (see {@link #deleteResourceData(int, Set)}), then re-inserts
 * everything from the file. This mirrors the delete-then-insert pattern used elsewhere
 * in the importer for reloadable data types, and ensures curator corrections to an
 * existing file (e.g. filling in previously-empty metadata) are reflected instead of
 * silently accumulating as duplicate rows.</p>
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

    /**
     * Deletes all existing {@code resource_data} rows for the given study that belong to any
     * of the given resource IDs. Intended to be called before re-inserting a resource file for
     * that study/resource-ID set, so a re-import replaces stale rows instead of duplicating them.
     *
     * @param cancerStudyId internal cancer-study ID
     * @param resourceIds   resource IDs present in the file being (re-)imported; a no-op if empty
     */
    public static void deleteResourceData(int cancerStudyId, Set<String> resourceIds) throws DaoException {
        if (resourceIds == null || resourceIds.isEmpty()) {
            return;
        }
        Set<Long> idsToDelete = findResourceDataIds(cancerStudyId, resourceIds);
        if (idsToDelete.isEmpty()) {
            return;
        }
        ClickHouseBulkDeleter.getBulkDeleter(RESOURCE_DATA_TABLE, "RESOURCE_DATA_ID").addIds(idsToDelete);
        ClickHouseBulkDeleter.flushAll();
    }

    private static Set<Long> findResourceDataIds(int cancerStudyId, Set<String> resourceIds) throws DaoException {
        Set<Long> ids = new HashSet<>();
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoResourceData.class);
            String placeholders = resourceIds.stream().map(id -> "?").collect(Collectors.joining(","));
            pstmt = con.prepareStatement(
                "SELECT `RESOURCE_DATA_ID` FROM `" + RESOURCE_DATA_TABLE + "` "
                + "WHERE `CANCER_STUDY_ID` = ? AND `RESOURCE_ID` IN (" + placeholders + ")"
            );
            int paramIndex = 1;
            pstmt.setInt(paramIndex++, cancerStudyId);
            for (String resourceId : resourceIds) {
                pstmt.setString(paramIndex++, resourceId);
            }
            rs = pstmt.executeQuery();
            while (rs.next()) {
                ids.add(rs.getLong("RESOURCE_DATA_ID"));
            }
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoResourceData.class, con, pstmt, rs);
        }
        return ids;
    }
}

