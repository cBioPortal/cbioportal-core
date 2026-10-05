package org.mskcc.cbio.portal.dao;

import java.sql.*;
import java.util.HashSet;
import java.util.Set;
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

    /**
     * resource_data_id comes from the shared sequence machinery rather than a local counter.
     * ClickHouse has no AUTO_INCREMENT, and seeding from the system clock does not guarantee
     * uniqueness: the clock can be wrong or move backwards, and an import on a second machine
     * knows nothing about ids the first one issued. ClickHouseAutoIncrement seeds from
     * max(persisted value, current table max) and persists as it goes.
     */
    private static final String RESOURCE_DATA_SEQUENCE = "seq_resource_data";


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
            String metadata) throws DaoException {

        if (ClickHouseBulkLoader.isBulkLoad()) {
            // Column order matches DESCRIBE TABLE resource_data:
            // resource_data_id, resource_id, cancer_study_id, entity_type,
            // patient_id, sample_id, url, display_name, type, metadata
            //
            // Nulls are passed through rather than substituted with "": the loader encodes a
            // null as \N, which ClickHouse stores as a real NULL, and these columns are
            // Nullable(String). The distinction matters. A patient-level row carries no sample,
            // and queries select those rows with "SAMPLE_ID IS NULL"; an empty string satisfies
            // neither that nor "SAMPLE_ID IN (...)", so such rows would be silently dropped from
            // every cohort-scoped query and miscounted by the distinct-sample count.
            ClickHouseBulkLoader.getClickHouseBulkLoader(RESOURCE_DATA_TABLE).insertRecord(
                Long.toString(ClickHouseAutoIncrement.nextId(RESOURCE_DATA_SEQUENCE)),
                resourceId,
                Integer.toString(cancerStudyId),
                entityType,
                patientId,
                sampleId,
                url,
                displayName,
                type,
                metadata
            );
            return 1;
        }

        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            // resource_data_id has no server-side default: allocate it from the same
            // sequence as the bulk-load path so both paths produce unique row IDs.
            long resourceDataId = ClickHouseAutoIncrement.nextId(RESOURCE_DATA_SEQUENCE);
            con = JdbcUtil.getDbConnection(DaoResourceData.class);
            pstmt = con.prepareStatement(
                "INSERT INTO `" + RESOURCE_DATA_TABLE + "` "
                + "(`resource_data_id`,`resource_id`,`cancer_study_id`,`entity_type`,"
                + "`patient_id`,`sample_id`,`url`,"
                + "`display_name`,`type`,`metadata`) "
                + "VALUES (?,?,?,?,?,?,?,?,?,?)"
            );
            pstmt.setLong(1, resourceDataId);
            pstmt.setString(2, resourceId);
            pstmt.setInt(3, cancerStudyId);
            pstmt.setString(4, entityType);
            pstmt.setString(5, patientId);
            pstmt.setString(6, sampleId);
            pstmt.setString(7, url);
            pstmt.setString(8, displayName);
            pstmt.setString(9, type);
            pstmt.setString(10, metadata);
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
        // Queued, not flushed: flushAll() executes every datatype's pending deletions, and there
        // is no dependency here that needs them applied now. The ids collected above belong only
        // to rows that already existed, and ClickHouseAutoIncrement seeds each counter from
        // max(persisted, current table max), so the rows this import is about to insert get ids
        // above every one of them. The deletion is therefore correct whenever it runs.
        ClickHouseBulkDeleter.getBulkDeleter(RESOURCE_DATA_TABLE, "resource_data_id").addIds(idsToDelete);
    }

    /**
     * Queues deletion of the {@code resource_data} rows attached to the given samples of a
     * study. The rows are removed by the next {@link ClickHouseBulkDeleter#flushAll()}, so
     * callers can combine this with the other per-sample deletes they already batch.
     *
     * @param cancerStudyId   internal cancer-study ID
     * @param sampleStableIds stable sample IDs; a no-op if empty
     */
    public static void addSampleResourceDataToBulkDelete(int cancerStudyId, Set<String> sampleStableIds)
            throws DaoException {
        addEntityResourceDataToBulkDelete(cancerStudyId, "sample_id", sampleStableIds);
    }

    /**
     * Queues deletion of every {@code resource_data} row attached to the given patients of a
     * study, including sample-level rows that carry the patient ID. The rows are removed by
     * the next {@link ClickHouseBulkDeleter#flushAll()}.
     *
     * @param cancerStudyId    internal cancer-study ID
     * @param patientStableIds stable patient IDs; a no-op if empty
     */
    public static void addPatientResourceDataToBulkDelete(int cancerStudyId, Set<String> patientStableIds)
            throws DaoException {
        addEntityResourceDataToBulkDelete(cancerStudyId, "patient_id", patientStableIds);
    }

    private static void addEntityResourceDataToBulkDelete(int cancerStudyId, String entityColumn,
            Set<String> stableIds) throws DaoException {
        if (stableIds == null || stableIds.isEmpty()) {
            return;
        }
        Set<Long> idsToDelete = findResourceDataIds(cancerStudyId, entityColumn, stableIds);
        if (!idsToDelete.isEmpty()) {
            ClickHouseBulkDeleter.getBulkDeleter(RESOURCE_DATA_TABLE, "resource_data_id").addIds(idsToDelete);
        }
    }

    private static Set<Long> findResourceDataIds(int cancerStudyId, Set<String> resourceIds) throws DaoException {
        return findResourceDataIds(cancerStudyId, "resource_id", resourceIds);
    }

    private static Set<Long> findResourceDataIds(int cancerStudyId, String filterColumn, Set<String> values)
            throws DaoException {
        Set<Long> ids = new HashSet<>();
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoResourceData.class);
            String placeholders = values.stream().map(id -> "?").collect(Collectors.joining(","));
            pstmt = con.prepareStatement(
                "SELECT `resource_data_id` FROM `" + RESOURCE_DATA_TABLE + "` "
                + "WHERE `cancer_study_id` = ? AND `" + filterColumn + "` IN (" + placeholders + ")"
            );
            int paramIndex = 1;
            pstmt.setInt(paramIndex++, cancerStudyId);
            for (String value : values) {
                pstmt.setString(paramIndex++, value);
            }
            rs = pstmt.executeQuery();
            while (rs.next()) {
                ids.add(rs.getLong("resource_data_id"));
            }
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoResourceData.class, con, pstmt, rs);
        }
        return ids;
    }
}
