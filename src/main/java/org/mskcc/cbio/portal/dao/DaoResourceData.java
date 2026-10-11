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
     * @param resourceScope  one of "PATIENT", "SAMPLE", or "STUDY"
     * @param patientId      stable patient ID (null for STUDY-level records)
     * @param sampleId       stable sample ID (null for PATIENT/STUDY-level records)
     * @param url            URL of the resource
     * @param displayName    optional display name override (may be null)
     * @param metadata       optional JSON metadata string (may be null)
     */
    public static int addResourceDatum(
            int cancerStudyId,
            String resourceId,
            String resourceScope,
            String patientId,
            String sampleId,
            String url,
            String displayName,
            String metadata) throws DaoException {

        if (ClickHouseBulkLoader.isBulkLoad()) {
            // Column order matches DESCRIBE TABLE resource_data:
            // resource_data_id, resource_id, cancer_study_id, resource_scope,
            // patient_id, sample_id, url, display_name, metadata
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
                resourceScope,
                patientId,
                sampleId,
                url,
                displayName,
                metadata
            );
            return 1;
        }

        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoResourceData.class);
            // resource_data_id comes from the shared sequence here too. Leaving it out of the
            // column list makes ClickHouse default it to 0 for every row this path writes, and
            // deletions select by resource_data_id -- so one row's deletion would take every
            // other row written this way with it.
            pstmt = con.prepareStatement(
                "INSERT INTO `" + RESOURCE_DATA_TABLE + "` "
                + "(`resource_data_id`,`resource_id`,`cancer_study_id`,`resource_scope`,"
                + "`patient_id`,`sample_id`,`url`,"
                + "`display_name`,`metadata`) "
                + "VALUES (?,?,?,?,?,?,?,?,?)"
            );
            pstmt.setLong(1, ClickHouseAutoIncrement.nextId(RESOURCE_DATA_SEQUENCE));
            pstmt.setString(2, resourceId);
            pstmt.setInt(3, cancerStudyId);
            pstmt.setString(4, resourceScope);
            pstmt.setString(5, patientId);
            pstmt.setString(6, sampleId);
            pstmt.setString(7, url);
            pstmt.setString(8, displayName);
            pstmt.setString(9, metadata);
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
        // Queued here, flushed by the caller once the whole file has been read. The ids
        // collected above belong only to rows that already existed, and ClickHouseAutoIncrement
        // seeds each counter from max(persisted, current table max), so the rows this import is
        // about to insert get ids above every one of them: the deletion is correct whichever
        // side of the inserts it lands on.
        ClickHouseBulkDeleter.getBulkDeleter(RESOURCE_DATA_TABLE, "resource_data_id").addIds(idsToDelete);
    }

    /**
     * Deletes the {@code resource_data} rows belonging to the given samples of a study.
     *
     * <p>The legacy {@code resource_sample} table keyed rows by the sample's <em>internal</em> id,
     * so deleting a sample could reach its resources directly. {@code resource_data} stores the
     * <em>stable</em> id instead, and a stable id is unique only within a study, so the lookup has
     * to be scoped by study before anything is deleted.
     *
     * @param cancerStudyId   internal cancer-study ID
     * @param sampleStableIds stable sample IDs being removed; a no-op if empty
     */
    public static void deleteResourceDataForSamples(int cancerStudyId, Set<String> sampleStableIds)
            throws DaoException {
        deleteResourceDataForEntities(cancerStudyId, "sample_id", sampleStableIds);
    }

    /**
     * Deletes the {@code resource_data} rows belonging to the given patients of a study. Only
     * patient-level rows carry a patient id with no sample; a sample's rows go with the sample.
     *
     * @param cancerStudyId    internal cancer-study ID
     * @param patientStableIds stable patient IDs being removed; a no-op if empty
     */
    public static void deleteResourceDataForPatients(int cancerStudyId, Set<String> patientStableIds)
            throws DaoException {
        deleteResourceDataForEntities(cancerStudyId, "patient_id", patientStableIds);
    }

    private static void deleteResourceDataForEntities(
            int cancerStudyId, String idColumn, Set<String> stableIds) throws DaoException {
        if (stableIds == null || stableIds.isEmpty()) {
            return;
        }
        Set<Long> idsToDelete = findResourceDataIdsByEntity(cancerStudyId, idColumn, stableIds);
        if (idsToDelete.isEmpty()) {
            return;
        }
        ClickHouseBulkDeleter.getBulkDeleter(RESOURCE_DATA_TABLE, "resource_data_id").addIds(idsToDelete);
    }

    private static Set<Long> findResourceDataIdsByEntity(
            int cancerStudyId, String idColumn, Set<String> stableIds) throws DaoException {
        // A sample's rows are the ones naming that sample; a patient's are the ones naming the
        // patient and no sample, so removing a patient does not take rows that belong to a sample
        // being kept.
        String scope = "sample_id".equals(idColumn) ? "" : " AND `sample_id` IS NULL";
        return queryResourceDataIds(
            "WHERE `cancer_study_id` = ? AND `" + idColumn + "` IN (%s)" + scope,
            cancerStudyId, stableIds);
    }

    private static Set<Long> findResourceDataIds(int cancerStudyId, Set<String> resourceIds) throws DaoException {
        return queryResourceDataIds(
            "WHERE `cancer_study_id` = ? AND `resource_id` IN (%s)", cancerStudyId, resourceIds);
    }

    /**
     * Collects the {@code resource_data_id}s a deletion should queue. The where clause carries one
     * {@code %s} for the IN placeholders, and is always scoped by study: the values are stable ids,
     * which repeat across studies.
     */
    private static Set<Long> queryResourceDataIds(
            String whereTemplate, int cancerStudyId, Set<String> values) throws DaoException {
        Set<Long> ids = new HashSet<>();
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoResourceData.class);
            String placeholders = values.stream().map(id -> "?").collect(Collectors.joining(","));
            pstmt = con.prepareStatement(
                "SELECT `resource_data_id` FROM `" + RESOURCE_DATA_TABLE + "` "
                + String.format(whereTemplate, placeholders)
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

