package org.mskcc.cbio.portal.dao;

import java.sql.*;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Data Access Object for the unified {@code resource} table, which replaced resource_sample,
 * resource_patient and resource_study.
 */
public final class DaoResourceData {

    public static final String RESOURCE_TABLE = "resource";

    /**
     * The columns both write paths below bind, in the order they bind them. Declared once so the
     * bulk and direct-load paths cannot drift apart, and so neither depends on the column order
     * of a table this repository does not define -- the portal's schema creates resource.
     */
    private static final String[] RESOURCE_COLUMNS = {
        "internal_id", "resource_id", "cancer_study_id", "resource_scope",
        "patient_id", "sample_id", "url", "display_name", "metadata"
    };

    private static final String RESOURCE_SEQUENCE = "seq_resource";


    private DaoResourceData() {
    }

    /**
     * Inserts a row into the unified {@code resource} table.
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
            // The column names are declared rather than left to the loader, which otherwise
            // falls back to DESCRIBE TABLE and pairs the values below with whatever order that
            // returns. resource is created by the portal's schema, in another repository,
            // so this would bind nine values positionally against a column order this code does
            // not own: a reordered or newly inserted column lands the url in display_name with
            // no error, since only the value count is checked.
            //
            // Pass nulls through: the loader encodes null as \N, which ClickHouse stores as a
            // real NULL. Patient-level rows have no sample, and queries select them with
            // sample_id IS NULL, which an empty string does not match.
            ClickHouseBulkLoader loader =
                ClickHouseBulkLoader.getClickHouseBulkLoader(RESOURCE_TABLE);
            loader.setFieldNames(RESOURCE_COLUMNS);
            loader.insertRecord(
                Long.toString(ClickHouseAutoIncrement.nextId(RESOURCE_SEQUENCE)),
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
            // internal_id comes from the shared sequence here too. Leaving it out of the
            // column list makes ClickHouse default it to 0 for every row this path writes, and
            // deletions select by internal_id -- so one row's deletion would take every
            // other row written this way with it.
            pstmt = con.prepareStatement(buildInsertStatement());
            pstmt.setLong(1, ClickHouseAutoIncrement.nextId(RESOURCE_SEQUENCE));
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
     * Deletes all existing {@code resource} rows for the given study that belong to any
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
        ClickHouseBulkDeleter.getBulkDeleter(RESOURCE_TABLE, "internal_id").addIds(idsToDelete);
    }

    /**
     * Deletes the rows a re-import is about to replace: those of the given resources that belong
     * to the entities the incoming file names.
     *
     * <p>Rows are deleted and reinserted rather than updated. MergeTree makes single-row updates
     * expensive, and the importer cannot tell which existing row corresponds to an input line.
     *
     * <p>The entity restriction keeps a single-file import from reaching beyond its own contents:
     * rows of other samples carry the same {@code resource_id} and must survive it.
     * {@code cbioportalImporter.py import-study-data} loads one file without removing the study
     * first, so nothing else narrows the deletion.
     *
     * @param cancerStudyId   internal cancer-study ID
     * @param resourceScope   "PATIENT", "SAMPLE" or "STUDY": the level the incoming file loads
     * @param resourceIds     resource IDs the file carries on rows that passed validation
     * @param entityStableIds stable patient or sample IDs on those rows; unused for STUDY scope
     */
    public static void deleteResourceDataForReimport(
            int cancerStudyId, String resourceScope, Set<String> resourceIds,
            Set<String> entityStableIds) throws DaoException {
        if (resourceIds == null || resourceIds.isEmpty()) {
            return;
        }
        Set<Long> idsToDelete;
        if ("STUDY".equals(resourceScope)) {
            // Study-level rows hang off nothing else, so the resource ids identify them fully.
            idsToDelete = queryResourceDataIds(
                "WHERE `cancer_study_id` = ? AND `resource_id` IN (%1$s)"
                    + " AND `patient_id` IS NULL AND `sample_id` IS NULL",
                cancerStudyId, resourceIds, null);
        } else if (entityStableIds == null || entityStableIds.isEmpty()) {
            // No row passed validation, so there is nothing this file is replacing. Deleting on
            // the resource ids alone would empty the resource and insert nothing in its place.
            return;
        } else if ("SAMPLE".equals(resourceScope)) {
            idsToDelete = queryResourceDataIds(
                "WHERE `cancer_study_id` = ? AND `resource_id` IN (%1$s) AND `sample_id` IN (%2$s)",
                cancerStudyId, resourceIds, entityStableIds);
        } else {
            // Patient-level rows are the ones with no sample; a sample's rows belong to the
            // sample and are replaced by a sample file.
            idsToDelete = queryResourceDataIds(
                "WHERE `cancer_study_id` = ? AND `resource_id` IN (%1$s) AND `patient_id` IN (%2$s)"
                    + " AND `sample_id` IS NULL",
                cancerStudyId, resourceIds, entityStableIds);
        }
        if (idsToDelete.isEmpty()) {
            return;
        }
        ClickHouseBulkDeleter.getBulkDeleter(RESOURCE_TABLE, "internal_id").addIds(idsToDelete);
    }

    /**
     * Deletes the {@code resource} rows belonging to the given samples of a study.
     *
     * <p>The legacy {@code resource_sample} table keyed rows by the sample's <em>internal</em> id,
     * so deleting a sample could reach its resources directly. {@code resource} stores the
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
     * Deletes the {@code resource} rows belonging to the given patients of a study. Only
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
        ClickHouseBulkDeleter.getBulkDeleter(RESOURCE_TABLE, "internal_id").addIds(idsToDelete);
    }

    private static Set<Long> findResourceDataIdsByEntity(
            int cancerStudyId, String idColumn, Set<String> stableIds) throws DaoException {
        // A sample's rows are the ones naming that sample; a patient's are the ones naming the
        // patient and no sample, so removing a patient does not take rows that belong to a sample
        // being kept.
        String scope = "sample_id".equals(idColumn) ? "" : " AND `sample_id` IS NULL";
        return queryResourceDataIds(
            "WHERE `cancer_study_id` = ? AND `" + idColumn + "` IN (%2$s)" + scope,
            cancerStudyId, Collections.emptySet(), stableIds);
    }

    /** {@code INSERT INTO resource (col, ...) VALUES (?, ...)} over RESOURCE_COLUMNS. */
    private static String buildInsertStatement() {
        StringBuilder columns = new StringBuilder();
        StringBuilder placeholders = new StringBuilder();
        for (String column : RESOURCE_COLUMNS) {
            if (columns.length() > 0) {
                columns.append(',');
                placeholders.append(',');
            }
            columns.append('`').append(column).append('`');
            placeholders.append('?');
        }
        return "INSERT INTO `" + RESOURCE_TABLE + "` (" + columns + ") VALUES (" + placeholders + ")";
    }

    private static Set<Long> findResourceDataIds(int cancerStudyId, Set<String> resourceIds) throws DaoException {
        return queryResourceDataIds(
            "WHERE `cancer_study_id` = ? AND `resource_id` IN (%1$s)", cancerStudyId, resourceIds, null);
    }

    /**
     * Collects the {@code internal_id}s a deletion should queue. The where clause is always scoped
     * by study, since stable ids repeat across studies. {@code %1$s} expands to IN placeholders
     * for the resource ids, which are bounded by the study's resource definitions. {@code %2$s}
     * expands to a subquery over a staging table of entity ids: a file can name every sample in a
     * study, and the driver inlines bound values, so an IN list of them exceeds max_query_size.
     *
     * @param entityStableIds staged when non-null; pass null when the template has no {@code %2$s}
     */
    private static Set<Long> queryResourceDataIds(
            String whereTemplate, int cancerStudyId, Set<String> resourceIds,
            Set<String> entityStableIds) throws DaoException {
        return ClickHouseBulkUploader.uploadStrings(entityStableIds, stagingTable -> {
            Set<Long> ids = new HashSet<>();
            Connection con = null;
            PreparedStatement pstmt = null;
            ResultSet rs = null;
            try {
                con = JdbcUtil.getDbConnection(DaoResourceData.class);
                String resourcePlaceholders =
                    resourceIds.stream().map(id -> "?").collect(Collectors.joining(","));
                String entitySubquery = stagingTable == null ? null : "SELECT id FROM " + stagingTable;
                pstmt = con.prepareStatement(
                    "SELECT `internal_id` FROM `" + RESOURCE_TABLE + "` "
                    + String.format(whereTemplate, resourcePlaceholders, entitySubquery)
                );
                int paramIndex = 1;
                pstmt.setInt(paramIndex++, cancerStudyId);
                for (String resourceId : resourceIds) {
                    pstmt.setString(paramIndex++, resourceId);
                }
                rs = pstmt.executeQuery();
                while (rs.next()) {
                    ids.add(rs.getLong("internal_id"));
                }
            } finally {
                JdbcUtil.closeAll(DaoResourceData.class, con, pstmt, rs);
            }
            return ids;
        });
    }
}

