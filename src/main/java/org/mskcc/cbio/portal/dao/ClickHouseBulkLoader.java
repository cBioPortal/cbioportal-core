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
 * but WITHOUT ANY WARRANTY, without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.mskcc.cbio.portal.dao;

import org.mskcc.cbio.portal.dao.DaoException;
import org.mskcc.cbio.portal.util.ProgressMonitor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Minimal bulk loader that buffers rows in memory and inserts them via JDBC batches.
 * This replaces the previous legacy LOAD DATA LOCAL INFILE implementation.
 * <p>
 * The loader keeps the public API intact so that the various DAO classes do not need to
 * change their interaction pattern.
 */
public class ClickHouseBulkLoader {

    private static final Map<String, ClickHouseBulkLoader> BULK_LOADERS = new LinkedHashMap<>();

    private static boolean bulkLoad = false;
    private static boolean relaxedMode = false;

    private final String tableName;
    private final List<String[]> pendingRecords = new ArrayList<>();
    private String[] fieldNames = null;

    private ClickHouseBulkLoader(String tableName) {
        this.tableName = tableName;
    }

    public static ClickHouseBulkLoader getClickHouseBulkLoader(String tableName) {
        return BULK_LOADERS.computeIfAbsent(tableName, ClickHouseBulkLoader::new);
    }

    public static int flushAll() throws DaoException {
        int totalInserted = 0;
        for (ClickHouseBulkLoader loader : BULK_LOADERS.values()) {
            totalInserted += loader.flushPendingRecords();
        }
        BULK_LOADERS.clear();
        return totalInserted;
    }

    private int flushPendingRecords() throws DaoException {
        if (pendingRecords.isEmpty()) {
            return 0;
        }

        final int expectedRows = pendingRecords.size();
        Connection con = null;
        PreparedStatement stmt = null;
        try {
            con = JdbcUtil.getDbConnection(ClickHouseBulkLoader.class);
            stmt = con.prepareStatement(buildInsertStatement());
            for (String[] record : pendingRecords) {
                bindRecord(stmt, record);
                stmt.addBatch();
            }

            int[] updateCounts = stmt.executeBatch();
            int rowsInserted = calculateInsertedRows(updateCounts, expectedRows);
            ProgressMonitor.setCurrentMessage(" --> records inserted into `" + tableName + "` table: " + rowsInserted);

            if (!relaxedMode && rowsInserted != expectedRows) {
                throw new DaoException("DB Error: only " + rowsInserted + " of the " + expectedRows
                    + " records were inserted in `" + tableName + "`.");
            }

            pendingRecords.clear();
            return rowsInserted;
        } catch (SQLException exception) {
            throw new DaoException(exception);
        } finally {
            JdbcUtil.closeAll(ClickHouseBulkLoader.class, con, stmt, null);
        }
    }

    private String buildInsertStatement() {
        final int columnCount = fieldNames != null ? fieldNames.length : pendingRecords.get(0).length;
        final String columnsClause = fieldNames == null ? "" : " (" + String.join(",", fieldNames) + ")";
        final String placeholders = "(" + String.join(",", Collections.nCopies(columnCount, "?")) + ")";
        return "INSERT INTO " + tableName + columnsClause + " VALUES " + placeholders;
    }

    private void bindRecord(PreparedStatement stmt, String[] record) throws SQLException {
        final int columnCount = fieldNames != null ? fieldNames.length : record.length;
        for (int i = 0; i < columnCount; i++) {
            String value = record[i];
            if (value == null || "\\N".equals(value)) {
                stmt.setNull(i + 1, Types.VARCHAR);
            } else {
                stmt.setString(i + 1, value);
            }
        }
    }

    private int calculateInsertedRows(int[] updateCounts, int expectedRows) {
        int rowsInserted = 0;
        for (int count : updateCounts) {
            if (count == Statement.SUCCESS_NO_INFO) {
                return expectedRows;
            }
            if (count > 0) {
                rowsInserted += count;
            }
        }
        return rowsInserted;
    }

    public void insertRecord(String... fieldValues) {
        if (fieldValues.length == 0) {
            return;
        }
        pendingRecords.add(fieldValues);
    }

    public static boolean isBulkLoad() {
        return bulkLoad;
    }

    public static void bulkLoadOn() {
        bulkLoad = true;
    }

    public static void bulkLoadOff() {
        bulkLoad = false;
    }

    public static void relaxedModeOn() {
        relaxedMode = true;
    }

    public static void relaxedModeOff() {
        relaxedMode = false;
    }

    public void setFieldNames(String[] fieldNames) {
        this.fieldNames = fieldNames;
    }
}
