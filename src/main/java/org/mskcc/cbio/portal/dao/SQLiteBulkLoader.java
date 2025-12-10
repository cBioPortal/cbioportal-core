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

import org.mskcc.cbio.portal.util.*;

import java.sql.*;
import java.util.*;

/**
 * Bulk loader implementation using JDBC batch inserts for SQLite.
 * Uses batched prepared statements for efficient bulk inserts.
 *
 * Note: This class retains the SQLiteBulkLoader name for backward compatibility,
 * but now only supports SQLite.
 */
public class SQLiteBulkLoader {
    private static boolean bulkLoad = false;
    private static boolean relaxedMode = false;
    private String[] fieldNames = null;

    private static final Map<String, SQLiteBulkLoader> bulkLoaders = new LinkedHashMap<String, SQLiteBulkLoader>();
    private static final int BATCH_SIZE = 10000;

    /**
     * Get a SQLiteBulkLoader for the given table
     * @param tableName table name
     * @return SQLiteBulkLoader instance
     */
    public static SQLiteBulkLoader getSQLiteBulkLoader(String tableName) {
        SQLiteBulkLoader bulkLoader = bulkLoaders.get(tableName);
        if (bulkLoader == null) {
            bulkLoader = new SQLiteBulkLoader(tableName);
            bulkLoaders.put(tableName, bulkLoader);
        }
        return bulkLoader;
    }

    /**
     * Flushes all pending data from the bulk writer.
     * Implements deferred index creation for performance:
     * 1. Saves all index definitions
     * 2. Drops all indexes
     * 3. Performs bulk import (fast without index maintenance)
     * 4. Recreates all indexes
     * @return the number of rows added
     * @throws DaoException
     */
    public static int flushAll() throws DaoException {
        int checks = 0;
        Statement stmt = null;
        boolean executedSetFKChecks = false;
        Connection con = null;
        List<String> savedIndexDefinitions = new ArrayList<>();
        long startTime = System.currentTimeMillis();

        try {
            con = JdbcUtil.getDbConnection(SQLiteBulkLoader.class);

            // Check and disable foreign key constraints
            stmt = con.createStatement();
            ResultSet result = stmt.executeQuery("PRAGMA foreign_keys");
            if (result.next()) {
                checks = result.getInt(1);
            }
            stmt.execute("PRAGMA foreign_keys = OFF");
            executedSetFKChecks = true;

            // PHASE 1: Save all index definitions from sqlite_master
            ProgressMonitor.setCurrentMessage(" --> Saving index definitions...");
            ResultSet rs = stmt.executeQuery(
                "SELECT sql FROM sqlite_master WHERE type='index' AND sql IS NOT NULL"
            );
            while (rs.next()) {
                String indexDef = rs.getString("sql");
                if (indexDef != null && !indexDef.isEmpty()) {
                    savedIndexDefinitions.add(indexDef);
                }
            }
            rs.close();
            ProgressMonitor.setCurrentMessage(" --> Saved " + savedIndexDefinitions.size() + " index definitions");

            // PHASE 2: Drop all indexes
            ProgressMonitor.setCurrentMessage(" --> Dropping indexes for faster import...");
            int droppedCount = 0;
            for (String indexDef : savedIndexDefinitions) {
                String indexName = extractIndexName(indexDef);
                if (indexName != null) {
                    try {
                        stmt.execute("DROP INDEX IF EXISTS " + indexName);
                        droppedCount++;
                    } catch (SQLException e) {
                        // Log warning but continue - some indexes might not exist
                        System.err.println("Warning: Could not drop index " + indexName + ": " + e.getMessage());
                    }
                }
            }
            ProgressMonitor.setCurrentMessage(" --> Dropped " + droppedCount + " indexes");

            // PHASE 3: Perform bulk import (fast without index maintenance)
            long importStartTime = System.currentTimeMillis();
            ProgressMonitor.setCurrentMessage(" --> Importing data without indexes...");
            int n = 0;
            for (SQLiteBulkLoader bulkLoader : bulkLoaders.values()) {
                n += bulkLoader.loadDataIntoDB();
            }
            long importTime = System.currentTimeMillis() - importStartTime;
            ProgressMonitor.setCurrentMessage(" --> Data import completed in " + (importTime / 1000) + " seconds");

            // PHASE 4: Recreate all indexes
            ProgressMonitor.setCurrentMessage(" --> Recreating indexes...");
            long indexStartTime = System.currentTimeMillis();
            int recreatedCount = 0;
            for (String indexDef : savedIndexDefinitions) {
                try {
                    stmt.execute(indexDef);
                    recreatedCount++;
                } catch (SQLException e) {
                    // Log warning but continue
                    System.err.println("Warning: Could not recreate index: " + indexDef);
                    System.err.println("Error: " + e.getMessage());
                }
            }
            long indexTime = System.currentTimeMillis() - indexStartTime;
            ProgressMonitor.setCurrentMessage(" --> Recreated " + recreatedCount + " indexes in " + (indexTime / 1000) + " seconds");

            long totalTime = System.currentTimeMillis() - startTime;
            ProgressMonitor.setCurrentMessage(" --> Total flush time: " + (totalTime / 1000) + " seconds (import: " +
                (importTime / 1000) + "s, indexes: " + (indexTime / 1000) + "s)");

            return n;
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            bulkLoaders.clear();
            if (executedSetFKChecks && stmt != null) {
                try {
                    stmt.execute("PRAGMA foreign_keys = " + (checks == 1 ? "ON" : "OFF"));
                } catch (SQLException e) {
                    throw new DaoException(e);
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    // ignore
                }
            }
            if (con != null) {
                try {
                    con.close();
                } catch (SQLException e) {
                    // ignore
                }
            }
        }
    }

    private String tableName;
    private List<String[]> recordBuffer = new ArrayList<>();
    private int rows;
    private static final long numDebuggingRowsToPrint = 0;

    protected SQLiteBulkLoader(String tableName) {
        this.tableName = tableName;
    }

    /**
     * Buffer a record's fields for later batch insertion.
     * @param fieldValues field values to insert
     */
    public void insertRecord(String... fieldValues) {
        if (fieldValues.length == 0) {
            return;
        }

        recordBuffer.add(fieldValues);

        if (rows++ < numDebuggingRowsToPrint) {
            StringBuilder sb = new StringBuilder(fieldValues[0] != null ? fieldValues[0] : "NULL");
            for (int i = 1; i < fieldValues.length; i++) {
                sb.append("\t").append(fieldValues[i] != null ? fieldValues[i] : "NULL");
            }
            System.err.println("SQLiteBulkLoader: Buffered " + sb.toString() + " for table '" + tableName + "'.");
        }
    }

    /**
     * Load the buffered records into the database using batch inserts.
     * @return number of records inserted
     * @throws DaoException
     */
    private int loadDataIntoDB() throws DaoException {
        if (recordBuffer.isEmpty()) {
            return 0;
        }

        Connection con = null;
        PreparedStatement pstmt = null;

        try {
            con = JdbcUtil.getDbConnection(SQLiteBulkLoader.class);
            con.setAutoCommit(false);

            // Build the INSERT statement
            int numFields = recordBuffer.get(0).length;
            StringBuilder sql = new StringBuilder("INSERT INTO ").append(tableName);

            // Add field names if specified
            if (fieldNames != null) {
                sql.append(" (");
                for (int i = 0; i < fieldNames.length; i++) {
                    if (i > 0) sql.append(", ");
                    sql.append(fieldNames[i]);
                }
                sql.append(")");
            }

            sql.append(" VALUES (");
            for (int i = 0; i < numFields; i++) {
                if (i > 0) sql.append(", ");
                sql.append("?");
            }
            sql.append(")");

            pstmt = con.prepareStatement(sql.toString());

            int batchCount = 0;
            int totalInserted = 0;

            for (String[] record : recordBuffer) {
                for (int i = 0; i < record.length; i++) {
                    if (record[i] == null || record[i].equals("\\N")) {
                        pstmt.setNull(i + 1, Types.VARCHAR);
                    } else {
                        // Unescape values that were escaped for file format
                        String value = record[i].replace("\\n", "\n").replace("\\t", "\t");
                        pstmt.setString(i + 1, value);
                    }
                }
                pstmt.addBatch();
                batchCount++;

                // Execute batch every BATCH_SIZE records
                if (batchCount >= BATCH_SIZE) {
                    int[] results = pstmt.executeBatch();
                    con.commit();
                    totalInserted += results.length;
                    batchCount = 0;
                }
            }

            // Execute remaining records
            if (batchCount > 0) {
                int[] results = pstmt.executeBatch();
                con.commit();
                totalInserted += results.length;
            }

            ProgressMonitor.setCurrentMessage(" --> records inserted into `" + tableName + "` table: " + totalInserted);

            if (totalInserted != recordBuffer.size() && !relaxedMode) {
                throw new DaoException("DB Error: only " + totalInserted + " of the " + recordBuffer.size()
                    + " records were inserted in `" + tableName + "`.");
            }

            recordBuffer.clear();
            return totalInserted;

        } catch (SQLException e) {
            try {
                if (con != null) con.rollback();
            } catch (SQLException ex) {
                // Ignore rollback errors
            }
            throw new DaoException(e);
        } finally {
            try {
                if (con != null) con.setAutoCommit(true);
            } catch (SQLException e) {
                // Ignore
            }
            JdbcUtil.closeAll(SQLiteBulkLoader.class, con, pstmt, null);
        }
    }

    public String getTableName() {
        return tableName;
    }

    public static boolean isBulkLoad() {
        return bulkLoad;
    }

    public static void bulkLoadOn() {
        SQLiteBulkLoader.bulkLoad = true;
    }

    public static void bulkLoadOff() {
        SQLiteBulkLoader.bulkLoad = false;
    }

    public static void relaxedModeOn() {
        SQLiteBulkLoader.relaxedMode = true;
    }

    public static void relaxedModeOff() {
        SQLiteBulkLoader.relaxedMode = false;
    }

    public void setFieldNames(String[] fieldNames) {
        this.fieldNames = fieldNames;
    }

    /**
     * Extract index name from a CREATE INDEX SQL statement.
     * Example: "CREATE INDEX `idx_name` ON table ..." -> "idx_name"
     * @param createIndexSql the CREATE INDEX SQL statement
     * @return the index name without backticks
     */
    private static String extractIndexName(String createIndexSql) {
        if (createIndexSql == null || createIndexSql.isEmpty()) {
            return null;
        }

        // Pattern: CREATE INDEX `index_name` or CREATE INDEX index_name
        String[] parts = createIndexSql.split("\\s+");
        for (int i = 0; i < parts.length - 1; i++) {
            if (parts[i].equalsIgnoreCase("INDEX")) {
                String indexName = parts[i + 1];
                // Remove backticks if present
                indexName = indexName.replace("`", "");
                // Remove anything after the index name (e.g., "ON")
                int onIndex = indexName.indexOf("ON");
                if (onIndex > 0) {
                    indexName = indexName.substring(0, onIndex).trim();
                }
                return indexName;
            }
        }

        return null;
    }
}
