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
 * SQLite bulk loader implementation using JDBC batch inserts.
 * Unlike MySQL's LOAD DATA INFILE, SQLite doesn't have a direct file-based bulk loading mechanism,
 * so this implementation uses batched prepared statements for efficient bulk inserts.
 *
 * @author cBioPortal SQLite Migration
 */
public class SQLiteBulkLoader {
    private static boolean bulkLoad = false;
    private static boolean relaxedMode = false;
    private String[] fieldNames = null;

    private static final Map<String, SQLiteBulkLoader> sqliteBulkLoaders = new LinkedHashMap<String, SQLiteBulkLoader>();
    private static final int BATCH_SIZE = 1000; // Commit every 1000 records

    /**
     * Get a SQLiteBulkLoader
     * @param tableName table name
     * @return SQLiteBulkLoader instance
     */
    public static SQLiteBulkLoader getSQLiteBulkLoader(String tableName) {
        SQLiteBulkLoader sqliteBulkLoader = sqliteBulkLoaders.get(tableName);
        if (sqliteBulkLoader == null) {
            sqliteBulkLoader = new SQLiteBulkLoader(tableName);
            sqliteBulkLoaders.put(tableName, sqliteBulkLoader);
        }
        return sqliteBulkLoader;
    }

    /**
     * Flushes all pending data from the bulk writer.
     * @return the number of rows added
     * @throws DaoException
     */
    public static int flushAll() throws DaoException {
        int checks = 0;
        Statement stmt = null;
        boolean executedSetFKChecks = false;
        Connection con = null;
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

            int n = 0;
            for (SQLiteBulkLoader sqliteBulkLoader : sqliteBulkLoaders.values()) {
                n += sqliteBulkLoader.loadDataIntoDB();
            }

            return n;
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            sqliteBulkLoaders.clear();
            if (executedSetFKChecks && stmt != null) {
                try {
                    stmt.execute("PRAGMA foreign_keys = " + (checks == 1 ? "ON" : "OFF"));
                } catch (SQLException e) {
                    throw new DaoException(e);
                }
            }
            // Manually close resources since we have a Statement, not PreparedStatement
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

    private SQLiteBulkLoader(String tableName) {
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
                        // Unescape values that were escaped for MySQL file format
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
}
