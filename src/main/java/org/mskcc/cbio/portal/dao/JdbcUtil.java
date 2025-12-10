/*
 * Copyright (c) 2015 - 2017 Memorial Sloan-Kettering Cancer Center.
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

import java.sql.*;
import java.util.*;
import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Connection Utility for JDBC.
 *
 * @author Ethan Cerami
 * @author Ersin Ciftci
 */
public class JdbcUtil {
    private static DataSource dataSource;
    private static Map<String,Integer> activeConnectionCount = new HashMap<String,Integer>(); // keep track of the number of active connection per class/requester
    private static final Logger LOG = LoggerFactory.getLogger(JdbcUtil.class);
    private static DataSourceTransactionManager transactionManager;
    private static TransactionTemplate transactionTemplate;

    /**
     * Gets the data source
     * @return the data source
     */
    public static DataSource getDataSource() {
        if (dataSource == null) {
            DataSource unwrapped = new JdbcDataSource();
            dataSource = new TransactionAwareDataSourceProxy(unwrapped);
            setupTransactionManagement();
            // Apply SQLite optimizations once at startup using unwrapped connection
            applySQLiteGlobalOptimizations(unwrapped);
        }
        return dataSource;
    }

    private static void setupTransactionManagement() {
        transactionManager = new DataSourceTransactionManager(dataSource);
        transactionTemplate = new TransactionTemplate(transactionManager);
    }

    /**
     * Apply SQLite global optimizations once at startup before any transactions.
     * These must be set outside of transactions.
     */
    private static void applySQLiteGlobalOptimizations(DataSource unwrappedDataSource) {
        try (Connection con = unwrappedDataSource.getConnection()) {
            String dbUrl = con.getMetaData().getURL();
            if (dbUrl != null && dbUrl.startsWith("jdbc:sqlite")) {
                try (Statement stmt = con.createStatement()) {
                    // Disable synchronous writes - HUGE performance boost for bulk inserts
                    stmt.execute("PRAGMA synchronous = OFF");
                    // Use Write-Ahead Logging for better concurrency
                    stmt.execute("PRAGMA journal_mode = WAL");
                    // Increase cache size to 256MB (default is ~2MB)
                    stmt.execute("PRAGMA cache_size = -262144");
                    // Store temp tables in memory
                    stmt.execute("PRAGMA temp_store = MEMORY");
                    LOG.info("Applied SQLite global performance optimizations");
                }
            }
        } catch (Exception e) {
            LOG.warn("Failed to apply SQLite global optimizations", e);
        }
    }

    /**
     * Sets the data source
     * @param value the data source
     */
    public static void setDataSource(DataSource value) {
        dataSource = value;
        setupTransactionManagement();
    }

    public static TransactionTemplate getTransactionTemplate() {
        if (transactionTemplate == null) {
            getDataSource();
        }
        return transactionTemplate;
    }

    /**
     * Gets Connection to the Database.
     *
     * @param clazz class
     * @return Live Connection to Database.
     * @throws java.sql.SQLException Error Connecting to Database.
     */
    public static Connection getDbConnection(Class clazz) throws SQLException {
        return getDbConnection(clazz.getName());
    }

    /**
     * Gets Connection to the Database.
     *
     * @param requester name
     * @return Live Connection to Database.
     * @throws java.sql.SQLException Error Connecting to Database.
     */
    private static Connection getDbConnection(String requester) throws SQLException {
        // this method should be syncronized
        // but may slow the speed?
        Connection con;
        try {
            con = getDataSource().getConnection();
            // For SQLite, explicitly set auto-commit to false to work with Spring transactions
            if (con.getMetaData().getURL().startsWith("jdbc:sqlite")) {
                if (con.getAutoCommit()) {
                    con.setAutoCommit(false);
                }
            }
        } catch (Exception e) {
            logMessage(e.getMessage());
            throw new SQLException(e);
        }
        if (requester != null) {
            Integer count = activeConnectionCount.get(requester);
            activeConnectionCount.put(requester, count == null ? 1 : (count + 1));
        }
        return con;
    }

    /**
     * Frees Database Connection.
     *
     * @param con Connection Object.
     */
    public static void closeConnection(Class clazz, Connection con) {
        closeConnection(clazz.getName(), con);
    }

    private static void closeConnection(String requester, Connection con) {
        try {
            if (con != null && !con.isClosed()) {
                // For SQLite with auto-commit disabled, commit before closing to ensure
                // changes are visible to subsequent connections/commands
                if (!con.getAutoCommit() && con.getMetaData().getURL().startsWith("jdbc:sqlite")) {
                    try {
                        con.commit();
                    } catch (SQLException e) {
                        LOG.warn("Failed to commit SQLite connection before closing", e);
                    }
                }
                con.close();
                if (requester!=null) {
                    int count = activeConnectionCount.get(requester) - 1;
                    if (count < 0) {
                        // since adding connection is not synchronized, the count may not be the real one
                        count = 0;
                    }
                    activeConnectionCount.put(requester, count);
                }
            }
        } catch (Exception e) {
            logMessage("Problem Closed a MySQL connection from " + requester + ": " + activeConnectionCount.toString());
            e.printStackTrace();
        }
    }

    /**
     * Frees PreparedStatement and ResultSet.
     *
     * @param rs  ResultSet Object.
     */
    public static void closeAll(ResultSet rs) {
        JdbcUtil.closeAll((String)null, null, null, rs);
    }

    /**
     * Frees Database Connection.
     *
     * @param con Connection Object.
     * @param ps  Prepared Statement Object.
     * @param rs  ResultSet Object.
     */
    public static void closeAll(Class clazz, Connection con, PreparedStatement ps, ResultSet rs) {
        closeAll(clazz.getName(), con, ps, rs);
    }

    /**
     * Frees Database Connection.
     *
     * @param con Connection Object.
     * @param rs  ResultSet Object.
     */
    private static void closeAll(String requester, Connection con, PreparedStatement ps, ResultSet rs) {
        if (rs != null) {
            try {
                if (!rs.isClosed()) {
                    rs.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (ps != null) {
            try {
                if (!ps.isClosed()) {
                    ps.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        closeConnection(requester, con);
    }

    /**
     * Why does closeAll need a PreparedStatement?
     * This is a copy of closeAll without the PreparedStatement
     * @param clazz
     * @param con
     * @param rs
     */
    public static void closeAll(Class clazz, Connection con, ResultSet rs) {
        String requester = clazz.getName();
        closeConnection(requester, con);
        if (rs != null) {
            try {
                if (!rs.isClosed()) {
                    rs.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private static void logMessage(String message) {
        if (LOG.isInfoEnabled()) {
            LOG.info(message);
        }
        System.err.println(message);
    }

    // is it good to put the two methods below here?
    static Integer readIntegerFromResultSet(ResultSet rs, String column) throws SQLException {
        int i = rs.getInt(column);
        return rs.wasNull() ? null : i;
    }

    static Long readLongFromResultSet(ResultSet rs, String column) throws SQLException {
        long l = rs.getInt(column);
        return rs.wasNull() ? null : l;
    }

    static Double readDoubleFromResultSet(ResultSet rs, String column) throws SQLException {
        double d = rs.getDouble(column);
        return rs.wasNull() ? null : d;
    }

    /**
     * Tells the database to ignore foreign key constraints, effective only for current session.
     * Useful when you want to truncate a table that has foreign key constraints. Note that this
     * may create orphan records in child tables.
     * @param con Database connection
     * @throws SQLException
     */
    public static void disableForeignKeyCheck(Connection con) throws SQLException {
        Statement stmt = con.createStatement();
        String dbUrl = con.getMetaData().getURL();
        if (dbUrl != null && dbUrl.startsWith("jdbc:sqlite")) {
            stmt.execute("PRAGMA foreign_keys = OFF");
        } else {
            // MySQL syntax
            stmt.execute("SET FOREIGN_KEY_CHECKS=0");
        }
        stmt.close();
    }

    /**
     * Reverses the effect of disableForeignKeyCheck method.
     * @param con Database Connection
     * @throws SQLException
     */
    public static void enableForeignKeyCheck(Connection con) throws SQLException {
        Statement stmt = con.createStatement();
        String dbUrl = con.getMetaData().getURL();
        if (dbUrl != null && dbUrl.startsWith("jdbc:sqlite")) {
            stmt.execute("PRAGMA foreign_keys = ON");
        } else {
            // MySQL syntax
            stmt.execute("SET FOREIGN_KEY_CHECKS=1");
        }
        stmt.close();
    }

    /**
     * Get the auto-generated ID after an INSERT in a database-agnostic way.
     * MySQL supports getGeneratedKeys(), but SQLite JDBC driver has limited support,
     * so we use last_insert_rowid() for SQLite.
     *
     * @param pstmt PreparedStatement that was just executed with an INSERT
     * @param con Connection used for the INSERT
     * @return The auto-generated ID, or -1 if not available
     * @throws SQLException
     */
    public static int getInsertedId(PreparedStatement pstmt, Connection con) throws SQLException {
        // SQLite-only: Use last_insert_rowid() to get auto-increment ID
        PreparedStatement pstmt2 = null;
        ResultSet rs2 = null;
        try {
            pstmt2 = con.prepareStatement("SELECT last_insert_rowid()");
            rs2 = pstmt2.executeQuery();
            if (rs2.next()) {
                return rs2.getInt(1);
            }
        } finally {
            if (rs2 != null) rs2.close();
            if (pstmt2 != null) pstmt2.close();
        }
        return -1;
    }
}
