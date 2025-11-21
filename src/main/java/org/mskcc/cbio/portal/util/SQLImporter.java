package org.mskcc.cbio.portal.util;

import org.mskcc.cbio.portal.dao.JdbcUtil;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Utility to import SQL files into the database.
 * Handles basic MySQL to SQLite syntax conversion.
 */
public class SQLImporter {

    /**
     * Import a SQL file into the database
     * @param sqlFilePath path to the SQL file
     * @throws SQLException
     * @throws IOException
     */
    public static void importSQL(String sqlFilePath) throws SQLException, IOException {
        Connection con = null;
        Statement stmt = null;

        try {
            con = JdbcUtil.getDbConnection(SQLImporter.class);
            stmt = con.createStatement();

            // Detect database type
            String dbUrl = con.getMetaData().getURL();
            boolean isSQLite = (dbUrl != null && dbUrl.startsWith("jdbc:sqlite"));

            StringBuilder sqlBuilder = new StringBuilder();

            try (BufferedReader reader = new BufferedReader(new FileReader(sqlFilePath))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    // Skip comments and empty lines
                    line = line.trim();
                    if (line.isEmpty() || line.startsWith("--") || line.startsWith("#")) {
                        continue;
                    }

                    // Skip MySQL-specific commands
                    if (isSQLite) {
                        if (line.toUpperCase().startsWith("SET ") ||
                            line.toUpperCase().startsWith("USE ") ||
                            line.toUpperCase().startsWith("LOCK ") ||
                            line.toUpperCase().startsWith("UNLOCK ")) {
                            continue;
                        }

                        // Basic MySQL to SQLite conversion
                        line = line.replaceAll("(?i)ENGINE\\s*=\\s*\\w+", "");
                        line = line.replaceAll("(?i)DEFAULT\\s+CHARSET\\s*=\\s*\\w+", "");
                        line = line.replaceAll("(?i)AUTO_INCREMENT", "AUTOINCREMENT");
                        line = line.replaceAll("(?i)\\s+unsigned", "");
                    }

                    sqlBuilder.append(line).append(" ");

                    // Execute when we hit a semicolon (end of statement)
                    if (line.endsWith(";")) {
                        String sql = sqlBuilder.toString().trim();
                        if (!sql.isEmpty()) {
                            try {
                                stmt.execute(sql);
                                System.out.println("Executed: " +
                                    (sql.length() > 100 ? sql.substring(0, 100) + "..." : sql));
                            } catch (SQLException e) {
                                System.err.println("Error executing SQL: " + sql);
                                System.err.println("Error: " + e.getMessage());
                                // Continue with next statement
                            }
                        }
                        sqlBuilder.setLength(0); // Clear for next statement
                    }
                }
            }

            System.out.println("SQL import completed successfully.");

        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            JdbcUtil.closeConnection(SQLImporter.class, con);
        }
    }

    /**
     * Main method for command-line usage
     * @param args SQL file path
     */
    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: java SQLImporter <sql-file-path>");
            System.exit(1);
        }

        try {
            importSQL(args[0]);
        } catch (Exception e) {
            System.err.println("Error importing SQL file: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
