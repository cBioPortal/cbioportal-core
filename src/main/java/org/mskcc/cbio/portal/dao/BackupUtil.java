package org.mskcc.cbio.portal.dao;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class BackupUtil {

    @FunctionalInterface
    public interface ThrowingRunnable {
        void run() throws Exception;
    }

    public static void withBackup(List<String> tableNames, ThrowingRunnable fn) throws Exception {
        List<String> backedUp = new ArrayList<>();
        try {
            for (String table : tableNames) {
                backup(table);
                backedUp.add(table);
            }
            fn.run();
        } catch (Throwable t) {
            List<String> toRestore = new ArrayList<>(backedUp);
            Collections.reverse(toRestore);
            for (String table : toRestore) {
                try {
                    restore(table);
                } catch (Throwable restoreEx) {
                    t.addSuppressed(restoreEx);
                }
            }
            throw t;
        } finally {
            for (String table : backedUp) {
                deleteBackup(table);
            }
        }
    }

    public static void backup(String tableName) throws DaoException {
        String backupTable = tableName + "_backup";
        Connection con = null;
        try {
            con = JdbcUtil.getDbConnection(BackupUtil.class);
            con.prepareStatement("DROP TABLE IF EXISTS " + backupTable + ";").executeUpdate();
            con.prepareStatement("CREATE TABLE " + backupTable + " AS " + tableName + ";").executeUpdate();
            con.prepareStatement("INSERT INTO " + backupTable + " SELECT * FROM " + tableName + ";").executeUpdate();
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(BackupUtil.class, con, null, null);
        }
    }

    public static void restore(String tableName) throws DaoException {
        String backupTable = tableName + "_backup";
        Connection con = null;
        try {
            con = JdbcUtil.getDbConnection(BackupUtil.class);
            con.prepareStatement("EXCHANGE TABLES " + backupTable + " AND " + tableName + ";").executeUpdate();
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(BackupUtil.class, con, null, null);
        }
    }

    private static void deleteBackup(String tableName) {
        String backupTable = tableName + "_backup";
        Connection con = null;
        try {
            con = JdbcUtil.getDbConnection(BackupUtil.class);
            con.prepareStatement("DROP TABLE IF EXISTS " + backupTable + ";").executeUpdate();
        } catch (Exception ignored) {
        } finally {
            JdbcUtil.closeAll(BackupUtil.class, con, null, null);
        }
    }
}
