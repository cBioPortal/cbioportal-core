package org.mskcc.cbio.portal.dao;

import java.sql.Connection;
import java.sql.SQLException;

public class BackupUtil {

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
            con.prepareStatement("DROP TABLE IF EXISTS " + backupTable + ";").executeUpdate();
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(BackupUtil.class, con, null, null);
        }
    }
}
