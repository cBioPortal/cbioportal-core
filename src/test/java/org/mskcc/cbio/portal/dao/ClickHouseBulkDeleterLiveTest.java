package org.mskcc.cbio.portal.dao;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import javax.sql.DataSource;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeNotNull;
import static org.junit.Assume.assumeTrue;

/**
 * Opt-in against disposable databases: CORE_DELETE_TEST_URL with a single-node default
 * cluster tests flush/recovery. Also set CORE_DELETE_PEER_URL and configure both nodes
 * in the first server's default cluster to test divergent replica visibility instead.
 */
public class ClickHouseBulkDeleterLiveTest {
    @Test
    public void deletesAndRecoversCompletedLeftoverOnClickHouse() throws Exception {
        String url = System.getenv("CORE_DELETE_TEST_URL");
        assumeNotNull(url);
        assumeTrue(System.getenv("CORE_DELETE_PEER_URL") == null);
        java.lang.reflect.Field field = JdbcUtil.class.getDeclaredField("dataSource");
        field.setAccessible(true);
        DataSource previous = (DataSource) field.get(null);
        try (BasicDataSource dataSource = new BasicDataSource()) {
            dataSource.setUrl(url);
            dataSource.setDriverClassName("com.clickhouse.jdbc.ClickHouseDriver");
            dataSource.setUsername("default");
            dataSource.setPassword("");
            dataSource.addConnectionProperty("custom_settings", JdbcDataSource.IDLE_KEEPALIVE_SETTINGS);
            JdbcUtil.setDataSource(dataSource);
            try (Connection connection = dataSource.getConnection(); Statement stmt = connection.createStatement()) {
                stmt.execute("CREATE TABLE review_delete_target (id Int64) ENGINE=MergeTree ORDER BY id");
                stmt.execute("INSERT INTO review_delete_target VALUES (1),(2),(3)");
                ClickHouseBulkDeleter.getBulkDeleter("review_delete_target", "id").addId(1);
                ClickHouseBulkDeleter.flushAll();
                try (ResultSet result = stmt.executeQuery("SELECT count(), min(id), max(id) FROM review_delete_target")) {
                    assertTrue(result.next());
                    assertEquals(2, result.getLong(1));
                    assertEquals(2, result.getLong(2));
                    assertEquals(3, result.getLong(3));
                }
                stmt.execute("CREATE TABLE staging_delete_for_table_review_delete_target_column_id (id Int64) ENGINE=MergeTree ORDER BY id");
                stmt.execute("INSERT INTO staging_delete_for_table_review_delete_target_column_id VALUES (1)");
                ClickHouseBulkDeleter.getBulkDeleter("review_delete_target", "id").addId(2);
                ClickHouseBulkDeleter.flushAll();
                try (ResultSet result = stmt.executeQuery("SELECT count(), min(id) FROM review_delete_target")) {
                    assertTrue(result.next());
                    assertEquals(1, result.getLong(1));
                    assertEquals(3, result.getLong(2));
                }
                try (ResultSet result = stmt.executeQuery("EXISTS TABLE staging_delete_for_table_review_delete_target_column_id")) {
                    assertTrue(result.next());
                    assertEquals(0, result.getLong(1));
                }
                stmt.execute("DROP TABLE review_delete_target");
            }
        } finally {
            JdbcUtil.setDataSource(previous);
        }
    }

    @Test
    public void rejectsLaggingReplicaEvenWhenItsStagingCopyIsEmpty() throws Exception {
        String url = System.getenv("CORE_DELETE_TEST_URL");
        String peerUrl = System.getenv("CORE_DELETE_PEER_URL");
        assumeNotNull(url, peerUrl);
        try (BasicDataSource source = new BasicDataSource();
                Connection peer = java.sql.DriverManager.getConnection(peerUrl, "default", "");
                Statement peerStatement = peer.createStatement()) {
            source.setUrl(url);
            source.setUsername("default");
            source.setPassword("");
            java.lang.reflect.Field field = JdbcUtil.class.getDeclaredField("dataSource");
            field.setAccessible(true);
            DataSource previous = (DataSource) field.get(null);
            JdbcUtil.setDataSource(source);
            try (Connection connection = source.getConnection(); Statement statement = connection.createStatement()) {
                for (Statement node : new Statement[]{statement, peerStatement}) {
                    node.execute("CREATE TABLE review_replica_target (id Int64) ENGINE=MergeTree ORDER BY id");
                    node.execute("CREATE TABLE staging_delete_for_table_review_replica_target_column_id (id Int64) ENGINE=MergeTree ORDER BY id");
                }
                statement.execute("INSERT INTO staging_delete_for_table_review_replica_target_column_id VALUES (1)");
                peerStatement.execute("INSERT INTO review_replica_target VALUES (1)");
                java.lang.reflect.Method check = ClickHouseBulkDeleter.class.getDeclaredMethod("deletionIsComplete");
                check.setAccessible(true);
                ClickHouseBulkDeleter deleter = ClickHouseBulkDeleter.getBulkDeleter("review_replica_target", "id");
                assertEquals(false, check.invoke(deleter));
                peerStatement.execute("DELETE FROM review_replica_target WHERE id = 1");
                assertEquals(true, check.invoke(deleter));
                peerStatement.execute("DROP TABLE review_replica_target");
                assertThrows(java.lang.reflect.InvocationTargetException.class, () -> check.invoke(deleter));
                statement.execute("DROP TABLE review_replica_target");
                for (Statement node : new Statement[]{statement, peerStatement}) {
                    node.execute("DROP TABLE staging_delete_for_table_review_replica_target_column_id");
                }
            } finally {
                JdbcUtil.setDataSource(previous);
            }
        }
    }
}
