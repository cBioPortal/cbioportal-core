package org.mskcc.cbio.portal.dao;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class ClickHouseBulkDeleterCompletionTest {
    private MockedStatic<JdbcUtil> jdbc;
    private Connection connection;
    private final List<String> updates = new ArrayList<>();
    private String[] reportedHosts = {"a", "b"};
    private long[] remaining = {0, 0};
    private boolean leftover;
    private boolean failDelete;
    private boolean failRecovery;
    private int checksBeforeFirstDrop;
    private int deletionChecks;

    @Before
    public void setUp() throws Exception {
        connection = mock(Connection.class);
        jdbc = mockStatic(JdbcUtil.class);
        jdbc.when(() -> JdbcUtil.getDbConnection(ClickHouseBulkDeleter.class)).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenAnswer(call -> {
            String sql = call.getArgument(0);
            PreparedStatement statement = mock(PreparedStatement.class);
            when(statement.executeQuery()).thenAnswer(ignored -> {
                if (sql.contains("AS table_count")) {
                    if (failRecovery) throw new SQLException("unavailable replica");
                    return result(new String[]{"a", "b"}, new long[]{leftover ? 1 : 0, leftover ? 1 : 0});
                }
                if (sql.contains("AS table_exists") || sql.contains("AS total_rows"))
                    return result(new String[]{"a", "b"}, new long[]{1, 1});
                if (sql.contains("GLOBAL IN")) {
                    deletionChecks++;
                    assertTrue(sql.contains("skip_unavailable_shards = 0"));
                    return result(reportedHosts, remaining);
                }
                return result(new String[]{"a", "b"}, new long[]{0, 0});
            });
            when(statement.executeUpdate()).thenAnswer(ignored -> {
                if (sql.startsWith("DROP") && updates.isEmpty()) checksBeforeFirstDrop = deletionChecks;
                updates.add(sql);
                if (sql.startsWith("DELETE") && failDelete) throw new SQLException("response lost");
                return 1;
            });
            return statement;
        });
    }

    private ResultSet result(String[] hosts, long[] counts) throws SQLException {
        ResultSet result = mock(ResultSet.class);
        int[] index = {-1};
        when(result.next()).thenAnswer(call -> ++index[0] < hosts.length);
        when(result.getString("host")).thenAnswer(call -> hosts[index[0]]);
        when(result.getLong(anyString())).thenAnswer(call -> counts[index[0]]);
        return result;
    }

    private boolean deletionIsComplete() throws Exception {
        Method method = ClickHouseBulkDeleter.class.getDeclaredMethod("deletionIsComplete");
        method.setAccessible(true);
        return (boolean) method.invoke(ClickHouseBulkDeleter.getBulkDeleter("test_target", "id"));
    }

    @After
    public void tearDown() throws Exception {
        jdbc.close();
        Field registry = ClickHouseBulkDeleter.class.getDeclaredField("BULK_DELETERS");
        registry.setAccessible(true);
        ((Map<?, ?>) registry.get(null)).clear();
    }

    @Test
    public void everyReplicaMustReportZero() throws Exception {
        remaining = new long[]{0, 1};
        assertFalse(deletionIsComplete());
        remaining = new long[]{0, 0};
        assertTrue(deletionIsComplete());
        reportedHosts = new String[]{"a"};
        remaining = new long[]{0};
        assertFalse(deletionIsComplete());
        reportedHosts = new String[]{};
        remaining = new long[]{};
        assertFalse(deletionIsComplete());
    }

    @Test
    public void confirmsLeftoverBeforeDrop() throws Exception {
        leftover = true;
        ClickHouseBulkDeleter.getBulkDeleter("test_target", "id").addId(2);
        ClickHouseBulkDeleter.flushAll();
        assertEquals(1, checksBeforeFirstDrop);
        assertEquals(2, deletionChecks);
        assertTrue(updates.get(updates.size() - 1).startsWith("DROP"));
    }

    @Test
    public void ambiguousDeleteKeepsStagingTableAndInvalidatesDeleter() throws Exception {
        failDelete = true;
        ClickHouseBulkDeleter deleter = ClickHouseBulkDeleter.getBulkDeleter("test_target", "id");
        deleter.addId(2);
        assertThrows(DaoException.class, ClickHouseBulkDeleter::flushAll);
        assertTrue(updates.get(updates.size() - 1).startsWith("DELETE"));
        assertThrows(RuntimeException.class, () -> deleter.addId(3));
        assertNotSame(deleter, ClickHouseBulkDeleter.getBulkDeleter("test_target", "id"));
    }

    @Test
    public void failedRecoveryCannotDropOrReplacePreviousStagingTable() throws Exception {
        failRecovery = true;
        ClickHouseBulkDeleter.getBulkDeleter("test_target", "id").addId(2);
        assertThrows(DaoException.class, ClickHouseBulkDeleter::flushAll);
        assertTrue(updates.isEmpty());
    }
}
