package org.mskcc.cbio.portal.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class ClickHouseBulkDeleterSettingsTest {

    @Test
    public void probesSettingWithoutReadingSystemTables() throws SQLException {
        Connection con = mock(Connection.class);
        PreparedStatement stmt = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);
        when(con.prepareStatement("SELECT 1 SETTINGS lightweight_deletes_sync = 2")).thenReturn(stmt);
        when(stmt.executeQuery()).thenReturn(rs);

        assertTrue(ClickHouseBulkDeleter.supportsSynchronousDeleteSetting(con));
        verify(rs).close();
        verify(stmt).close();
    }

    @Test
    public void onlyUnknownSettingFallsBackToLegacySetting() throws SQLException {
        for (int errorCode : new int[]{115, 497, 452, 0}) {
            Connection con = mock(Connection.class);
            PreparedStatement stmt = mock(PreparedStatement.class);
            SQLException failure = new SQLException("server error", "07000", errorCode);
            when(con.prepareStatement("SELECT 1 SETTINGS lightweight_deletes_sync = 2")).thenReturn(stmt);
            when(stmt.executeQuery()).thenThrow(failure);

            if (errorCode == 115) {
                assertFalse(ClickHouseBulkDeleter.supportsSynchronousDeleteSetting(con));
            } else {
                SQLException actual = assertThrows(SQLException.class,
                        () -> ClickHouseBulkDeleter.supportsSynchronousDeleteSetting(con));
                assertSame(failure, actual);
            }
            verify(stmt).close();
        }
    }
}
