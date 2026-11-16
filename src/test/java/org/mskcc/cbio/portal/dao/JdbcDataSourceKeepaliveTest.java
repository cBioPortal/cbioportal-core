package org.mskcc.cbio.portal.dao;

import org.junit.Test;
import java.util.Properties;

import static org.junit.Assert.*;

public class JdbcDataSourceKeepaliveTest {

    @Test
    public void missingTimeoutsGetBoundedDefaults() {
        for (String prefix : new String[] {"jdbc:clickhouse:", "jdbc:ch:"}) {
            Properties settings = JdbcDataSource.connectionTimeoutDefaults(prefix + "//host:8443/db?ssl=true");
            assertEquals("3600000", settings.getProperty("socket_timeout"));
            assertEquals("60000", settings.getProperty("connect_timeout"));
            assertEquals("60000", settings.getProperty("connection_request_timeout"));
            assertEquals(3, settings.size());
        }
    }

    @Test
    public void explicitTimeoutsArePreservedIndependently() {
        assertTrue(JdbcDataSource.connectionTimeoutDefaults(
                "jdbc:clickhouse://host/db?socket_timeout=600000&connect_timeout=5000&connection_request_timeout=2000").isEmpty());
        Properties settings = JdbcDataSource.connectionTimeoutDefaults(
                "jdbc:clickhouse://host/db?socket_timeout=600000");
        assertNull(settings.getProperty("socket_timeout"));
        assertEquals("60000", settings.getProperty("connect_timeout"));
        settings = JdbcDataSource.connectionTimeoutDefaults("jdbc:clickhouse://host/db?connect_timeout=5000");
        assertEquals("3600000", settings.getProperty("socket_timeout"));
        assertNull(settings.getProperty("connect_timeout"));
        assertEquals("60000", settings.getProperty("connection_request_timeout"));
        settings = JdbcDataSource.connectionTimeoutDefaults(
                "jdbc:clickhouse://host/db?connection_request_timeout=2000");
        assertNull(settings.getProperty("connection_request_timeout"));
        assertEquals("3600000", settings.getProperty("socket_timeout"));
        assertEquals("60000", settings.getProperty("connect_timeout"));
    }

    @Test
    public void timeoutKeysAreParsedRatherThanMatchedInsideOtherValues() {
        Properties settings = JdbcDataSource.connectionTimeoutDefaults(
                "jdbc:clickhouse://host/db?custom_settings=log_comment%3Dsocket_timeout&%63onnect_timeout=5000");
        assertEquals("3600000", settings.getProperty("socket_timeout"));
        assertNull(settings.getProperty("connect_timeout"));
        assertTrue(JdbcDataSource.connectionTimeoutDefaults("jdbc:mysql://host/db").isEmpty());
        assertTrue(JdbcDataSource.connectionTimeoutDefaults(null).isEmpty());
    }

    @Test
    public void clickHouseUrlGetsProgressHeaderKeepalive() {
        for (String prefix : new String[] {"jdbc:clickhouse:", "jdbc:ch:"}) {
            assertEquals(
                    "send_progress_in_http_headers=1,http_headers_progress_interval_ms=60000",
                    JdbcDataSource.idleConnectionKeepaliveSettings(prefix + "//host:8443/db?ssl=true&socket_timeout=600000"));
        }
    }

    @Test
    public void existingCustomSettingsAreLeftAlone() {
        for (String prefix : new String[] {"jdbc:clickhouse:", "jdbc:ch:"}) {
            assertNull(JdbcDataSource.idleConnectionKeepaliveSettings(prefix + "//host:8443/db?custom_settings=max_threads=4"));
        }
    }

    @Test
    public void nonClickHouseUrlsAreLeftAlone() {
        assertNull(JdbcDataSource.idleConnectionKeepaliveSettings("jdbc:mysql://host:3306/db"));
        assertNull(JdbcDataSource.idleConnectionKeepaliveSettings(null));
    }
}
