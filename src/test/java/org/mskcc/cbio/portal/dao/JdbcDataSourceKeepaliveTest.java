package org.mskcc.cbio.portal.dao;

import org.junit.Test;

import static org.junit.Assert.*;

public class JdbcDataSourceKeepaliveTest {

    @Test
    public void clickHouseUrlGetsProgressHeaderKeepalive() {
        assertEquals(
                "send_progress_in_http_headers=1,http_headers_progress_interval_ms=60000",
                JdbcDataSource.idleConnectionKeepaliveSettings("jdbc:clickhouse://host:8443/db?ssl=true&socket_timeout=600000"));
    }

    @Test
    public void existingCustomSettingsAreLeftAlone() {
        assertNull(JdbcDataSource.idleConnectionKeepaliveSettings("jdbc:clickhouse://host:8443/db?custom_settings=max_threads=4"));
    }

    @Test
    public void nonClickHouseUrlsAreLeftAlone() {
        assertNull(JdbcDataSource.idleConnectionKeepaliveSettings("jdbc:mysql://host:3306/db"));
        assertNull(JdbcDataSource.idleConnectionKeepaliveSettings(null));
    }
}
