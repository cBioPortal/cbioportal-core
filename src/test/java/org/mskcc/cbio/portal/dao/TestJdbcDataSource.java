package org.mskcc.cbio.portal.dao;

import static org.junit.Assert.*;

import java.time.Duration;
import org.junit.Test;
import org.mskcc.cbio.portal.util.DatabaseProperties;

public class TestJdbcDataSource {
    @Test
    public void clickHouseConnectionValidationUsesBoundedDriverPing() throws Exception {
        DatabaseProperties properties = DatabaseProperties.getInstance();
        String previousUser = properties.getSpringDbUser();
        String previousPassword = properties.getSpringDbPassword();
        String previousUrl = properties.getSpringConnectionURL();
        String previousDriver = properties.getSpringDbDriverClassName();
        try {
            properties.setSpringDbUser("test");
            properties.setSpringDbPassword("test");
            properties.setSpringConnectionURL("jdbc:clickhouse://localhost:8443/test");
            properties.setSpringDbDriverClassName("com.clickhouse.jdbc.ClickHouseDriver");
            try (JdbcDataSource source = new JdbcDataSource()) {
                assertTrue(source.getTestOnBorrow());
                assertTrue(source.getTestWhileIdle());
                assertNull("No SQL validation query: DBCP must call Connection.isValid", source.getValidationQuery());
                assertEquals(Duration.ofSeconds(10), source.getValidationQueryTimeoutDuration());
            }
        } finally {
            properties.setSpringDbUser(previousUser);
            properties.setSpringDbPassword(previousPassword);
            properties.setSpringConnectionURL(previousUrl);
            properties.setSpringDbDriverClassName(previousDriver);
        }
    }
}
