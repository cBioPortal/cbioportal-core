package org.mskcc.cbio.portal.dao;

import org.apache.commons.dbcp2.BasicDataSource;
import org.mskcc.cbio.portal.util.DatabaseProperties;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.Assert;

/**
 * Data source that self-initializes based on cBioPortal configuration.
 */
public class JdbcDataSource extends BasicDataSource {

    public JdbcDataSource () {
        DatabaseProperties dbProperties = DatabaseProperties.getInstance();

        String userName = dbProperties.getSpringDbUser();
        String password = dbProperties.getSpringDbPassword();
        String mysqlDriverClassName = dbProperties.getSpringDbDriverClassName();
        String enablePooling = (!StringUtils.isBlank(dbProperties.getDbEnablePooling())) ? dbProperties.getDbEnablePooling(): "false";
        String connectionURL = dbProperties.getSpringConnectionURL();
        
        Assert.isTrue(
            !defined(dbProperties.getDbHost()) && !defined(dbProperties.getDbUser()) && !defined(dbProperties.getDbPassword()) && !defined(dbProperties.getDbName()) && !defined(dbProperties.getConnectionURL()) && !defined(dbProperties.getDbDriverClassName()) && !defined(dbProperties.getDbUseSSL()),
            "\n----------------------------------------------------------------------------------------------------------------" +
                "-- Connection error:\n" +
                "-- You try to connect to the database using the deprecated 'db.host', 'db.portal_db_name' and 'db.use_ssl' or 'db.connection_string' and. 'db.driver' properties.\n" +
                "-- Please remove these properties and use the 'spring.datasource.url' property instead. See https://docs.cbioportal.org/deployment/customization/application.properties-reference/\n" +
                "-- for assistance on building a valid connection string.\n" +
                "----------------------------------------------------------------------------------------------------------------\n"
        );
        
        Assert.hasText(userName, errorMessage("username", "spring.datasource.username"));
        Assert.hasText(password, errorMessage("password", "spring.datasource.password"));
        Assert.hasText(mysqlDriverClassName, errorMessage("driver class name", "spring.datasource.driver-class-name"));

        this.setUrl(connectionURL);

        //  Set up poolable data source
        this.setDriverClassName(mysqlDriverClassName);
        this.setUsername(userName);
        this.setPassword(password);
        // Disable this to avoid caching statements
        this.setPoolPreparedStatements(Boolean.valueOf(enablePooling));
        // these are the values cbioportal has been using in their production
        // context.xml files when using jndi
        this.setMaxTotal(500);
        this.setMaxIdle(30);
        this.setMaxWaitMillis(10000);
        this.setMinEvictableIdleTimeMillis(30000);
        this.setTestOnBorrow(true);
        this.setValidationQuery("SELECT 1");
    }
    
    private String errorMessage(String displayName, String propertyName) {
        return String.format("No %s provided for database connection. Please set '%s' in application.properties.", displayName, propertyName);
    }
    
    private boolean defined(String property) {
        return property != null && !property.isEmpty();
    }
}
