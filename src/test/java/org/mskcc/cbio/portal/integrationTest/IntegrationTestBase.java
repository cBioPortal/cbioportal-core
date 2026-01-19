package org.mskcc.cbio.portal.integrationTest;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

public abstract class IntegrationTestBase {
    private static final Object LOCK = new Object();
    private static final String DB_NAME = "cgds_test";
    private static final String DB_USER = "cbio_user";
    private static final String DB_PASSWORD = "somepassword";
    private static final String ROOT_PASSWORD = "root";
    private static final String MYSQL_IMAGE = "mysql:5.7";
    private static final String SKIP_ENV = "CBIOPORTAL_TEST_DB_SKIP";
    private static final String TARGET_DIR = "target/test-db";
    private static final String VERSION_FILE = "cbioportal.version";
    private static final String CGDS_SQL_FILE = "cgds.sql";
    private static final String CGDS_URL_TEMPLATE =
        "https://raw.githubusercontent.com/cBioPortal/cbioportal/%s/src/main/resources/db-scripts/cgds.sql";
    private static final Pattern CBIOPORTAL_VERSION_PATTERN =
        Pattern.compile("<cbioportal\\.version>\\s*([^<]+)\\s*</cbioportal\\.version>");

    private static MySQLContainer<?> container;

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(IntegrationTestBase::stopContainer));
        ensureStarted();
    }

    protected static void ensureStarted() {
        getOrStartContainer();
    }

    protected static void restartTestDb() {
        synchronized (LOCK) {
            if (shouldSkip()) {
                return;
            }
            stopContainer();
            container = startContainer();
        }
    }

    private static MySQLContainer<?> getOrStartContainer() {
        synchronized (LOCK) {
            if (container == null && !shouldSkip()) {
                container = startContainer();
            }
            return container;
        }
    }

    private static void stopContainer() {
        synchronized (LOCK) {
            if (container != null) {
                container.stop();
                container = null;
            }
        }
    }

    private static boolean shouldSkip() {
        String value = System.getenv(SKIP_ENV);
        if (value == null) {
            value = System.getProperty(SKIP_ENV);
        }
        if (value == null) {
            return false;
        }
        String normalized = value.trim().toLowerCase(Locale.ROOT);
        return "1".equals(normalized) || "true".equals(normalized);
    }

    private static MySQLContainer<?> startContainer() {
        Path cgdsPath = ensureCgdsSql();
        MySQLContainer<?> mysql = new MySQLContainer<>(DockerImageName.parse(MYSQL_IMAGE))
            .withDatabaseName(DB_NAME)
            .withUsername(DB_USER)
            .withPassword(DB_PASSWORD)
            .withEnv("MYSQL_ROOT_PASSWORD", ROOT_PASSWORD)
            .withStartupTimeout(Duration.ofMinutes(5))
            .withCopyFileToContainer(
                MountableFile.forClasspathResource("seed_mini.sql"),
                "/docker-entrypoint-initdb.d/seed.sql")
            .withCopyFileToContainer(
                MountableFile.forHostPath(cgdsPath),
                "/docker-entrypoint-initdb.d/cgds.sql");
        mysql.start();
        setSystemProperties(mysql);
        waitForSchema(mysql);
        return mysql;
    }

    private static void setSystemProperties(MySQLContainer<?> mysql) {
        String host = mysql.getHost();
        String port = Integer.toString(mysql.getMappedPort(3306));
        String jdbcUrl = buildJdbcUrl(host, port, DB_NAME);

        System.setProperty("db.test.host", host);
        System.setProperty("db.test.port", port);
        System.setProperty("db.test.database", DB_NAME);
        System.setProperty("db.test.username", DB_USER);
        System.setProperty("db.test.password", DB_PASSWORD);

        System.setProperty("spring.datasource.url", jdbcUrl);
        System.setProperty("spring.datasource.username", DB_USER);
        System.setProperty("spring.datasource.password", DB_PASSWORD);
        System.setProperty("spring.datasource.driver-class-name", "com.mysql.jdbc.Driver");
    }

    private static void waitForSchema(MySQLContainer<?> mysql) {
        String jdbcUrl = buildJdbcUrl(mysql.getHost(), Integer.toString(mysql.getMappedPort(3306)), DB_NAME);
        long deadline = System.currentTimeMillis() + Duration.ofMinutes(5).toMillis();
        while (System.currentTimeMillis() < deadline) {
            try (Connection connection = DriverManager.getConnection(jdbcUrl, DB_USER, DB_PASSWORD);
                 Statement statement = connection.createStatement();
                 ResultSet result = statement.executeQuery("SELECT 1 FROM cancer_study LIMIT 1")) {
                if (result.next()) {
                    return;
                }
            } catch (Exception ex) {
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted while waiting for test database", interrupted);
                }
            }
        }
        throw new IllegalStateException("Timed out waiting for test database schema initialization");
    }

    private static String buildJdbcUrl(String host, String port, String database) {
        return String.format(
            "jdbc:mysql://%s:%s/%s?allowPublicKeyRetrieval=true&allowLoadLocalInfile=true&useSSL=false",
            host,
            port,
            database);
    }

    private static Path ensureCgdsSql() {
        String cbioportalVersion = readCbioportalVersion();
        Path targetDir = Paths.get(System.getProperty("user.dir"), TARGET_DIR);
        Path cgdsPath = targetDir.resolve(CGDS_SQL_FILE);
        Path versionPath = targetDir.resolve(VERSION_FILE);
        try {
            Files.createDirectories(targetDir);
            if (!Files.exists(cgdsPath) || !Files.exists(versionPath)
                || !cbioportalVersion.equals(readFile(versionPath).trim())) {
                downloadCgdsSql(cbioportalVersion, cgdsPath);
                Files.writeString(versionPath, cbioportalVersion, StandardCharsets.UTF_8);
            }
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to prepare cgds.sql for integration tests", ex);
        }
        return cgdsPath;
    }

    private static String readCbioportalVersion() {
        Path pomPath = Paths.get(System.getProperty("user.dir"), "pom.xml");
        try {
            String pomContents = readFile(pomPath);
            Matcher matcher = CBIOPORTAL_VERSION_PATTERN.matcher(pomContents);
            if (matcher.find()) {
                return matcher.group(1).trim();
            }
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to read pom.xml for cbioportal.version", ex);
        }
        throw new IllegalStateException("cbioportal.version not found in pom.xml");
    }

    private static String readFile(Path path) throws IOException {
        return Files.readString(path, StandardCharsets.UTF_8);
    }

    private static void downloadCgdsSql(String version, Path destination) throws IOException {
        String url = String.format(CGDS_URL_TEMPLATE, version);
        System.out.println("Downloading cgds.sql for " + version + "...");
        try (InputStream input = new URL(url).openStream()) {
            Files.copy(input, destination, StandardCopyOption.REPLACE_EXISTING);
        }
    }
}
