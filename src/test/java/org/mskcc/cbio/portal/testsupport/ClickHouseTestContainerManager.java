package org.mskcc.cbio.portal.testsupport;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;
import org.testcontainers.containers.Container.ExecResult;

final class ClickHouseTestContainerManager {

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseTestContainerManager.class);
    private static final String DATABASE_NAME = "cgds_test";
    private static final String DB_USER = "cbio_test_user";
    private static final String DB_PASSWORD = "cbio_test_password";
    private static final AtomicBoolean INITIALIZED = new AtomicBoolean(false);

    private static GenericContainer<?> container;

    private ClickHouseTestContainerManager() {
    }

    static void ensureStarted() {
        if (INITIALIZED.get()) {
            return;
        }
        synchronized (ClickHouseTestContainerManager.class) {
            if (INITIALIZED.get()) {
                return;
            }
            startContainer();
            applySql("db/clickhouse/cgds.clickhouse.sql");
            applySql("db/clickhouse/seed_mini.clickhouse.sql");
            String jdbcUrl = String.format("jdbc:clickhouse://%s:%d/%s",
                container.getHost(),
                container.getMappedPort(8123),
                DATABASE_NAME);
            System.setProperty("test.datasource.url", jdbcUrl);
            System.setProperty("test.datasource.username", DB_USER);
            System.setProperty("test.datasource.password", DB_PASSWORD);
            INITIALIZED.set(true);
        }
    }

    private static void startContainer() {
        DockerImageName image = DockerImageName.parse("clickhouse/clickhouse-server:24.3");
        LOG.info("Starting ClickHouse test container with image {}", image);
        container = new GenericContainer<>(image)
            .withEnv("CLICKHOUSE_DB", DATABASE_NAME)
            .withEnv("CLICKHOUSE_USER", DB_USER)
            .withEnv("CLICKHOUSE_PASSWORD", DB_PASSWORD)
            .withExposedPorts(8123, 9000)
            .withReuse(true)
            .waitingFor(Wait.forHttp("/").forStatusCode(200).withStartupTimeout(Duration.ofMinutes(2)));
        container.start();
        String jdbcUrl = String.format("jdbc:clickhouse://%s:%d/%s",
            container.getHost(),
            container.getMappedPort(8123),
            DATABASE_NAME);
        LOG.info("ClickHouse test container started on JDBC URL {}", jdbcUrl);
    }

    private static void applySql(String relativePath) {
        Path sqlPath = Path.of(relativePath);
        if (!Files.exists(sqlPath)) {
            throw new IllegalStateException("Missing SQL file: " + sqlPath.toAbsolutePath());
        }
        String target = "/tmp/" + sqlPath.getFileName();
        container.copyFileToContainer(MountableFile.forHostPath(sqlPath), target);
        try {
            ExecResult result = container.execInContainer(
                "clickhouse-client",
                "--user",
                DB_USER,
                "--password",
                DB_PASSWORD,
                "--multiquery",
                "--database",
                DATABASE_NAME,
                "--queries-file",
                target
            );
            if (result.getExitCode() != 0) {
                throw new IllegalStateException("Failed to apply " + sqlPath + ": " + result.getStderr());
            }
            LOG.info("Applied SQL file {}", sqlPath);
        } catch (IOException | InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Failed to execute SQL file " + sqlPath, e);
        }
    }
}
