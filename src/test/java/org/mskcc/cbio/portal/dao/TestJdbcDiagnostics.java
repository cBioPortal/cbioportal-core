package org.mskcc.cbio.portal.dao;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Proxy;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestJdbcDiagnostics {
    private final ByteArrayOutputStream output = new ByteArrayOutputStream();
    private PrintStream previous;
    private String previousProperty;

    @Before public void setup() {
        previous = System.err;
        previousProperty = System.getProperty(JdbcDiagnostics.ENABLED);
        System.setProperty(JdbcDiagnostics.ENABLED, "true");
        System.setErr(new PrintStream(output, true, StandardCharsets.UTF_8));
    }

    @After public void teardown() {
        System.setErr(previous);
        if (previousProperty == null) System.clearProperty(JdbcDiagnostics.ENABLED);
        else System.setProperty(JdbcDiagnostics.ENABLED, previousProperty);
    }

    private String logs() { return output.toString(StandardCharsets.UTF_8); }

    private BasicDataSource source(Connection connection) {
        BasicDataSource source = new BasicDataSource() {
            @Override public Connection getConnection() { return connection; }
        };
        source.setUrl("jdbc:clickhouse://user:secret@db.example:8443/private_db"
                + "?password=secret&socket_timeout=600000&connection_timeout=10000&token=secret");
        return source;
    }

    private Connection connection() {
        return (Connection) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[] {Connection.class}, (p, m, a) -> {
                    if (m.getName().equals("isClosed")) return false;
                    if (m.getName().equals("unwrap")) return p;
                    return null;
                });
    }

    @Test public void disabledReturnsOriginalWithoutLogging() throws Exception {
        System.clearProperty(JdbcDiagnostics.ENABLED);
        Connection original = connection();
        assertSame(original, JdbcDiagnostics.acquire(source(original), "Test"));
        assertEquals("", logs());
    }

    @Test public void acquisitionIncludesSafeSettingsOnly() throws Exception {
        JdbcDiagnostics.acquire(source(connection()), "Test");
        assertTrue(logs().contains("host=db.example port=8443"));
        assertTrue(logs().contains("url_socket_timeout=600000"));
        assertTrue(logs().contains("url_connection_timeout=10000"));
        assertTrue(logs().contains("pool_active=0"));
        assertTrue(logs().contains("elapsed_ms="));
        assertFalse(logs().contains("secret"));
        assertFalse(logs().contains("private_db"));
    }

    @Test public void acquisitionFailurePreservesExceptionAndDoesNotRetry() throws Exception {
        SQLException original = new SQLException("secret SQL and password", "08001", 42,
                new SocketTimeoutException("secret host password"));
        AtomicInteger calls = new AtomicInteger();
        BasicDataSource source = new BasicDataSource() {
            @Override public Connection getConnection() throws SQLException {
                calls.incrementAndGet();
                throw original;
            }
        };
        try {
            JdbcDiagnostics.acquire(source, "Test");
            fail();
        } catch (SQLException e) { assertSame(original, e); }
        assertEquals(1, calls.get());
        assertTrue(logs().contains("operation=getConnection phase=failure"));
        assertTrue(logs().contains("sql_state=08001 vendor_code=42"));
        assertTrue(logs().contains("java.net.SocketTimeoutException"));
        assertFalse(logs().contains("secret"));
    }

    @Test public void prepareFailureIsVisibleWithoutSql() throws Exception {
        SQLException original = new SQLException("secret SQL");
        Connection connection = (Connection) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[] {Connection.class}, (p, m, a) -> { throw original; });
        Connection wrapped = JdbcDiagnostics.acquire(source(connection), "Test");
        try { wrapped.prepareStatement("SELECT secret FROM patient"); fail(); }
        catch (SQLException e) { assertSame(original, e); }
        assertTrue(logs().contains("operation=Connection.prepareStatement phase=failure"));
        assertFalse(logs().contains("secret"));
        assertFalse(logs().contains("patient"));
    }

    @Test public void statementExecutionAndCloseDelegateWithoutParameters() throws Exception {
        AtomicInteger executions = new AtomicInteger();
        AtomicInteger closes = new AtomicInteger();
        PreparedStatement statement = (PreparedStatement) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[] {PreparedStatement.class}, (p, m, a) -> {
                    if (m.getName().equals("executeUpdate")) { executions.incrementAndGet(); return 7; }
                    if (m.getName().equals("close")) closes.incrementAndGet();
                    return null;
                });
        Connection original = (Connection) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[] {Connection.class}, (p, m, a) -> statement);
        try (PreparedStatement wrapped = JdbcDiagnostics.acquire(source(original), "Test")
                .prepareStatement("INSERT secret")) {
            wrapped.setString(1, "patient-secret");
            assertEquals(7, wrapped.executeUpdate());
        }
        assertEquals(1, executions.get());
        assertEquals(1, closes.get());
        assertTrue(logs().contains("operation=PreparedStatement.executeUpdate phase=success"));
        assertTrue(logs().contains("operation=PreparedStatement.close phase=success"));
        assertFalse(logs().contains("secret"));
    }

    @Test public void wrappersHaveSafeObjectAndJdbcMethods() throws Exception {
        Connection wrapped = JdbcDiagnostics.acquire(source(connection()), "Test");
        assertTrue(wrapped.equals(wrapped));
        assertFalse(wrapped.equals(connection()));
        assertEquals(System.identityHashCode(wrapped), wrapped.hashCode());
        assertEquals("DiagnosticJDBC[Connection]", wrapped.toString());
        assertSame(wrapped, wrapped.unwrap(Connection.class));
        assertTrue(wrapped.isWrapperFor(Connection.class));
    }

    @Test public void executeFailurePreservesOriginalAndHidesValues() throws Exception {
        SQLException original = new SQLException("secret SQL value", "08006", 9);
        PreparedStatement statement = (PreparedStatement) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[] {PreparedStatement.class}, (p, m, a) -> { throw original; });
        Connection connection = (Connection) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[] {Connection.class}, (p, m, a) -> statement);
        PreparedStatement wrapped = JdbcDiagnostics.acquire(source(connection), "Test").prepareStatement("secret");
        try { wrapped.executeBatch(); fail(); }
        catch (SQLException e) { assertSame(original, e); }
        assertTrue(logs().contains("operation=PreparedStatement.executeBatch phase=failure"));
        assertTrue(logs().contains("sql_state=08006 vendor_code=9"));
        assertFalse(logs().contains("secret"));
    }

    @Test public void plainAndCallableStatementInterfacesArePreserved() throws Exception {
        java.sql.CallableStatement statement = (java.sql.CallableStatement) Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[] {java.sql.CallableStatement.class},
                (p, m, a) -> m.getName().equals("execute") ? true : null);
        Connection connection = (Connection) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[] {Connection.class}, (p, m, a) -> statement);
        Connection wrapped = JdbcDiagnostics.acquire(source(connection), "Test");
        assertTrue(wrapped.createStatement().execute("secret"));
        assertTrue(wrapped.prepareCall("secret").execute());
        assertFalse(logs().contains("secret"));
    }

    @Test public void malformedUrlAndTimeoutValuesAreNotEchoed() {
        BasicDataSource source = source(connection());
        source.setUrl("jdbc:clickhouse://db.example:8443/private?socket_timeout=secret&password=secret");
        assertFalse(JdbcDiagnostics.settings(source).contains("secret"));
        source.setUrl("not a URI containing secret");
        assertTrue(JdbcDiagnostics.settings(source).contains("endpoint=unparsed"));
        assertFalse(JdbcDiagnostics.settings(source).contains("secret"));
    }

    @Test public void nextAndSuppressedExceptionsAreBoundedAndRedacted() throws Exception {
        SQLException original = new SQLException("secret");
        SQLException next = new SQLException("secret", "08006", 3);
        original.setNextException(next);
        next.initCause(original);
        original.addSuppressed(new IllegalArgumentException("secret"));
        BasicDataSource source = new BasicDataSource() {
            @Override public Connection getConnection() throws SQLException { throw original; }
        };
        try { JdbcDiagnostics.acquire(source, "Test"); fail(); }
        catch (SQLException e) { assertSame(original, e); }
        assertTrue(logs().contains("exception_relation=next"));
        assertTrue(logs().contains("exception_relation=suppressed"));
        assertFalse(logs().contains("secret"));
        assertTrue(logs().length() < 50000);
    }
}
