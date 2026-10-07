package org.mskcc.cbio.portal.dao;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import javax.sql.DataSource;
import org.apache.commons.dbcp2.BasicDataSource;

/** Opt-in JDBC boundary diagnostics. Never renders SQL, arguments or exception messages. */
final class JdbcDiagnostics {
    static final String ENABLED = "cbio.jdbc.diagnostics";
    private static final String PROCESS = UUID.randomUUID().toString();
    private static final AtomicLong IDS = new AtomicLong();

    private JdbcDiagnostics() { }

    static Connection acquire(DataSource source, String requester) throws SQLException {
        if (!Boolean.getBoolean(ENABLED)) {
            return source.getConnection();
        }
        String context = "process=" + PROCESS + " connection=" + IDS.incrementAndGet()
                + " requester=" + token(requester);
        long start = System.nanoTime();
        emit(context + " operation=getConnection phase=start" + settings(source));
        try {
            Connection connection = source.getConnection();
            emit(context + " operation=getConnection phase=success elapsed_ms=" + elapsed(start)
                    + pool(source));
            return wrap(connection, Connection.class, context, source);
        } catch (SQLException | RuntimeException e) {
            failure(context, "getConnection", start, e, source);
            throw e;
        }
    }

    private static <T> T wrap(T target, Class<T> type, String context, DataSource source) {
        return type.cast(Proxy.newProxyInstance(JdbcDiagnostics.class.getClassLoader(),
                new Class<?>[] {type}, (proxy, method, args) -> {
                    // Preserve JDBC wrapper escape hatches; vendor APIs remain uninstrumented.
                    if (method.getName().equals("unwrap") && ((Class<?>) args[0]).isInstance(proxy)) {
                        return proxy;
                    }
                    if (method.getName().equals("isWrapperFor") && ((Class<?>) args[0]).isInstance(proxy)) {
                        return true;
                    }
                    if (method.getDeclaringClass() == Object.class) {
                        switch (method.getName()) {
                            case "equals": return proxy == args[0];
                            case "hashCode": return System.identityHashCode(proxy);
                            case "toString": return "DiagnosticJDBC[" + type.getSimpleName() + "]";
                            default: break;
                        }
                    }
                    boolean traced = method.getName().startsWith("prepare")
                            || method.getName().startsWith("createStatement")
                            || method.getName().startsWith("execute")
                            || method.getName().equals("commit") || method.getName().equals("rollback")
                            || method.getName().equals("close");
                    long start = System.nanoTime();
                    String operation = type.getSimpleName() + "." + method.getName();
                    String event = context + " event=" + IDS.incrementAndGet();
                    if (traced) emit(event + " operation=" + operation + " phase=start");
                    try {
                        Object result = invoke(target, method, args);
                        if (traced) emit(event + " operation=" + operation
                                + " phase=success elapsed_ms=" + elapsed(start));
                        if (result instanceof CallableStatement) {
                            return wrap((CallableStatement) result, CallableStatement.class,
                                    context + " statement=" + IDS.incrementAndGet(), source);
                        }
                        if (result instanceof PreparedStatement) {
                            return wrap((PreparedStatement) result, PreparedStatement.class,
                                    context + " statement=" + IDS.incrementAndGet(), source);
                        }
                        if (result instanceof Statement) {
                            return wrap((Statement) result, Statement.class,
                                    context + " statement=" + IDS.incrementAndGet(), source);
                        }
                        return result;
                    } catch (Throwable e) {
                        if (traced) failure(event, operation, start, e, source);
                        throw e;
                    }
                }));
    }

    private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    private static long elapsed(long start) {
        return (System.nanoTime() - start) / 1_000_000;
    }

    private static void failure(String context, String operation, long start, Throwable error,
            DataSource source) {
        emit(context + " operation=" + operation + " phase=failure elapsed_ms=" + elapsed(start)
                + pool(source));
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        describe(context, error, "root", seen);
    }

    private static void describe(String context, Throwable error, String relation, Set<Throwable> seen) {
        if (error == null || seen.size() >= 32 || !seen.add(error)) return;
        String details = context + " exception_relation=" + relation + " exception="
                + token(error.getClass().getName());
        if (error instanceof SQLException) {
            SQLException sql = (SQLException) error;
            details += " sql_state=" + (sql.getSQLState() != null
                    && sql.getSQLState().matches("[A-Z0-9]{5}") ? sql.getSQLState() : "unknown")
                    + " vendor_code=" + sql.getErrorCode();
        }
        emit(details);
        // Stack locations only: Throwable.toString/printStackTrace can expose SQL and credentials.
        StackTraceElement[] frames = error.getStackTrace();
        for (int i = 0; i < Math.min(frames.length, 24); i++) {
            emit(context + " frame=" + token(frames[i].getClassName()) + "."
                    + token(frames[i].getMethodName()) + ":" + frames[i].getLineNumber());
        }
        describe(context, error.getCause(), "cause", seen);
        if (error instanceof SQLException) describe(context, ((SQLException) error).getNextException(), "next", seen);
        for (Throwable suppressed : error.getSuppressed()) describe(context, suppressed, "suppressed", seen);
    }

    static String settings(DataSource source) {
        if (!(source instanceof BasicDataSource)) return " datasource=" + token(source.getClass().getName());
        BasicDataSource ds = (BasicDataSource) source;
        String result = " driver=" + token(ds.getDriverClassName()) + pool(source);
        try {
            String url = ds.getUrl();
            URI uri = URI.create(url.startsWith("jdbc:") ? url.substring(5) : url);
            result += " host=" + token(uri.getHost()) + " port=" + uri.getPort();
            // Do not log userinfo, database path, URL, or arbitrary query properties.
            if (uri.getRawQuery() != null) {
                for (String parameter : uri.getRawQuery().split("&")) {
                    String[] pair = parameter.split("=", 2);
                    if (pair.length == 2 && pair[0].matches("(connection_timeout|connect_timeout|socket_timeout)")
                            && pair[1].matches("[0-9]{1,10}")) {
                        result += " url_" + pair[0] + "=" + pair[1];
                    }
                }
            }
        } catch (RuntimeException ignored) {
            result += " endpoint=unparsed";
        }
        return result;
    }

    private static String pool(DataSource source) {
        if (!(source instanceof BasicDataSource)) return " pool=unavailable";
        BasicDataSource ds = (BasicDataSource) source;
        return " pool_active=" + ds.getNumActive() + " pool_idle=" + ds.getNumIdle()
                + " pool_max=" + ds.getMaxTotal() + " pool_wait_ms=" + ds.getMaxWaitMillis();
    }

    private static String token(String value) {
        if (value == null) return "unknown";
        return value.replaceAll("[^a-zA-Z0-9_.:\\[\\]-]", "_").substring(0, Math.min(value.length(), 200));
    }

    private static void emit(String message) {
        // The importer fat JAR may have no SLF4J provider. One line per event, no provider required.
        System.err.println("CBIO_JDBC time=" + Instant.now() + " " + message);
    }
}
