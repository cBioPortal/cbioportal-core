# Opt-in importer JDBC diagnostics

Enable on a diagnostic importer JVM with `-Dcbio.jdbc.diagnostics=true`.
For Python-launched importer JVMs, set `JAVA_TOOL_OPTIONS=-Dcbio.jdbc.diagnostics=true`
in the diagnostic container. Do not include credentials in this environment variable.
Unset the property to restore the original connection path (default).

The `CBIO_JDBC` lines go directly to stderr, including in a standalone core JAR
without an SLF4J provider. This is deliberately scoped instrumentation, not a
global JDBC/HTTP debug setting. It does not add retries, change connection/pool
timeouts, alter SQL, or enable derived-table generation. No production deployment
or database access is required to run its unit tests.

## Evidence provided

- UTC timestamp, process UUID, connection ID, calling DAO class, statement ID and
  operation event ID. Correlate these with the surrounding importer study/phase
  log; process IDs do not persist across separately launched Java commands.
- Start/success/failure and monotonic elapsed milliseconds for connection
  acquisition, statement preparation/creation/execution, commit, rollback and close.
  A start with no completion also locates a blocked operation.
- DBCP active/idle/max connections and pool wait limit at acquisition and failure.
  Acquisition includes pool wait, connection creation and validation; it is not
  a measurement of TCP connect latency alone.
- Configured JDBC host/port and numeric URL `connection_timeout`, `connect_timeout`
  and `socket_timeout` values. These are **URL settings**, not a claim about
  effective driver defaults or settings supplied through other property sources.
- Exception classes, SQL state/vendor code and stack locations, traversing causes,
  chained SQL exceptions and suppressed exceptions with cycle/size limits
  (32 exceptions, 24 stack frames per exception).

For example, a `Connection.prepareStatement phase=failure` with a nested
`SocketTimeoutException` distinguishes metadata-fetch failure during preparation
from a failed pool acquisition or `PreparedStatement.executeUpdate` call.

## Privacy and limits

Diagnostics never render JDBC URLs, userinfo, database paths, arbitrary URL query
parameters, SQL text, bound values, result rows or exception messages. Existing
importer/driver logging is unchanged and may independently print such information;
this option does **not** make an entire importer log safe for public posting.
Keep diagnostic logs access-controlled. Hostnames and DAO class names are included.

The endpoint is the configured hostname, **not the resolved IP or actual socket peer**.
There is no extra DNS lookup or network probe that could change timing or fail the
import. Physical driver connections, driver retries and server query IDs are not
instrumented. ResultSet iteration and vendor APIs reached through JDBC `unwrap`
are also outside this first implementation. Connections returned directly by a
DataSource rather than through `JdbcUtil` are outside scope.

Only enable this for a bounded diagnostic run: wrappers and synchronous stderr
events add overhead. JDBC standard interfaces are preserved, while direct casts
to vendor connection/statement classes require `unwrap` when enabled. The default
disabled path returns the original connection object unchanged.

## Testing

```sh
mvn -Dtest=TestJdbcDiagnostics test
```

Tests use fake JDBC objects, including simulated connection and preparation
timeouts. They verify delegation, original exception identity, no retries,
disabled-path identity, bounded cause traversal and sensitive-value omission.
Real ClickHouse reproduction and driver-specific socket/query tracing remain
separate follow-up validation; a diagnostic event alone does not identify the
network or server root cause.
