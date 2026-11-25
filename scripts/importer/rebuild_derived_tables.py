
import os
import re
import subprocess
import sys
import time
from pathlib import Path


def rebuild_derived_tables(derived_table_sql_filepath=None):
    """Rebuild ClickHouse derived tables after a database-mutating operation.

    Returns True on success, False on failure.
    """
    RED = '\033[91m'
    END = '\033[0m'
    try:
        if not derived_table_sql_filepath:
            portal_home = os.environ.get('PORTAL_HOME', '')
            if not portal_home:
                raise RuntimeError("PORTAL_HOME not set, could not locate derived table script")
            derived_table_sql_filepath = os.path.join(
                portal_home, 'db-scripts', 'clickhouse', 'populate_derived_tables.sql')
            if not os.path.exists(derived_table_sql_filepath):
                raise RuntimeError(f"Could not find derived table script at {derived_table_sql_filepath}")

        required_props = {
            'host': 'CLICKHOUSE_HOST',
            'port': 'CLICKHOUSE_NATIVE_PORT', # the clickhouse-client binary communicates over the native port for maximum efficiency
            'user': 'CLICKHOUSE_USER',
            'password': 'CLICKHOUSE_PASSWORD',
            'database': 'CLICKHOUSE_DB',
        }
        missing = []
        ch_props = {}
        for key, env_var in required_props.items():
            value = os.environ.get(env_var)
            if not value:
                missing.append(env_var)
            ch_props[key] = value
        if missing:
            raise RuntimeError(
                f"ClickHouse properties not set: {', '.join(missing)}")
        
        ch_props['optimize_backoff_secs'] = os.environ.get('CLICKHOUSE_OPTIMIZE_BACKOFF_SECS', '0')

        execute_clickhouse_sql(derived_table_sql_filepath, ch_props)
        return True
    except Exception as e:
        print(RED + f"Derived table construction failed: {e}" + END, file=sys.stderr)
        return False

# A retry opens a new client session. Resume only inside the standalone SQL
# preamble used by populate_derived_tables.sql, before any session-dependent
# statements. Restore its numeric SETs and leave all completed writes alone.
def _discovery_retry_sql(sql, result):
    if result.returncode != 236:
        return None
    failure = re.search(
        r"^Code: 236\.[^\n]*Timeout, interrupting no-future-parts discovery\. \(ABORTED\)[^\n]*\n"
        r"\(query: (OPTIMIZE TABLE \w+ FINAL;)\)\s*\Z", result.stderr, re.MULTILINE)
    if not failure:
        return None
    statement = failure.group(1)
    if sql.count(statement) != 1:
        return None
    settings = []
    lines = sql.splitlines(keepends=True)
    for index, line in enumerate(lines):
        command = line.strip()
        if command == statement:
            return ''.join(settings + lines[index:])
        if not command or command.startswith('--'):
            continue
        if re.fullmatch(r"SET \w+ = [0-9]+;", command):
            settings.append(line)
            continue
        if not re.fullmatch(
            r"(?:TRUNCATE TABLE IF EXISTS \w+ SETTINGS alter_sync = 2"
            r"|OPTIMIZE TABLE \w+ FINAL"
            r"|SELECT sleepEachRow\(1\) FROM numbers\(\{optimize_backoff_secs:UInt64\}\) FORMAT Null);",
            command):
            return None
    return None


def execute_clickhouse_sql(sql_filepath, ch_props):
    """Execute a SQL file via the clickhouse client using --multiquery."""
    cmd = [
        'clickhouse', 'client',
        '--host', ch_props['host'],
        '--port', ch_props['port'],
        '--user', ch_props['user'],
        '--password', ch_props['password'],
        '--database', ch_props['database'],
        '--multiquery',
        '--param_optimize_backoff_secs', ch_props['optimize_backoff_secs'],
        '--echo',
        '--send_logs_level=information'
    ]
    sql = Path(sql_filepath).read_text(encoding='utf-8')
    for delay in (30, 60, 120, None):
        try:
            result = subprocess.run(cmd, input=sql, capture_output=True, text=True)
        except FileNotFoundError:
            raise RuntimeError(
                "clickhouse client not found. Install it with:\n"
                "  curl https://clickhouse.com/install | sh"
            )
        if result.returncode == 0:
            return
        remaining = _discovery_retry_sql(sql, result)
        if remaining is None or delay is None:
            raise RuntimeError(
                f"clickhouse client failed (exit {result.returncode}):\n{result.stderr}"
            )
        print(f"ClickHouse OPTIMIZE part discovery timed out; retrying the failed statement "
              f"in {delay}s.", file=sys.stderr, flush=True)
        time.sleep(delay)
        sql = remaining
