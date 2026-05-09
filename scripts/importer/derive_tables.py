
import os
import re
import sys
import urllib.request
import urllib.parse


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
            derived_table_sql_filepath = os.path.join(portal_home, 'clickhouse.sql')
            if not os.path.exists(derived_table_sql_filepath):
                raise RuntimeError(f"Could not find derived table script at {derived_table_sql_filepath}")

        required_props = {
            'host': 'CLICKHOUSE_HOST',
            'port': 'CLICKHOUSE_PORT',
            'user': 'CLICKHOUSE_USER',
            'password': 'CLICKHOUSE_PASSWORD',
            'database': 'CLICKHOUSE_DATABASE',
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

        with open(derived_table_sql_filepath, encoding='utf8') as f:
            sql_content = f.read()

        execute_clickhouse_sql_via_http(sql_content, ch_props)
        return True
    except Exception as e:
        print(RED + f"Derived table construction failed: {e}" + END, file=sys.stderr)
        return False

def execute_clickhouse_sql_via_http(sql_content, ch_props):
    """Execute SQL statements via ClickHouse HTTP interface.

    Strips single-line and multi-line comments, splits on semicolons,
    and POSTs each statement individually to ClickHouse.

    Returns number of statements executed.
    Raises RuntimeError on failure.
    """
    host = ch_props['host']
    port = ch_props['port']
    user = ch_props['user']
    password = ch_props['password']
    database = ch_props['database']

    base_url = (
        f"http://{host}:{port}/?"
        f"user={urllib.parse.quote(user)}&"
        f"password={urllib.parse.quote(password)}&"
        f"database={urllib.parse.quote(database)}"
    )

    sql = re.sub(r'--[^\n]*', '', sql_content)
    sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
    statements = [s.strip() for s in sql.split(';') if s.strip()]

    for stmt in statements:
        data = stmt.encode()
        req = urllib.request.Request(base_url, data=data, method='POST')
        try:
            with urllib.request.urlopen(req, timeout=300) as resp:
                pass
        except urllib.error.HTTPError as e:
            body = e.read().decode(errors='replace')
            raise RuntimeError(
                f"ClickHouse HTTP error {e.code}:\n"
                f"  Statement (first 300 chars): {stmt[:300]}...\n"
                f"  Response: {body[:500]}"
            )

    return len(statements)
