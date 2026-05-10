#!/usr/bin/env python3

import os
import re
import sys
import urllib.request
import urllib.parse

def get_usage_string():
    program_filepath = sys.argv[0]
    return f"usage : {program_filepath} derived_table_construction_file"

def get_required_properties_from_environment(ch_props):
    required_props = {
        'host': 'CLICKHOUSE_HOST',
        'port': 'CLICKHOUSE_HTTP_PORT',
        'user': 'CLICKHOUSE_USER',
        'password': 'CLICKHOUSE_PASSWORD',
        'database': 'CLICKHOUSE_DB',
    }
    optional_props = {
        'request_timeout': 'CLICKHOUSE_REQUEST_TIMEOUT',
    }
    ch_props['request_timeout'] = 500 # default value
    missing = []
    set_to_empty_string = []
    for key, env_var in required_props.items():
        if env_var not in os.environ:
            missing.append(env_var)
        else:
            value = os.environ.get(env_var)
            if not value:
                set_to_empty_string.append(env_var)
            ch_props[key] = value
    if missing or set_to_empty_string:
        raise RuntimeError(
            f"\n"
            f"    Required ClickHouse properties not set: {', '.join(missing)}\n"
            f"    Required ClickHouse properties set to empty string: {', '.join(set_to_empty_string)}\n"
            f"{get_usage_string()}"
            )
    for key, env_var in optional_props.items():
        if env_var in os.environ:
            value = os.environ.get(env_var)
            if value:
                ch_props[key] = value

def get_derived_table_sql_filepath(args):
    if len(args) != 2:
        raise RuntimeError(
            f"\n"
            f"{get_usage_string()}"
            )
    return sys.argv[1]

def rebuild_derived_tables(ch_props, derived_table_sql_filepath):
    """Rebuild ClickHouse derived tables after a database-mutating operation.

    Returns on success, exits on RuntimeException.
    """
    RED = '\033[91m'
    END = '\033[0m'
    if not derived_table_sql_filepath:
        if not os.path.exists(derived_table_sql_filepath):
            raise RuntimeError(f"Could not find derived table construction script '{derived_table_sql_filepath}'")
    with open(derived_table_sql_filepath, encoding='utf8') as f:
        sql_content = f.read()
    statement_count = execute_clickhouse_sql_via_http(sql_content, ch_props)
    print(f"derived tables have been rebuilt. {statement_count} sql statements executed.")

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
    request_timeout = ch_props['request_timeout']
    base_url = (
        f"http://{host}:{port}/?"
        f"user={urllib.parse.quote(user)}&"
        f"password={urllib.parse.quote(password)}&"
        f"database={urllib.parse.quote(database)}"
    )

    filtered_sql_content = re.sub(r'--[^\n]*', '', sql_content)
    filtered_sql_content = re.sub(r'/\*.*?\*/', '', filtered_sql_content, flags=re.DOTALL)
    statements = [s.strip() for s in filtered_sql_content.split(';') if s.strip()]

    for stmt in statements:
        data = stmt.encode()
        req = urllib.request.Request(base_url, data=data, method='POST')
        try:
            with urllib.request.urlopen(req, timeout=int(request_timeout)) as resp:
                pass
        except urllib.error.HTTPError as e:
            body = e.read().decode(errors='replace')
            raise RuntimeError(
                f"ClickHouse HTTP error {e.code}:\n"
                f"  Statement (first 300 chars): {stmt[:300]}...\n"
                f"  Response: {body[:500]}"
            )
    return len(statements)

def main():
    ch_props = {}
    get_required_properties_from_environment(ch_props)
    derived_table_sql_filepath = get_derived_table_sql_filepath(sys.argv)
    rebuild_derived_tables(ch_props, derived_table_sql_filepath)

if __name__ == "__main__":
    main()
