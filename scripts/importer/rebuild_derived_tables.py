
import hashlib
import json
import os
import subprocess
import sys
from datetime import datetime, timezone


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
            derived_table_sql_filepath = os.path.join(portal_home, 'populate_derived_tables.sql')
            if not os.path.isfile(derived_table_sql_filepath):
                # The primary cBioPortal repository keeps ClickHouse scripts
                # under db-scripts/clickhouse in both source and build
                # resource trees.  Accept that layout as well as the legacy
                # flat PORTAL_HOME layout used by older deployments.
                nested = os.path.join(
                    portal_home, 'db-scripts', 'clickhouse', 'populate_derived_tables.sql'
                )
                if os.path.isfile(nested):
                    derived_table_sql_filepath = nested
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
        counts = verify_source_backed_derived_tables(ch_props)
        receipt_path = os.environ.get('DERIVED_TABLE_RECEIPT')
        if receipt_path:
            with open(derived_table_sql_filepath, 'rb') as sql_file:
                sql_sha256 = hashlib.sha256(sql_file.read()).hexdigest()
            receipt = {
                'database': ch_props['database'],
                'sql_sha256': sql_sha256,
                'completed_at': datetime.now(timezone.utc).isoformat(),
                'source_derived_counts': counts,
            }
            temporary_path = f'{receipt_path}.tmp'
            with open(temporary_path, 'w', encoding='utf-8') as receipt_file:
                json.dump(receipt, receipt_file, indent=2, sort_keys=True)
                receipt_file.write('\n')
            os.replace(temporary_path, receipt_path)
        return True
    except Exception as e:
        print(RED + f"Derived table construction failed: {e}" + END, file=sys.stderr)
        return False

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
        '--queries-file', sql_filepath,
        '--param_optimize_backoff_secs', ch_props['optimize_backoff_secs'],
        '--echo',
        '--send_logs_level=information'
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
    except FileNotFoundError:
        raise RuntimeError(
            "clickhouse client not found. Install it with:\n"
            "  curl https://clickhouse.com/install | sh"
        )
    if result.returncode != 0:
        raise RuntimeError(
            f"clickhouse client failed (exit {result.returncode}):\n{result.stderr}"
        )


def execute_clickhouse_query(query, ch_props):
    """Run a bounded verification query and return TSV rows."""
    cmd = [
        'clickhouse', 'client',
        '--host', ch_props['host'],
        '--port', ch_props['port'],
        '--user', ch_props['user'],
        '--password', ch_props['password'],
        '--database', ch_props['database'],
        '--format', 'TSVRaw',
        '--query', query,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"clickhouse verification query failed (exit {result.returncode}):\n{result.stderr}"
        )
    return result.stdout


def verify_source_backed_derived_tables(ch_props):
    """Reject a rebuild that leaves a source-backed derived table empty."""
    pairs = (
        ('sample', 'sample_derived'),
        ('clinical_event', 'clinical_event_derived'),
        ('clinical_event_data', 'clinical_event_data_derived'),
        ('genetic_alteration', 'genetic_alteration_derived'),
        ('mutation', 'mutation_derived'),
    )
    query = ' UNION ALL '.join(
        f"SELECT '{source}', toString(count()), '{derived}', "
        f"toString((SELECT count() FROM {derived})) FROM {source}"
        for source, derived in pairs
    )
    rows = {}
    for line in execute_clickhouse_query(query, ch_props).splitlines():
        source, source_count, derived, derived_count = line.split('\t')
        rows[source] = {
            'source': int(source_count),
            'derived_table': derived,
            'derived': int(derived_count),
        }
        if int(source_count) > 0 and int(derived_count) == 0:
            raise RuntimeError(
                f"derived table {derived} is empty while source table {source} has "
                f"{source_count} rows"
            )
    return rows
