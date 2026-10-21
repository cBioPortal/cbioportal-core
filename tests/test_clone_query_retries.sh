#!/usr/bin/env bash
set -euo pipefail
repo_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
test_dir=$(mktemp -d)
trap 'rm -rf "$test_dir"' EXIT
cd "$test_dir"
source "$repo_dir/scripts/clickhouse_import_support/clone_clickhouse_database.sh"
[[ "$QUICK_STATEMENT_CONNECT_TIMEOUT_SECONDS" == 10 ]]
QUICK_STATEMENT_CONNECT_TIMEOUT_SECONDS=25
source "$repo_dir/scripts/clickhouse_import_support/clone_clickhouse_database.sh"
[[ "$QUICK_STATEMENT_CONNECT_TIMEOUT_SECONDS" == 25 ]]
attempts=0
sleeps=0
succeed_on=2
function sleep() { sleeps=$((sleeps + 1)); }
function execute_sql_statement_via_clickhouse_client() {
    attempts=$((attempts + 1))
    [[ "$1" == 'SELECT 1' && "$2" == "$test_dir/result" ]] || exit 97
    [[ "$3" == '--connect_timeout=25' ]] || exit 97
    [[ "$4" == '--send_timeout=120' && "$5" == '--receive_timeout=120' ]] || exit 97
    (( attempts >= succeed_on ))
}
execute_quick_sql_statement_with_retry 'SELECT 1' "$test_dir/result"
[[ "$attempts" == 2 && "$sleeps" == 1 ]]
attempts=0; sleeps=0; succeed_on=99
if execute_quick_sql_statement_with_retry 'SELECT 1' "$test_dir/result"; then
    echo 'Expected exhausted retries to fail' >&2
    exit 1
fi
[[ "$attempts" == 3 && "$sleeps" == 2 ]]
attempts=0; sleeps=0; succeed_on=1
execute_quick_sql_statement_with_retry 'SELECT 1' "$test_dir/result"
[[ "$attempts" == 1 && "$sleeps" == 0 ]]
echo 'PASS: connect timeout default/override, success, retry recovery and exhaustion'
