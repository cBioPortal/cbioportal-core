import sys
import tempfile
import unittest
from pathlib import Path
from subprocess import CompletedProcess
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts/importer'))
import rebuild_derived_tables as rebuild

SET = 'SET function_sleep_max_microseconds_per_block = 3600000000;\n'
OPT = 'OPTIMIZE TABLE genetic_alteration FINAL;'
TAIL = OPT + '\nINSERT INTO filled SELECT * FROM source;'
SQL = SET + 'TRUNCATE TABLE IF EXISTS filled SETTINGS alter_sync = 2;\n' + TAIL
ERROR = CompletedProcess([], 236, stderr='Code: 236. DB::Exception: Timeout, interrupting '
                         'no-future-parts discovery. (ABORTED)\n(query: ' + OPT + ')\n')


class DerivedOptimizeRetry(unittest.TestCase):
    def execute(self, sql, results):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / 'derived.sql'
            path.write_text(sql)
            with patch.object(rebuild.subprocess, 'run', side_effect=results) as self.run_client, \
                    patch.object(rebuild.time, 'sleep') as self.sleep:
                rebuild.execute_clickhouse_sql(path, dict.fromkeys(
                    ('host', 'port', 'user', 'password', 'database', 'optimize_backoff_secs'), 'x'))

    def test_resumes_with_settings_without_repeating_truncate(self):
        self.execute(SQL, [ERROR, CompletedProcess([], 0)])
        self.assertEqual([SQL, SET + TAIL], [call.kwargs['input'] for call in self.run_client.call_args_list])
        self.sleep.assert_called_once_with(30)

    def test_exhaustion_is_failure(self):
        with self.assertRaisesRegex(RuntimeError, 'exit 236'):
            self.execute(SQL, [ERROR] * 4)
        self.assertEqual(self.run_client.call_count, 4)

    def test_ambiguous_or_session_dependent_script_and_other_errors_fail_closed(self):
        for sql, error in [(SQL + OPT, ERROR), ('USE other;\n' + SQL, ERROR),
                           (SQL, CompletedProcess([], 236, stderr='query cancelled'))]:
            with self.subTest(sql=sql, error=error), self.assertRaises(RuntimeError):
                self.execute(sql, [error])
            self.sleep.assert_not_called()
