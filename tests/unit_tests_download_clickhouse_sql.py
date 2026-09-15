import unittest
from unittest.mock import patch

from clickhouse_import_support import download_clickhouse_sql_scripts_py3 as downloader


class _Response:
    def __init__(self, status, headers=None):
        self.status = status
        self._headers = headers or {}

    def getheader(self, name):
        return self._headers.get(name)


class _Connection:
    def __init__(self, response):
        self.response = response

    def request(self, method, path, headers=None):
        return None

    def getresponse(self):
        return self.response


class DownloadClickhouseSqlTests(unittest.TestCase):
    def test_rate_limit_waits_until_reset_then_retries(self):
        responses = iter(
            [
                _Response(
                    429,
                    {"x-ratelimit-remaining": "0", "x-ratelimit-reset": "1004"},
                ),
                _Response(200),
            ]
        )

        with patch.object(
            downloader.http.client,
            "HTTPSConnection",
            side_effect=lambda *args, **kwargs: _Connection(next(responses)),
        ), patch.object(downloader.time, "time", return_value=1000), patch.object(
            downloader.time, "sleep"
        ) as sleep:
            result = downloader.request_via_http_with_retry("api.github.com", "/", 2)

        self.assertEqual(result.status, 200)
        sleep.assert_called_once_with(5)

    def test_rate_limit_beyond_maximum_exits(self):
        response = _Response(
            429,
            {
                "x-ratelimit-remaining": "0",
                "x-ratelimit-reset": str(1000 + downloader.MAXIMUM_SLEEP_WAITING_FOR_RATELIMIT_RESET + 1),
            },
        )

        with patch.object(
            downloader.http.client,
            "HTTPSConnection",
            return_value=_Connection(response),
        ), patch.object(downloader.time, "time", return_value=1000), patch.object(
            downloader.time, "sleep"
        ) as sleep:
            with self.assertRaises(SystemExit):
                downloader.request_via_http_with_retry("api.github.com", "/", 2)

        sleep.assert_not_called()


if __name__ == "__main__":
    unittest.main()
