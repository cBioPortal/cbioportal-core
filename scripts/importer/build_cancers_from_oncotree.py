#!/usr/bin/env python3
"""Build a tab-delimited cancers.txt file from the OncoTree tumorTypes API.

This mirrors ImporterImpl.importTypesOfCancer logic:
- query {oncotree_url}/tumorTypes[?version=...]
- write rows: code<TAB>name<TAB>color<TAB>parent
- fail if response is empty
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any, Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urljoin
from urllib.request import Request, urlopen

DEFAULT_FILENAME = "cancers.txt"


def _build_url(base_url: str, version: str | None) -> str:
    base = base_url if base_url.endswith("/") else base_url + "/"
    url = urljoin(base, "tumorTypes")
    if version:
        return f"{url}?{urlencode({'version': version})}"
    return url


def _query_oncotree(url: str) -> list[dict[str, Any]]:
    request = Request(url, headers={"Content-Type": "application/x-www-form-urlencoded"})
    for attempt in range(2):
        try:
            with urlopen(request, timeout=60) as response:
                payload = response.read().decode("utf-8")
            data = json.loads(payload)
            if not isinstance(data, list):
                raise ValueError("Unexpected OncoTree response: expected a JSON list")
            return data
        except (HTTPError, URLError, TimeoutError) as exc:
            if attempt == 0:
                print("Warning: error loading OncoTree data, reattempting in 5 seconds...", file=sys.stderr)
                print(f"Warning detail: {exc}", file=sys.stderr)
                time.sleep(5)
                continue
            raise

    # Unreachable but keeps static analyzers happy.
    raise RuntimeError("Failed to query OncoTree")


def _normalize(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _to_lines(rows: Iterable[dict[str, Any]]) -> list[str]:
    lines: list[str] = []
    for row in rows:
        code = _normalize(row.get("code"))
        name = _normalize(row.get("name"))
        color = _normalize(row.get("color"))
        parent = _normalize(row.get("parent"))
        lines.append("\t".join([code, name, color, parent]))
    return lines


def build(oncotree_url: str, oncotree_version: str | None = None, output: str = DEFAULT_FILENAME) -> str:
    """Fetch cancer types from OncoTree and write to a file. Returns the output file path."""
    url = _build_url(oncotree_url, oncotree_version or None)
    oncotree_data = _query_oncotree(url)

    if not oncotree_data:
        raise RuntimeError("No oncotree data returned!")

    lines = _to_lines(oncotree_data)
    output_path = Path(output)
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(f"Wrote {len(lines)} rows to {output_path}")
    return str(output_path)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build cancers.txt from OncoTree tumorTypes API using importer-equivalent logic."
    )
    parser.add_argument(
        "--oncotree-url",
        required=True,
        help="OncoTree base URL (equivalent to importer property oncotree.url)",
    )
    parser.add_argument(
        "--oncotree-version",
        default="",
        help="Optional OncoTree version value passed as query parameter 'version'.",
    )
    parser.add_argument(
        "--output",
        default=DEFAULT_FILENAME,
        help=f"Output filename (default: {DEFAULT_FILENAME})",
    )

    args = parser.parse_args()
    build(args.oncotree_url, args.oncotree_version or None, args.output)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # noqa: BLE001
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1)
