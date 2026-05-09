#!/usr/bin/env python3
"""Derived ClickHouse table construction for cBioPortal.

Reads ClickHouse connection info from application.properties,
downloads derived table SQL files from GitHub (or reads from a local dir),
and executes each SQL statement via ClickHouse's HTTP interface.

No shell scripts, no clickhouse CLI dependency, no environment variables
for connection properties — everything comes from application.properties.
"""

import sys
import os
import re
import urllib.request
import urllib.parse
import json
import logging

LOGGER = logging.getLogger(__name__)

# GitHub source for derived table SQL
GITHUB_API_BASE = "https://api.github.com/repos/cBioPortal/cbioportal/contents"
SQL_PATH = "src/main/resources/db-scripts/clickhouse"
DEFAULT_BRANCH = "master"


def parse_clickhouse_properties(properties_filepath):
    """Parse ClickHouse connection info from application.properties.

    Reads standard Spring Boot datasource properties:
      spring.datasource.url
      spring.datasource.username
      spring.datasource.password

    The URL is expected in format: jdbc:ch://host:port/database
    or jdbc:clickhouse://host:port/database

    Returns dict with keys: host, port, user, password, database
    """
    props = {}
    with open(properties_filepath) as f:
        for line in f:
            line = line.strip()
            if line.startswith('#') or '=' not in line:
                continue
            key, _, value = line.partition('=')
            props[key.strip()] = value.strip()

    url = props.get('spring.datasource.url', '')
    if not url:
        raise ValueError(
            f"spring.datasource.url not found in {properties_filepath}")

    # Parse jdbc:ch://host:port/database or jdbc:clickhouse://host:port/database
    match = re.match(r'jdbc:(?:ch|clickhouse)://([^:/]+)(?::(\d+))?/([^?;]+)',
                     url)
    if not match:
        raise ValueError(f"Cannot parse ClickHouse URL: {url}")

    host = match.group(1)
    port = match.group(2) or '8123'
    database = match.group(3)
    user = props.get('spring.datasource.username', 'default')
    password = props.get('spring.datasource.password', '')

    return {
        'host': host,
        'port': port,
        'user': user,
        'password': password,
        'database': database,
    }


def download_sql_files_from_github(branch=DEFAULT_BRANCH, dest_dir=None):
    """Download all .sql files from the cBioPortal GitHub repo.

    Returns list of (filename, sql_content) tuples.
    """
    import http.client
    import time

    api_path = (f"/repos/cBioPortal/cbioportal/contents/{SQL_PATH}"
                f"?ref={branch}")

    conn = http.client.HTTPSConnection("api.github.com", timeout=60)
    conn.request("GET", api_path, headers={
        "User-Agent": "cbioportal-metaimport",
        "Accept": "application/vnd.github+json",
    })
    response = conn.getresponse()
    if response.status != 200:
        raise RuntimeError(
            f"Failed to list SQL files from GitHub: HTTP {response.status}\n"
            f"URL: https://api.github.com{api_path}")

    directory_content = json.loads(response.read().decode())
    sql_files = []

    for item in directory_content:
        name = item.get("name", "")
        if not name.casefold().endswith(".sql"):
            continue

        file_url = item["url"]
        conn2 = http.client.HTTPSConnection("api.github.com", timeout=60)
        conn2.request("GET", file_url, headers={
            "User-Agent": "cbioportal-metaimport",
            "Accept": "application/vnd.github+json",
        })
        file_resp = conn2.getresponse()
        if file_resp.status != 200:
            LOGGER.warning(
                f"Failed to download {name}: HTTP {file_resp.status}")
            continue

        import base64
        file_data = json.loads(file_resp.read().decode())
        content = base64.b64decode(file_data["content"]).decode()
        sql_files.append((name, content))

        if dest_dir:
            os.makedirs(dest_dir, exist_ok=True)
            with open(os.path.join(dest_dir, name), "w") as f:
                f.write(content)

    return sql_files


def read_sql_files_from_dir(sql_dir):
    """Read all .sql files from a local directory.

    Returns list of (filename, sql_content) tuples, sorted by filename.
    """
    sql_files = []
    for filename in sorted(os.listdir(sql_dir)):
        if filename.casefold().endswith(".sql"):
            path = os.path.join(sql_dir, filename)
            with open(path) as f:
                sql_files.append((filename, f.read()))
    return sql_files


def execute_sql_via_http(ch_props, sql_content):
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

    # Strip single-line comments (-- to end of line)
    sql = re.sub(r'--[^\n]*', '', sql_content)
    # Remove multi-line comments (/* ... */)
    sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
    # Split on semicolons
    statements = [s.strip() for s in sql.split(';') if s.strip()]

    for stmt in statements:
        data = stmt.encode()
        req = urllib.request.Request(base_url, data=data, method='POST')
        try:
            with urllib.request.urlopen(req, timeout=300) as resp:
                pass  # success — ClickHouse returns 200 with result data
        except urllib.error.HTTPError as e:
            body = e.read().decode(errors='replace')
            raise RuntimeError(
                f"ClickHouse HTTP error {e.code}:\n"
                f"  Statement (first 300 chars): {stmt[:300]}...\n"
                f"  Response: {body[:500]}"
            )

    return len(statements)


def derive_tables(ch_props, sql_files):
    """Construct all derived tables from SQL files.

    Args:
        ch_props: dict from parse_clickhouse_properties()
        sql_files: list of (name, content) tuples

    Returns total number of statements executed.
    Raises RuntimeError on any failure.
    """
    total = 0
    for name, content in sql_files:
        LOGGER.info(f"  Processing {name}...")
        count = execute_sql_via_http(ch_props, content)
        LOGGER.info(f"  {name}: executed {count} statements")
        total += count
    return total


def main_derive(ch_props_filepath, github_branch=None, sql_dir=None):
    """Main entry point for derived table construction.

    Args:
        ch_props_filepath: Path to application.properties
        github_branch: GitHub branch name (overrides default)
        sql_dir: Local directory with SQL files (overrides GitHub download)

    Returns True on success, False on failure.
    """
    try:
        print("Parsing ClickHouse connection properties from "
              f"{ch_props_filepath}...")
        ch_props = parse_clickhouse_properties(ch_props_filepath)
        print(f"  Host: {ch_props['host']}:{ch_props['port']}, "
              f"Database: {ch_props['database']}")

        if sql_dir:
            print(f"Reading SQL files from local directory: {sql_dir}")
            sql_files = read_sql_files_from_dir(sql_dir)
        else:
            branch = github_branch or DEFAULT_BRANCH
            print(f"Downloading SQL files from GitHub "
                  f"(cBioPortal/cbioportal, branch: {branch})...")
            sql_files = download_sql_files_from_github(branch)

        if not sql_files:
            print("ERROR: No SQL files found!", file=sys.stderr)
            return False

        print(f"Found {len(sql_files)} SQL file(s)")
        total = derive_tables(ch_props, sql_files)
        print(f"Derived tables rebuilt successfully "
              f"({total} statements executed).")
        return True

    except RuntimeError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return False
    except Exception as e:
        print(f"ERROR: Unexpected error during derived table construction: "
              f"{e}", file=sys.stderr)
        LOGGER.exception("Unexpected error")
        return False
