#!/usr/bin/env bash
set -euo pipefail

if [[ "${CBIOPORTAL_TEST_DB_SKIP:-}" == "1" || "${CBIOPORTAL_TEST_DB_SKIP:-}" == "true" ]]; then
  echo "Skipping test DB container startup because CBIOPORTAL_TEST_DB_SKIP is set."
  exit 0
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONTAINER_NAME="${CBIOPORTAL_TEST_DB_CONTAINER:-cbioportal-core-test-mysql}"
PORT="${CBIOPORTAL_TEST_DB_PORT:-3306}"
TARGET_DIR="${ROOT}/target/test-db"
CGDS_SQL="${TARGET_DIR}/cgds.sql"
VERSION_FILE="${TARGET_DIR}/cbioportal.version"

cd "$ROOT"

CBIOPORTAL_VERSION="$(
  python3 - <<'PY'
import xml.etree.ElementTree as ET

tree = ET.parse("pom.xml")
root = tree.getroot()
ns = {"m": "http://maven.apache.org/POM/4.0.0"}
node = root.find(".//m:cbioportal.version", ns)
if node is None or not node.text:
    raise SystemExit("cbioportal.version not found in pom.xml")
print(node.text.strip())
PY
)"

mkdir -p "$TARGET_DIR"

if [[ ! -f "$CGDS_SQL" || ! -f "$VERSION_FILE" || "$(cat "$VERSION_FILE")" != "$CBIOPORTAL_VERSION" ]]; then
  echo "Downloading cgds.sql for ${CBIOPORTAL_VERSION}..."
  curl -sSfL -o "$CGDS_SQL" \
    "https://raw.githubusercontent.com/cBioPortal/cbioportal/${CBIOPORTAL_VERSION}/src/main/resources/db-scripts/cgds.sql"
  echo "$CBIOPORTAL_VERSION" > "$VERSION_FILE"
fi

if docker ps -a --format '{{.Names}}' | grep -qx "$CONTAINER_NAME"; then
  docker rm -f "$CONTAINER_NAME" >/dev/null
fi

docker run -d --name "$CONTAINER_NAME" --label cbioportal-core-test-db=true -p "${PORT}:3306" \
  -v "${ROOT}/src/test/resources/seed_mini.sql:/docker-entrypoint-initdb.d/seed.sql:ro" \
  -v "${CGDS_SQL}:/docker-entrypoint-initdb.d/cgds.sql:ro" \
  -e MYSQL_ROOT_PASSWORD=root \
  -e MYSQL_USER=cbio_user \
  -e MYSQL_PASSWORD=somepassword \
  -e MYSQL_DATABASE=cgds_test \
  mysql:5.7

echo "Waiting for cBioPortal test database..."
until docker exec "$CONTAINER_NAME" mysqladmin ping -uroot -proot -h 127.0.0.1 -P 3306 --silent; do
  sleep 2
done

until docker exec "$CONTAINER_NAME" mysql --protocol=TCP -uroot -proot -h 127.0.0.1 -P 3306 -D cgds_test \
  -e "SELECT 1 FROM cancer_study LIMIT 1;" >/dev/null 2>&1; do
  echo "Waiting for schema initialization..."
  sleep 2
done
