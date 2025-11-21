#!/bin/bash
# Import cBioPortal schema into SQLite database

DB_FILE="${1:-cbioportal.db}"
SCHEMA_FILE="cgds_sqlite.sql"

echo "=============================================="
echo "cBioPortal SQLite Schema Importer"
echo "=============================================="
echo ""

# Check if SQLite is installed
if ! command -v sqlite3 &> /dev/null; then
    echo "Error: sqlite3 is not installed"
    echo "Please install SQLite3:"
    echo "  - macOS: brew install sqlite"
    echo "  - Ubuntu/Debian: sudo apt-get install sqlite3"
    echo "  - CentOS/RHEL: sudo yum install sqlite"
    exit 1
fi

# Check if schema file exists
if [ ! -f "$SCHEMA_FILE" ]; then
    echo "Error: Schema file '$SCHEMA_FILE' not found"
    echo "Please run this script from the cbioportal-core directory"
    exit 1
fi

# Warn if database already exists
if [ -f "$DB_FILE" ]; then
    echo "Warning: Database file '$DB_FILE' already exists"
    read -p "Do you want to delete it and create a new one? (y/N) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        rm -f "$DB_FILE"
        echo "Deleted existing database"
    else
        echo "Import cancelled"
        exit 0
    fi
fi

echo "Creating SQLite database: $DB_FILE"
echo "Importing schema from: $SCHEMA_FILE"
echo ""

# Import the schema
if sqlite3 "$DB_FILE" < "$SCHEMA_FILE"; then
    echo ""
    echo "=============================================="
    echo "✓ Schema imported successfully!"
    echo "=============================================="
    echo ""
    echo "Database file: $DB_FILE"
    echo ""

    # Show table count
    TABLE_COUNT=$(sqlite3 "$DB_FILE" "SELECT COUNT(*) FROM sqlite_master WHERE type='table';")
    echo "Tables created: $TABLE_COUNT"
    echo ""

    # Show database size
    DB_SIZE=$(ls -lh "$DB_FILE" | awk '{print $5}')
    echo "Database size: $DB_SIZE"
    echo ""

    # Verify schema version
    SCHEMA_VERSION=$(sqlite3 "$DB_FILE" "SELECT DB_SCHEMA_VERSION FROM info;")
    echo "Schema version: $SCHEMA_VERSION"
    echo ""

    echo "To view your database:"
    echo "  sqlite3 $DB_FILE"
    echo ""
    echo "Useful SQLite commands:"
    echo "  .tables                  # List all tables"
    echo "  .schema table_name       # Show table structure"
    echo "  .quit                    # Exit SQLite"
    echo ""
else
    echo ""
    echo "=============================================="
    echo "✗ Error importing schema"
    echo "=============================================="
    echo "Check the error messages above for details"
    exit 1
fi
