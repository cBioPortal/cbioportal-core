#!/bin/bash
# Import cBioPortal seed data into SQLite database

SEED_FILE="seed-cbioportal.sql"
DB_FILE="${1:-cbioportal.db}"
CONVERTED_FILE="seed-cbioportal-sqlite.sql"

echo "=============================================="
echo "cBioPortal SQLite Seed Data Importer"
echo "=============================================="
echo ""

# Check if SQLite is installed
if ! command -v sqlite3 &> /dev/null; then
    echo "Error: sqlite3 is not installed"
    exit 1
fi

# Check if seed file exists
if [ ! -f "$SEED_FILE" ]; then
    echo "Error: Seed file '$SEED_FILE' not found"
    echo ""
    echo "Please download it first:"
    echo "  curl -L -o seed-cbioportal.sql.gz 'https://github.com/cBioPortal/datahub/raw/master/seedDB/seed-cbioportal_hg19_hg38_v2.13.1.sql.gz'"
    echo "  gunzip seed-cbioportal.sql.gz"
    exit 1
fi

# Check if database exists
if [ ! -f "$DB_FILE" ]; then
    echo "Error: Database '$DB_FILE' not found"
    echo "Please create the schema first: ./import_schema.sh"
    exit 1
fi

echo "Converting MySQL dump to SQLite format..."
echo "This may take a few minutes for a 192MB file..."
echo ""

# Convert MySQL dump to SQLite-compatible SQL
# Remove MySQL-specific syntax and filter out deprecated tables
# Change INSERT to INSERT OR REPLACE to handle duplicates
sed -e '/^\/\*![0-9]* /d' \
    -e '/^\/\*!40000 ALTER TABLE/d' \
    -e '/^LOCK TABLES/d' \
    -e '/^UNLOCK TABLES/d' \
    -e '/^SET /d' \
    -e '/^--.*GTID/d' \
    -e '/^\/\*!40/d' \
    -e '/^\/\*!50/d' \
    -e "s/\\\\'/''/g" \
    -e "s/\\\\\"/\"/g" \
    -e 's/^INSERT INTO/INSERT OR REPLACE INTO/g' \
    "$SEED_FILE" | grep -v "^INSERT" | grep -v "cosmic_mutation" > "$CONVERTED_FILE"

# Add back the INSERT statements with OR REPLACE
grep "^INSERT INTO" "$SEED_FILE" | \
    grep -v "cosmic_mutation" | \
    sed -e "s/\\\\'/''/g" \
        -e "s/\\\\\"/\"/g" \
        -e 's/^INSERT INTO/INSERT OR REPLACE INTO/g' >> "$CONVERTED_FILE"

echo "Note: Filtered out 'cosmic_mutation' table (deprecated in schema v2.14.5)"
echo "Note: Using INSERT OR REPLACE to handle duplicate entries"

echo "Converted file: $CONVERTED_FILE"
CONVERTED_SIZE=$(ls -lh "$CONVERTED_FILE" | awk '{print $5}')
echo "Size: $CONVERTED_SIZE"
echo ""

echo "Importing data into database: $DB_FILE"
echo "This will take several minutes..."
echo ""

# Get initial database size
INITIAL_SIZE=$(ls -lh "$DB_FILE" | awk '{print $5}')

# Import the converted SQL file
# Use .bail on to stop on first error
# Wrap in transaction for speed
{
    echo "PRAGMA foreign_keys = OFF;"
    echo "PRAGMA synchronous = OFF;"
    echo "PRAGMA journal_mode = MEMORY;"
    echo "BEGIN TRANSACTION;"
    cat "$CONVERTED_FILE"
    echo "COMMIT;"
    echo "PRAGMA synchronous = NORMAL;"
    echo "PRAGMA foreign_keys = ON;"
} | sqlite3 "$DB_FILE" 2>&1 | tee import.log

# Check if import was successful
if [ ${PIPESTATUS[1]} -eq 0 ]; then
    echo ""
    echo "=============================================="
    echo "✓ Seed data imported successfully!"
    echo "=============================================="
    echo ""

    # Get final database size
    FINAL_SIZE=$(ls -lh "$DB_FILE" | awk '{print $5}')

    echo "Database file: $DB_FILE"
    echo "Initial size: $INITIAL_SIZE"
    echo "Final size: $FINAL_SIZE"
    echo ""

    # Show some statistics
    echo "Database Statistics:"
    echo "-------------------"
    sqlite3 "$DB_FILE" "SELECT 'Cancer Studies: ' || COUNT(*) FROM cancer_study;"
    sqlite3 "$DB_FILE" "SELECT 'Genes: ' || COUNT(*) FROM gene;"
    sqlite3 "$DB_FILE" "SELECT 'Reference Genomes: ' || COUNT(*) FROM reference_genome;"
    sqlite3 "$DB_FILE" "SELECT 'Cancer Types: ' || COUNT(*) FROM type_of_cancer;"
    echo ""

    echo "You can now use your database with seed data!"
    echo ""
    echo "To query your data:"
    echo "  sqlite3 $DB_FILE"
    echo "  sqlite> SELECT * FROM cancer_study LIMIT 5;"
    echo ""

    # Clean up converted file
    echo "Cleaning up temporary files..."
    rm -f "$CONVERTED_FILE"

else
    echo ""
    echo "=============================================="
    echo "✗ Error importing seed data"
    echo "=============================================="
    echo "Check import.log for details"
    echo "Converted file preserved at: $CONVERTED_FILE"
    exit 1
fi
