# cBioPortal SQLite Migration Guide

This document describes how to use the SQLite version of cBioPortal Core.

## Overview

The cBioPortal Core application has been modified to support SQLite as an alternative to MySQL. This allows you to run the application without requiring an external MySQL server.

## Quick Start

### 1. Import the Schema

```bash
# Run the import script (creates cbioportal.db)
./import_schema.sh

# Or specify a custom database file name
./import_schema.sh my_custom.db
```

### 2. Update Configuration (if needed)

The `application.properties` file is already configured for SQLite by default:

```properties
spring.datasource.url=jdbc:sqlite:./cbioportal.db
spring.datasource.username=
spring.datasource.password=
spring.datasource.driver-class-name=org.sqlite.JDBC
spring.jpa.database-platform=org.sqlite.hibernate.dialect.SQLiteDialect
```

### 3. Build and Run

```bash
# Build the project
mvn clean package

# Run your application
java -jar target/core-1.0.15.jar
```

## Manual Schema Import

If you prefer to import manually:

```bash
# Create the database from the SQLite schema
sqlite3 cbioportal.db < cgds_sqlite.sql

# Verify the import
sqlite3 cbioportal.db
sqlite> .tables
sqlite> SELECT * FROM info;
sqlite> .quit
```

## Schema Conversions

The SQLite schema (`cgds_sqlite.sql`) was converted from the MySQL schema with the following changes:

### Data Types
- `AUTO_INCREMENT` → `AUTOINCREMENT`
- `int(11)`, `bigint(20)` → `INTEGER`
- `tinyint(1)` → `INTEGER` (for booleans)
- `float`, `double` → `REAL`
- `mediumtext`, `longtext` → `TEXT`
- `datetime` → `datetime` (stored as TEXT in SQLite)

### SQL Syntax
- `ENUM('value1', 'value2')` → `TEXT CHECK(column IN ('value1', 'value2'))`
- `JSON` → `TEXT` (SQLite has JSON functions but stores as TEXT)
- Removed MySQL-specific: `ENGINE=InnoDB`, `CHARSET`, `COLLATE`, `COMMENT`
- `KEY` → `INDEX` (more standard, though both work)

### Constraints
- `ON DUPLICATE KEY UPDATE` → `ON CONFLICT ... DO UPDATE` (UPSERT syntax)
- Foreign key constraints preserved
- Unique constraints preserved
- Primary key constraints preserved

## Using the Java Importer

You can also use the Java-based SQL importer:

```bash
# Compile the project first
mvn clean package

# Run the importer
java -cp target/core-1.0.15.jar org.mskcc.cbio.portal.util.SQLImporter cgds_sqlite.sql
```

The Java importer automatically handles basic MySQL-to-SQLite conversions.

## Database Operations

### Viewing Your Data

```bash
sqlite3 cbioportal.db
```

Useful commands:
```sql
-- List all tables
.tables

-- Show table structure
.schema cancer_study

-- Query data
SELECT * FROM info;
SELECT COUNT(*) FROM cancer_study;

-- Enable column headers and better formatting
.headers on
.mode column

-- Export to CSV
.mode csv
.output data.csv
SELECT * FROM cancer_study;
.output stdout

-- Exit
.quit
```

### Backup and Restore

```bash
# Backup
sqlite3 cbioportal.db ".backup backup.db"

# Or using sqlite3 dump
sqlite3 cbioportal.db .dump > backup.sql

# Restore from dump
sqlite3 cbioportal_restored.db < backup.sql
```

## Code Changes Summary

### 1. Dependencies (pom.xml)
- Added `org.xerial:sqlite-jdbc:3.44.1.0`
- Added `com.github.gwenn:sqlite-dialect:0.1.4`
- Kept MySQL driver for backward compatibility

### 2. Database Configuration (application.properties)
- Changed to SQLite by default
- MySQL configuration preserved but commented out

### 3. DAO Layer Updates
- **JdbcUtil.java**: Foreign key syntax detection (MySQL vs SQLite)
- **JdbcDataSource.java**: Conditional MySQL-specific properties
- **DaoSampleProfile.java**: UPSERT syntax detection
- **BulkLoader.java**: New database-agnostic wrapper
- **SQLiteBulkLoader.java**: New bulk loader using JDBC batch inserts

### 4. Bulk Loading
- **MySQL**: Uses `LOAD DATA LOCAL INFILE` (file-based)
- **SQLite**: Uses JDBC batch inserts (1000 records per batch)

## Switching Between MySQL and SQLite

To switch back to MySQL, edit `application.properties`:

```properties
# Comment out SQLite configuration
#spring.datasource.url=jdbc:sqlite:./cbioportal.db
#spring.datasource.username=
#spring.datasource.password=
#spring.datasource.driver-class-name=org.sqlite.JDBC
#spring.jpa.database-platform=org.sqlite.hibernate.dialect.SQLiteDialect

# Uncomment MySQL configuration
spring.datasource.url=jdbc:mysql://localhost:3306/cgds_gdac?useSSL=false&allowPublicKeyRetrieval=true&allowLoadLocalInfile=true
spring.datasource.username=cbio_user
spring.datasource.password=cbio_pass
spring.datasource.driver-class-name=com.mysql.cj.jdbc.Driver
spring.jpa.database-platform=org.hibernate.dialect.MySQL5InnoDBDialect
```

Then rebuild:
```bash
mvn clean package
```

## Performance Notes

### SQLite
- **Pros**: No external server, zero configuration, single file, portable
- **Cons**: Single-writer limitation, no separate server process
- **Best for**: Development, testing, small to medium datasets, single-user scenarios

### MySQL
- **Pros**: Multi-user, better concurrency, production-grade
- **Cons**: Requires external server, more configuration
- **Best for**: Production, multi-user environments, large datasets

## Limitations

1. **Concurrent Writes**: SQLite supports multiple readers but only one writer at a time
2. **Network Access**: SQLite is file-based; for remote access, use MySQL
3. **JSON Support**: SQLite stores JSON as TEXT (functions available, but not native type)
4. **Bulk Loading**: SQLite uses batch inserts instead of file-based loading (slightly slower)

## Troubleshooting

### "table already exists" error
```bash
# Delete existing database and reimport
rm cbioportal.db
./import_schema.sh
```

### "database is locked" error
- Another process is accessing the database
- Close all connections and try again

### Foreign key constraint errors
```bash
# Check if foreign keys are enabled
sqlite3 cbioportal.db "PRAGMA foreign_keys;"

# Should return: 1 (enabled)
# If not, it's enabled by default in the schema
```

### Checking schema version
```bash
sqlite3 cbioportal.db "SELECT * FROM info;"
```

Should show version: `2.14.5`

## Additional Resources

- [SQLite Documentation](https://www.sqlite.org/docs.html)
- [SQLite Data Types](https://www.sqlite.org/datatype3.html)
- [SQLite UPSERT](https://www.sqlite.org/lang_UPSERT.html)
- [cBioPortal Documentation](https://docs.cbioportal.org/)

## Support

For issues related to:
- **SQLite migration**: Check this document and SQLITE_MIGRATION.md
- **cBioPortal functionality**: See cBioPortal documentation
- **Original MySQL schema**: See ../portal/src/main/resources/db-scripts/cgds.sql
