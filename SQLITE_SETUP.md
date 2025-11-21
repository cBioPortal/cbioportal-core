# Quick Start: Using SQLite with cBioPortal Core

## TL;DR

```bash
# 1. Import the schema
./import_schema.sh

# 2. (Optional) Import seed data with 42K+ genes and reference data
./import_seed_data.sh

# 3. Build the project
mvn clean package

# 4. Run your application - it's already configured for SQLite!
java -jar target/core-1.0.15.jar
```

Your database file `cbioportal.db` will be created in the current directory.

---

## What Changed?

This cBioPortal Core project now supports **SQLite** in addition to MySQL. The application automatically detects which database you're using and adjusts accordingly.

## Files Created/Modified

### New Files
- **`cgds_sqlite.sql`** - SQLite-compatible schema (converted from MySQL)
- **`import_schema.sh`** - Easy schema import script
- **`SQLITE_MIGRATION.md`** - Detailed migration documentation
- **`SQLITE_SETUP.md`** - This quick start guide
- **`src/main/java/org/mskcc/cbio/portal/dao/SQLiteBulkLoader.java`** - SQLite bulk insert implementation
- **`src/main/java/org/mskcc/cbio/portal/dao/BulkLoader.java`** - Database-agnostic bulk loader wrapper
- **`src/main/java/org/mskcc/cbio/portal/util/SQLImporter.java`** - Java-based SQL import utility

### Modified Files
- **`pom.xml`** - Added SQLite dependencies
- **`application.properties`** - Changed to SQLite by default (MySQL config preserved but commented)
- **`src/main/java/org/mskcc/cbio/portal/dao/JdbcDataSource.java`** - Database-specific connection properties
- **`src/main/java/org/mskcc/cbio/portal/dao/JdbcUtil.java`** - Foreign key constraint handling for both databases
- **`src/main/java/org/mskcc/cbio/portal/dao/DaoSampleProfile.java`** - UPSERT syntax for both databases

## Database Setup

### Step 1: Import Schema (Required)

#### Option A: Using the Import Script (Recommended)

```bash
./import_schema.sh
```

This will create `cbioportal.db` with all 51 tables and proper schema version (2.14.5).

#### Option B: Manual Import

```bash
sqlite3 cbioportal.db < cgds_sqlite.sql
```

#### Option C: Custom Database Location

```bash
# Create database at a custom location
./import_schema.sh /path/to/my/database.db

# Update application.properties to match
spring.datasource.url=jdbc:sqlite:/path/to/my/database.db
```

### Step 2: Import Seed Data (Optional but Recommended)

The seed data provides essential reference information:

```bash
./import_seed_data.sh
```

**What's included:**
- 42,306 genes (human and mouse)
- 852 cancer types
- 34,837 gene sets for pathway analysis
- 3 reference genomes (hg19, hg38, mm10)

**Database size:** 464 KB (empty) → 139 MB (with seed data)

See [SEED_DATA_README.md](SEED_DATA_README.md) for details.

**Note:** The seed data import script will:
1. Download the seed file if needed (50MB compressed, 192MB uncompressed)
2. Convert it from MySQL to SQLite format
3. Import ~138MB of reference data
4. Take several minutes to complete

## Verifying Your Setup

```bash
# Check that the database was created
ls -lh cbioportal.db

# View tables
sqlite3 cbioportal.db ".tables"

# Check schema version
sqlite3 cbioportal.db "SELECT * FROM info;"
# Should show: 2.14.5||1.0.2|

# View a table structure
sqlite3 cbioportal.db ".schema cancer_study"
```

## Configuration

### Current Configuration (SQLite - Default)

`application.properties`:
```properties
spring.datasource.url=jdbc:sqlite:./cbioportal.db
spring.datasource.username=
spring.datasource.password=
spring.datasource.driver-class-name=org.sqlite.JDBC
spring.jpa.database-platform=org.sqlite.hibernate.dialect.SQLiteDialect
```

### To Switch Back to MySQL

Edit `application.properties`:
```properties
# Comment out SQLite
#spring.datasource.url=jdbc:sqlite:./cbioportal.db
#spring.datasource.username=
#spring.datasource.password=
#spring.datasource.driver-class-name=org.sqlite.JDBC
#spring.jpa.database-platform=org.sqlite.hibernate.dialect.SQLiteDialect

# Uncomment MySQL
spring.datasource.url=jdbc:mysql://localhost:3306/cgds_gdac?useSSL=false&allowPublicKeyRetrieval=true&allowLoadLocalInfile=true
spring.datasource.username=cbio_user
spring.datasource.password=cbio_pass
spring.datasource.driver-class-name=com.mysql.cj.jdbc.Driver
spring.jpa.database-platform=org.hibernate.dialect.MySQL5InnoDBDialect
```

Then rebuild: `mvn clean package`

## Key Differences

| Feature | MySQL | SQLite |
|---------|-------|--------|
| Setup | Requires server | Single file |
| Configuration | Username/password | No authentication |
| Concurrency | Multi-user | Single writer |
| Bulk Loading | File-based (LOAD DATA) | Batch inserts |
| Foreign Keys | Always enforced | Enabled via connection property |
| Use Case | Production, multi-user | Development, testing, single-user |

## How It Works

The application **automatically detects** which database you're using:

1. **Connection Detection**: Checks the JDBC URL
   - `jdbc:sqlite:...` → Use SQLite mode
   - `jdbc:mysql:...` → Use MySQL mode

2. **Automatic Adaptations**:
   - Foreign key constraints: `PRAGMA foreign_keys` (SQLite) vs `SET FOREIGN_KEY_CHECKS` (MySQL)
   - UPSERT syntax: `ON CONFLICT` (SQLite) vs `ON DUPLICATE KEY UPDATE` (MySQL)
   - Bulk loading: Batch inserts (SQLite) vs `LOAD DATA LOCAL INFILE` (MySQL)

3. **Code-Level Support**:
   - `BulkLoader` class automatically delegates to the right implementation
   - DAO classes detect database type and adjust SQL syntax

## Common Operations

### View Your Data

```bash
sqlite3 cbioportal.db

sqlite> .headers on
sqlite> .mode column
sqlite> SELECT * FROM info;
sqlite> .quit
```

### Backup

```bash
# Method 1: SQLite backup command
sqlite3 cbioportal.db ".backup backup.db"

# Method 2: SQL dump
sqlite3 cbioportal.db .dump > backup.sql

# Method 3: Simple file copy (must not be in use)
cp cbioportal.db backup.db
```

### Restore

```bash
# From backup file
cp backup.db cbioportal.db

# From SQL dump
sqlite3 cbioportal.db < backup.sql
```

### Reset Database

```bash
rm cbioportal.db
./import_schema.sh
```

## Troubleshooting

### "database is locked"
- Another process has the database open
- SQLite only allows one writer at a time
- Close other connections and retry

### "no such table"
- Schema not imported
- Run `./import_schema.sh`

### Foreign key constraint errors
- Foreign keys should be automatically enabled
- Verify: `sqlite3 cbioportal.db "PRAGMA foreign_keys;"`
- Should return: `1`

### "Unable to find schema file"
- Run the import script from the `cbioportal-core` directory
- Or specify full path: `sqlite3 cbioportal.db < /full/path/to/cgds_sqlite.sql`

## Performance Tips

1. **Use Transactions**: Wrap bulk operations in transactions
   ```sql
   BEGIN TRANSACTION;
   -- your inserts here
   COMMIT;
   ```

2. **Disable Sync for Bulk Loads** (development only):
   ```sql
   PRAGMA synchronous = OFF;
   -- perform bulk operations
   PRAGMA synchronous = NORMAL;
   ```

3. **Optimize Queries**: SQLite is very fast for reads
   - Create indexes on frequently queried columns
   - Use EXPLAIN QUERY PLAN to optimize

4. **Regular VACUUM**:
   ```bash
   sqlite3 cbioportal.db "VACUUM;"
   ```

## Further Reading

- **Full Migration Guide**: See [SQLITE_MIGRATION.md](SQLITE_MIGRATION.md)
- **SQLite Documentation**: https://www.sqlite.org/docs.html
- **cBioPortal Docs**: https://docs.cbioportal.org/

## Need Help?

Check these resources:
1. This guide (quick start)
2. [SQLITE_MIGRATION.md](SQLITE_MIGRATION.md) (detailed migration info)
3. Original MySQL schema: `../portal/src/main/resources/db-scripts/cgds.sql`
4. cBioPortal documentation

---

**You're all set!** The application is configured for SQLite and ready to use. Just run `./import_schema.sh` and `mvn clean package`.
