# cBioPortal Import Process Database Management Tools
These tools support a blue-green deployment approach to cBioPortal database updates.
This strategy was introduced to support the introduction of a coupled ClickHouse database
which will be used in conjunction with the prior MySQL database in order to improve the
runtime performance of the cBioPortal study view page.

Import of cancer studies is now directed into a not-in-production copy of the production
MySQL database using the existing import codebase. The newly populated MySQL database is
used as a datasource for populating a not-in-production ClickHouse database. Using this
approach, the production databases remain consistent because no changes occur to either
database during import operations. Once the ClickHouse database has been fully populated
and related derived tables and persistent views have been created in ClickHouse, the
cBioPortal web server backend can be switched over quickly to use the newly populated
database and make the newly imported cancer studies availabile in production.

## clone\_mysql\_database.sh
This bash script uses the *mysql* command line interface tool to make a complete copy
of the current production database into a separate database on the same MySQL server.
This will occur to initialize the not-in-production database and prepare it for cancer
study import.

## drop\_tables\_in\_mysql\_database.sh
This bash script uses the *mysql* command line interface tool to drop all tables which
exist in a mysql database. This will occur at the end of an import process in order to
clear the data from the prior production database (or the backup copy database) in order
to make the database empty and available for reuse during the next cycle of cancer study
import.

## copy\_mysql\_database\_tables\_to\_clickhouse.sh
This bash script uses the *sling* command line interface tool to copy data from all tables
present in the selected mysql database (green or blue) into the corresponding sling
database. Multiple retries are attempted on individual attempt failures. Copy results are
validated by record counts.
