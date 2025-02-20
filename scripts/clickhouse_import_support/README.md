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

## create\_derived\_tables\_in\_clickhouse\_database.sh
This bash script uses the *clickhouse* command line interface tool to generate derived 
tables in clickhouse from the newly copied tables in clickhouse. It takes in an ordered
list of SQL files, splits them into a set of files that each have one SQL statement.
It then iterates through the SQL statements sequentially. For most statements, it uses
the *clickhouse* command line interface tool to run the SQL statement. If it finds an
insert statement into either the *genetic_alteration_derived* or *generic_assay_data_derived*
tables, it executes the *create_derived_tables_in_clickhouse_database_by_profile.py* script
instead of executing the SQL statements directly.

## create\_derived\_tables\_in\_clickhouse\_database\_by\_profile.py
This python 3 script uses the *clickhouse* command line interface tool to modify two 
SQL insert statements so that instead of running for all genetic profiles at once,
the queries are run once per genetic profile. This is done to reduce memory usage and
also so that if there is an error for one genetic profile, it doesn't prevent all following
genetic profiles from being handled. The two insert statements are for the
*genetic_alteration_derived* and *generic_assay_data_derived* tables.

## synchronize\_user\_tables\_between\_databases.sh
This bash script uses both the *mysql* and *clickhouse* command line interface tools
to update both mysql and clickhouse databases with any users that have been put into 
the mysql database that they were cloned from. If the 'green' databases
have been cloned from the 'blue' databases, and now the 'blue' mysql database contains 
users not in the 'green' databases, this script can copy any new users in the 'blue'
mysql database to both the 'green' mysql database and the 'green' clickhouse database. 

## get\_database\_currently\_in\_production.sh
This bash script uses the *mysql* command line interface to get the current production database
from the management database, either 'green' or 'blue'.

## set\_update\_process\_state.sh
This bash script uses the *mysql* command line interface to set the management database
state to either 'running' or 'complete'. The script takes in the following options: 
'running', 'complete', or 'abandoned'. The status can only be set to 'running' if it is 
currently 'complete'.  If the script is passed 'complete' and the status is currently
'running' the *time_of_last_update_process_completion* is set to the current timestamp
and the *current_database_in_production* is switched either from blue -> green or from
green -> blue. If the script is passed 'abandoned', and the current status is 'running'
the *time_of_last_update_process_completion* and *current_database_in_production* 
are unchanged but the status is set to 'complete'.

## drop\_tables\_in\_clickhouse\_database.sh
This bash script uses the *clickhouse* command line interface tool to drop all tables which
exist in a clickhouse database. This will occur at the end of an import process in order to
clear the data from the prior production database (or the backup copy database) in order
to make the database empty and available for reuse during the next cycle of cancer study
import.

## Libraries:
* *mysql_command_line_functions.sh* contains functions for interacting with the *mysql* command
line interface.
* *sling_command_line_functions.sh* contains functions for interacting with the *sling* command
line interface.
* *clickhouse_client_command_line_functions.sh* contains functions for interacting with the
*clickhouse* command line interface.
* *parse_property_file_functions.sh* contains functions for parsing a *\*.properties* file.
