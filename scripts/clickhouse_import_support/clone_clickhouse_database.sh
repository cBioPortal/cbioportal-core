#!/usr/bin/env bash

# load dependencies
unset this_script_dir
this_script_dir="$(dirname "$(readlink -f $0)")"
if ! source "$this_script_dir/parse_property_file_functions.sh" ; then
    echo "Error : unable to load dependency : $this_script_dir/parse_property_file_functions.sh" >&2
    exit 1
fi
if ! source "$this_script_dir/clickhouse_client_command_line_functions.sh" ; then
    echo "Error : unable to load dependency : $this_script_dir/clickhouse_client_command_line_functions.sh" >&2
    exit 1
fi
unset this_script_dir

# other non-local environment variables in use
unset my_properties
unset database_table_list
unset source_database_name
unset destination_database_name
declare -A my_properties
declare -a database_table_list
database_table_list_filepath="$(pwd)/cmd_database_table_list.txt"
create_table_statement_filepath="$(pwd)/cmd_create_table_statement.txt"
create_table_result_filepath="$(pwd)/cmd_create_database_result.txt"
insert_table_data_result_filepath="$(pwd)/cmd_copy_table_data.txt"
record_count_comparison_filepath="$(pwd)/cmd_table_record_count.txt"

function usage() {
    echo "usage: clone_clickhouse_database.sh properties_filepath database_to_clone_tables_from database_to_clone_tables_to" >&2
    echo "         databases (from/to) must be in {blue, green}" >&2
    echo "         This tool is used to clone one clickhouse database to another clickhouse database on the same server for dual-database installations." >&2
}

function initialize_main() {
    local properties_filepath=$1
    local database_to_clone_tables_from=$2
    local database_to_clone_tables_to=$3
    if ! parse_property_file "$properties_filepath" my_properties ; then
        usage
        return 1
    fi
    if ! initialize_clickhouse_client_command_line_functions "$database_to_clone_tables_from" ; then
        usage
        return 1
    fi
    remove_credentials_from_properties my_properties # no longer needed - remove for security
    if [ "$database_to_clone_tables_from" == "blue" ] ; then
        source_database_name="${my_properties['clickhouse_blue_database_name']}"
    else
        if [ "$database_to_clone_tables_from" == "green" ] ; then
            source_database_name="${my_properties['clickhouse_green_database_name']}"
        else
            echo "Error : database_to_clone_tables_from must be one of {blue, green}" >&2
            usage
            return 1
        fi
    fi
    if [ "$database_to_clone_tables_to" == "blue" ] ; then
        destination_database_name="${my_properties['clickhouse_blue_database_name']}"
    else
        if [ "$database_to_clone_tables_to" == "green" ] ; then
            destination_database_name="${my_properties['clickhouse_green_database_name']}"
        else
            echo "Error : database_to_clone_tables_to must be one of {blue, green}" >&2
            usage
            return 1
        fi
    fi
    if [ "$database_to_clone_tables_to" == "$database_to_clone_tables_from" ] ; then
        echo "Error : database_to_clone_tables_to cannot be the same as database_to_clone_tables_from" >&2
        return 1
    fi
    return 0
}

function delete_output_stream_files() {
    rm -f "$database_table_list_filepath"
    rm -f "$create_table_statement_filepath"
    rm -f "$create_table_result_filepath"
    rm -f "$insert_table_data_result_filepath"
    rm -f "$record_count_comparison_filepath"
}

function shutdown_main_and_clean_up() {
    shutdown_clickhouse_client_command_line_functions
    delete_output_stream_files
    unset my_properties
    unset database_table_list
    unset source_database_name
    unset destination_database_name
    unset database_table_list_filepath
    unset create_table_statement_filepath
    unset create_table_result_filepath
    unset insert_table_data_result_filepath
    unset record_count_comparison_filepath
}

function destination_database_exists_and_is_empty() {
    if ! clickhouse_database_exists "$destination_database_name" ; then
        echo "Error : could not proceed with database cloning because destination database does not exist: $destination_database_name" >&2
        return 1
    fi
    if ! clickhouse_database_is_empty "$destination_database_name" ; then
        echo "Error : could not proceed with database cloning because destination database is not empty: $destination_database_name" >&2
        return 2
    fi
    return 0
}

function set_database_table_list() {
    local statement="SELECT name FROM system.tables WHERE database = '$source_database_name' ORDER BY name"
    rm -f "$database_table_list_filepath"
    if ! execute_sql_statement_via_clickhouse_client "$statement" "$database_table_list_filepath" ; then
        echo "Warning : failed to execute clickhouse statement : $statement" >&2
        return 1
    fi
    unset sql_data_array
    if ! set_clickhouse_sql_data_array_from_file "$database_table_list_filepath" 0 ; then
        return 1
    fi
    database_table_list=(${sql_data_array[@]})
    return 0
}

function create_destination_database_table_schema_only() {
    local table_name=$1
    local statement="SHOW CREATE TABLE \`$source_database_name\`.\`$table_name\`"
    rm -f "$create_table_statement_filepath"
    if ! execute_sql_statement_via_clickhouse_client "$statement" "$create_table_statement_filepath" ; then
        echo "Warning : failed to retrieve create table statement for $table_name" >&2
        return 1
    fi
    # replace source database name with destination database name in the CREATE TABLE statement
    local create_statement
    create_statement="$(tail -n +2 "$create_table_statement_filepath" | sed "s/\`$source_database_name\`/\`$destination_database_name\`/g")"
    rm -f "$create_table_result_filepath"
    if ! execute_sql_statement_via_clickhouse_client "$create_statement" "$create_table_result_filepath" ; then
        echo "Warning : failed to create table $table_name in destination database" >&2
        return 1
    fi
    return 0
}

function copy_source_database_table_data_to_destination() {
    local table_name=$1
    local statement="INSERT INTO \`$destination_database_name\`.\`$table_name\` SELECT * FROM \`$source_database_name\`.\`$table_name\`"
    rm -f "$insert_table_data_result_filepath"
    if ! execute_sql_statement_via_clickhouse_client "$statement" "$insert_table_data_result_filepath" ; then
        echo "Warning : failed to copy data from $table_name into destination database" >&2
        return 1
    fi
    return 0
}

function destination_table_matches_source_table() {
    local table_name=$1
    local source_count_statement="SELECT count(*) FROM \`$source_database_name\`.\`$table_name\`"
    local dest_count_statement="SELECT count(*) FROM \`$destination_database_name\`.\`$table_name\`"
    local source_count_filepath="$(pwd)/cmd_source_count.txt"
    local dest_count_filepath="$(pwd)/cmd_dest_count.txt"
    execute_sql_statement_via_clickhouse_client "$source_count_statement" "$source_count_filepath"
    execute_sql_statement_via_clickhouse_client "$dest_count_statement" "$dest_count_filepath"
    set_clickhouse_sql_data_array_from_file "$source_count_filepath" 0
    local source_count="${sql_data_array[0]}"
    set_clickhouse_sql_data_array_from_file "$dest_count_filepath" 0
    local dest_count="${sql_data_array[0]}"
    rm -f "$source_count_filepath" "$dest_count_filepath"
    if [ "$source_count" != "$dest_count" ] ; then
        echo "Error : when cloning data from table $table_name, source database and destination database tables contain different record counts ($source_count v. $dest_count)" >&2
        return 1
    fi
    return 0
}

function clone_all_source_database_tables_to_destination_database() {
    local pos=0
    local num_tables=${#database_table_list[@]}
    while [ "$pos" -lt "$num_tables" ] ; do
        local table_name="${database_table_list[$pos]}"
        echo "cloning $table_name"
        if ! create_destination_database_table_schema_only "$table_name" ; then
            echo "Error : could not create database table schema for $table_name in destination database" >&2
            return 1
        fi
        if ! copy_source_database_table_data_to_destination "$table_name" ; then
            echo "Error : could not copy data from table $table_name into destination database" >&2
            return 1
        fi
        if ! destination_table_matches_source_table "$table_name" ; then
            echo "Cloning operation canceled" >&2
            return 1
        fi
        pos=$(($pos+1))
    done
    return 0
}

function main() {
    local properties_filepath=$1
    local database_to_clone_tables_from=$2
    local database_to_clone_tables_to=$3
    local exit_status=0
    if ! initialize_main "$properties_filepath" "$database_to_clone_tables_from" "$database_to_clone_tables_to" ||
            ! destination_database_exists_and_is_empty ||
            ! set_database_table_list ||
            ! clone_all_source_database_tables_to_destination_database ; then
        exit_status=1
    fi
    shutdown_main_and_clean_up
    return $exit_status
}

main "$1" "$2" "$3"
