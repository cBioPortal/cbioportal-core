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
database_table_list_filepath="$(pwd)/ccd_database_table_list.txt"
create_table_result_filepath="$(pwd)/ccd_create_table_result.txt"
insert_table_data_result_filepath="$(pwd)/ccd_copy_table_data.txt"
record_count_comparison_filepath="$(pwd)/ccd_table_record_count.txt"
clickhouse_is_responsive_filepath="$(pwd)/ccd_clickhouse_is_responsive.txt"
SECONDS_BETWEEN_RESPONSIVENESS_RETRY=$((60))

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
    rm -f "$create_table_result_filepath"
    rm -f "$insert_table_data_result_filepath"
    rm -f "$record_count_comparison_filepath"
    rm -f "$clickhouse_is_responsive_filepath"
}

function shutdown_main_and_clean_up() {
    shutdown_clickhouse_client_command_line_functions
    delete_output_stream_files
    unset my_properties
    unset database_table_list
    unset source_database_name
    unset destination_database_name
    unset database_table_list_filepath
    unset create_table_result_filepath
    unset insert_table_data_result_filepath
    unset record_count_comparison_filepath
    unset clickhouse_is_responsive_filepath
    unset SECONDS_BETWEEN_RESPONSIVENESS_RETRY
}

function clickhouse_is_responding() {
    local remaining_try_count=3
    local statement="SELECT 1"
    while [ $remaining_try_count -ne 0 ] ; do
        if execute_sql_statement_via_clickhouse_client "$statement" "$clickhouse_is_responsive_filepath" ; then
            unset sql_data_array
            if set_clickhouse_sql_data_array_from_file "$clickhouse_is_responsive_filepath" 0 ; then
                local clickhouse_response=${sql_data_array[0]}
                if [ "$clickhouse_response" == "1" ] ; then
                    return 0
                fi
            fi
        fi
        remaining_try_count=$((remaining_try_count-1))
        if [ $remaining_try_count -gt 0 ] ; then
            sleep $SECONDS_BETWEEN_RESPONSIVENESS_RETRY
        fi
    done
    return 1
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
    local statement="SELECT name FROM system.tables WHERE database = '$source_database_name'"
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
    # CREATE TABLE ... AS source copies the schema only (no data)
    local statement="CREATE TABLE \`$destination_database_name\`.\`$table_name\` AS \`$source_database_name\`.\`$table_name\`"
    if ! execute_sql_statement_via_clickhouse_client "$statement" "$create_table_result_filepath" ; then
        return 1
    fi
    return 0
}

function copy_source_database_table_data_to_destination() {
    local table_name=$1
    local statement="INSERT INTO \`$destination_database_name\`.\`$table_name\` SELECT * FROM \`$source_database_name\`.\`$table_name\`"
    if ! execute_sql_statement_via_clickhouse_client "$statement" "$insert_table_data_result_filepath" ; then
        return 1
    fi
    return 0
}

function destination_table_matches_source_table() {
    local table_name=$1
    # SETTINGS select_sequential_consistency = 1 ensures that count(*) sees all
    # inserted rows even on ClickHouse Cloud (SharedMergeTree / S3-backed), where
    # reads are eventually consistent and a count(*) immediately after INSERT may
    # otherwise return a stale result.
    local statement="SELECT (SELECT count(*) FROM \`$destination_database_name\`.\`$table_name\`) = (SELECT count(*) FROM \`$source_database_name\`.\`$table_name\`), (SELECT count(*) FROM \`$source_database_name\`.\`$table_name\`), (SELECT count(*) FROM \`$destination_database_name\`.\`$table_name\`) SETTINGS select_sequential_consistency = 1"
    if ! execute_sql_statement_via_clickhouse_client "$statement" "$record_count_comparison_filepath" ; then
        echo "Warning : failed to execute clickhouse statement : $statement" >&2
        return 1
    fi
    unset sql_data_array
    set_clickhouse_sql_data_array_from_file "$record_count_comparison_filepath" 0
    if [[ "${sql_data_array[0]}" -ne 1 ]] ; then
        local source_count
        local destination_count
        set_clickhouse_sql_data_array_from_file "$record_count_comparison_filepath" 1
        source_count="${sql_data_array[0]}"
        set_clickhouse_sql_data_array_from_file "$record_count_comparison_filepath" 2
        destination_count="${sql_data_array[0]}"
        echo "Error : when cloning data from table $table_name, source database and destination database tables contain different record counts (source: $source_count, destination: $destination_count)" >&2
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
        if [ "$table_name" != "cbioportal_sequence_state" ] ; then
            if ! destination_table_matches_source_table "$table_name" ; then
                echo "Cloning operation canceled" >&2
                return 1
            fi
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
            ! clickhouse_is_responding ||
            ! destination_database_exists_and_is_empty ||
            ! set_database_table_list ||
            ! clone_all_source_database_tables_to_destination_database ; then
        exit_status=1
    fi
    shutdown_main_and_clean_up
    return $exit_status
}

main "$1" "$2" "$3"
