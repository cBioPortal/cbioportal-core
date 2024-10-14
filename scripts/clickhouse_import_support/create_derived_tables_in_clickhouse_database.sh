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

# non-local environment variables in use
unset my_properties
unset database_name
declare -A my_properties
database_name=""
create_derived_table_result_filepath="$(pwd)/cdtcd_create_derived_tables_result.txt"
clickhouse_is_responsive_filepath="$(pwd)/cdtcd_cmd_clickhouse_is_responsive.txt"
SECONDS_BETWEEN_RESPONSIVENESS_RETRY=$((60))

function usage() {
    echo "usage: create_derived_tables_in_clickhouse_database.sh properties_filepath database" >&2
    echo "         database must be in {blue, green}" >&2
}

function initialize_main() {
    local properties_filepath=$1
    local database_to_create_derived_tables_in=$2
    if ! parse_property_file "$properties_filepath" my_properties ; then
        usage
        return 1
    fi
    if ! initialize_clickhouse_client_command_line_functions "$database_to_create_derived_tables_in" ; then
        usage
        return 1
    fi
    remove_credentials_from_properties my_properties # no longer needed - remove for security
    if [ "$database_to_create_derived_tables_in" == "blue" ] ; then
        database_name="${my_properties['clickhouse_blue_database_name']}"
    else
        if [ "$database_to_create_derived_tables_in" == "green" ] ; then
            database_name="${my_properties['clickhouse_green_database_name']}"
        else
            echo "Error : database must be one of {blue, green}" >&2
            usage
            return 1
        fi
    fi
    return 0
}

function clickhouse_is_responding() {
    local remaining_try_count=3
    local statement="SELECT 1"
    while [ $remaining_try_count -ne 0 ] ; do
        if execute_sql_statement_via_clickhouse_client "$statement" "$clickhouse_is_responsive_filepath" ; then
            unset sql_data_array
            if set_sql_data_array_from_file "$clickhouse_is_responsive_filepath" 0 ; then
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

function selected_database_exists() {
    if ! clickhouse_database_exists "$database_name" ; then
        echo "Error : could not proceed with creation of derived tables because database does not exist: $database_name" >&2
        return 1
    fi
    return 0
}

function create_all_derived_tables() {
    local return_status=0
    local sql_filename_list_filepath="${my_properties['clickhouse_derived_table_construction_filenames_list_file']}"
    if ! [ -r "$sql_filename_list_filepath" ] ; then
        echo "Error : unable to read sql filename list from file \"$sql_filename_list_filepath\"" >&2
        return 1
    fi
    while read line ; do
        echo -n "."
        if ! execute_sql_statement_from_file_via_clickhouse_client "$line" "create_derived_table_result_filepath" ; then
            echo "Warning : failure occurred during execution of sql statements in file $line" >&2
            return_status=1
        fi
    done < "$sql_filename_list_filepath"
    echo
    return $return_status
}

function delete_output_stream_files() {
    rm -f "$create_derived_table_result_filepath"
    rm -f "$clickhouse_is_responsive_filepath"
}

function shutdown_main_and_clean_up() {
    shutdown_clickhouse_client_command_line_functions
    delete_output_stream_files
    unset my_properties
    unset database_name
    unset create_derived_table_result_filepath
    unset clickhouse_is_responsive_filepath
    unset SECONDS_BETWEEN_RESPONSIVENESS_RETRY
}

function main() {
    local properties_filepath=$1
    local database_to_create_derived_tables_in=$2
    local exit_status=0
    if ! initialize_main "$properties_filepath" "$database_to_create_derived_tables_in" ||
            ! clickhouse_is_responding ||
            ! selected_database_exists ||
            ! create_all_derived_tables ; then
        exit_status=1
    fi
    shutdown_main_and_clean_up
    return $exit_status
}

main "$1" "$2"
