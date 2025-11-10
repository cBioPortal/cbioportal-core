#!/usr/bin/env bash

# load dependencies
unset this_script_dir
this_script_dir="$(dirname "$(readlink -f $0)")"
if ! source "$this_script_dir/parse_property_file_functions.sh" ; then
    echo "Error : unable to load dependency : $this_script_dir/parse_property_file_functions.sh" >&2
    exit 1
fi
if ! source "$this_script_dir/mysql_command_line_functions.sh" ; then
    echo "Error : unable to load dependency : $this_script_dir/mysql_command_line_functions.sh" >&2
    exit 1
fi
unset this_script_dir

# other non-local environment variables in use
unset my_properties
unset update_management_database_name
update_management_database_name=""
declare -A my_properties
record_count_filepath="$(pwd)/update_status_record_count.txt"
update_status_filepath="$(pwd)/update_status.txt"

function usage() {
    echo "usage: set_update_process_state.sh properties_filepath state" >&2
    echo "         state must be in {running, complete, abandoned}" >&2
}

function initialize_main() {
    local properties_filepath=$1
    local state=$2
    if ! parse_property_file "$properties_filepath" my_properties ; then
        usage
        return 1
    fi
    if ! initialize_mysql_command_line_functions ; then
        usage
        return 1
    fi
    update_management_database_name="${my_properties['mysql_update_management_database']}"
    if ! [ "$state" == "running" ] && ! [ "$state" == "complete" ] && ! [ "$state" == "abandoned" ] ; then
        echo "Error : state must be one of {running, complete, abandoned}" >&2
        usage
        return 1
    fi
    return 0
}

function delete_output_stream_files() {
    rm -f "$record_count_filepath"
    rm -f "$update_status_filepath"
}

function shutdown_main_and_clean_up() {
    shutdown_mysql_command_line_functions
    delete_output_stream_files
    unset my_properties
    unset record_count_filepath
    unset update_status_filepath
}

function process_state_table_is_valid() {
    if ! database_exists "$update_management_database_name" ; then
        echo "Error : could not proceed with setting update status because database does not exist: $update_management_database_name" >&2
        return 1
    fi
    if ! table_exists "$update_management_database_name" "update_status" ; then
        echo "Error : could not proceed with setting update status because table 'update_status' does not exist in database : $update_management_database_name" >&2
    fi
    local get_record_count_statement="SELECT count(*) AS record_count from \`$update_management_database_name\`.update_status;"
    if ! execute_sql_statement_via_mysql "$get_record_count_statement" "$record_count_filepath" ; then
        echo "Error : could not validate process_state table. Mysql statement failed to execute properly : $get_record_count_statement" >&2
        return 1
    fi
    set_mysql_sql_data_array_from_file "$record_count_filepath" 0
    local rowcount="${sql_data_array[0]}"
    if [[ "$rowcount" -ne 1 ]] ; then
        echo "Error : database $update_management_database_name contains $rowcount rows instead of exactly 1 row as expected." >&2
        return 1
    fi
    return 0
}

function set_state_in_status_table() {
    local state=$1
    local update_status_statement=""
    if [ "$state" == "running" ] ; then
        update_status_statement="UPDATE \`$update_management_database_name\`.update_status SET update_process_status = 'running' WHERE update_process_status = 'complete'; SELECT ROW_COUNT()"
    else
        if [ "$state" == "complete" ] ; then
            update_status_statement="UPDATE \`$update_management_database_name\`.update_status SET update_process_status = 'complete', time_of_last_update_process_completion = NOW(), current_database_in_production = IF(current_database_in_production = 'blue', 'green', 'blue') WHERE update_process_status = 'running'; SELECT ROW_COUNT()"
        else
            # handle abandoned attempts to import (keep previous color and timestamp but return to state "completed")
            update_status_statement="UPDATE \`$update_management_database_name\`.update_status SET update_process_status = 'complete' WHERE update_process_status = 'running'; SELECT ROW_COUNT()"
        fi
    fi
    if ! execute_sql_statement_via_mysql "$update_status_statement" "$update_status_filepath" ; then
        echo "Error : failed to execute SQL statement \"$update_status_statement\"" >&2
        return 1
    fi
    set_mysql_sql_data_array_from_file "$update_status_filepath" 0
    local rowcount="${sql_data_array[0]}"
    if [ "$rowcount" -eq 1 ] ; then
        echo "Status table updated"
        return 0
    fi
    if [ "$rowcount" -eq 0 ] ; then
        if [ "$state" == "running" ] ; then
            echo "Error : cannot set process status to running because it appears to already be in a running status. You should determine which database is actually in production currently, and manually correct the update_status table in database $update_management_database_name" >&2
            return 1
        else
            echo "Warning : cannot set process status to complete because it appears to already be complete. You should determine which database is actually in production currently, and validate the update_status table in database $update_management_database_name" >&2
            return 0
        fi
    fi
    if [ "$rowcount" -eq -1 ] ; then
        echo "Error : an unexpected error occurred trying to execute this mysql statement : $update_status_statement" >&2
        return 1
    fi
    return 0
}
 
function main() {
    local properties_filepath=$1
    local state=$2
    local exit_status=0
    if ! initialize_main "$properties_filepath" "$state" ||
            ! process_state_table_is_valid ||
            ! set_state_in_status_table "$state" ; then
        exit_status=1
    fi
    shutdown_main_and_clean_up
    return $exit_status
}

main "$1" "$2"
