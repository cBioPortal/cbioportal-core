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
unset update_management_database_name
update_management_database_name=""
declare -A my_properties
record_count_filepath="$(pwd)/update_status_record_count.txt"
update_status_filepath="$(pwd)/update_status.txt"

function usage() {
    echo "usage: set_update_process_state_clickhouse.sh properties_filepath state" >&2
    echo "         state must be in {running, complete, abandoned}" >&2
}

function write_clickhouse_config_file_for_management_database() {
    configured_clickhouse_config_file_path="$(pwd)/clickhouse_client_config_$(date "+%Y-%m-%d-%H-%M-%S").yaml"
    if ! rm -f "$configured_clickhouse_config_file_path" || ! touch "$configured_clickhouse_config_file_path" ; then
        echo "Error : unable to create clickhouse_client_config file $configured_clickhouse_config_file_path" >&2
        return 1
    fi
    chmod 600 "$configured_clickhouse_config_file_path"
    echo "user: ${my_properties['clickhouse_server_username']}" >> "$configured_clickhouse_config_file_path"
    echo "password: ${my_properties['clickhouse_server_password']}" >> "$configured_clickhouse_config_file_path"
    echo "host: ${my_properties['clickhouse_server_host_name']}" >> "$configured_clickhouse_config_file_path"
    echo "port: ${my_properties['clickhouse_server_port']}" >> "$configured_clickhouse_config_file_path"
    echo "database: $update_management_database_name" >> "$configured_clickhouse_config_file_path"
    if ! [ "$(cat $configured_clickhouse_config_file_path | wc -l)" == "5" ] ; then
        echo "Error : could not successfully write clickhouse_client config properties to file $configured_clickhouse_config_file_path" >&2
        return 1
    fi
    return 0
}

function initialize_main() {
    local properties_filepath=$1
    local state=$2
    if ! parse_property_file "$properties_filepath" my_properties ; then
        usage
        return 1
    fi
    update_management_database_name="${my_properties['clickhouse_update_management_database']}"
    if [ -z "$update_management_database_name" ] ; then
        echo "Error : property 'clickhouse_update_management_database' is not set in $properties_filepath" >&2
        usage
        return 1
    fi
    remove_credentials_from_properties my_properties # no longer needed - remove for security
    if ! write_clickhouse_config_file_for_management_database ; then
        usage
        return 1
    fi
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
    rm -f "$configured_clickhouse_config_file_path"
    delete_output_stream_files
    unset my_properties
    unset update_management_database_name
    unset configured_clickhouse_config_file_path
    unset record_count_filepath
    unset update_status_filepath
}

function process_state_table_is_valid() {
    if ! clickhouse_database_exists "$update_management_database_name" ; then
        echo "Error : could not proceed with setting update status because database does not exist: $update_management_database_name" >&2
        return 1
    fi
    local table_exists_statement="SELECT COUNT(*) FROM system.tables WHERE database = '$update_management_database_name' AND name = 'update_status'"
    if ! execute_sql_statement_via_clickhouse_client "$table_exists_statement" "$record_count_filepath" ; then
        echo "Error : could not check for table 'update_status' in database : $update_management_database_name" >&2
        return 1
    fi
    set_clickhouse_sql_data_array_from_file "$record_count_filepath" 0
    if [[ "${sql_data_array[0]}" -ne 1 ]] ; then
        echo "Error : could not proceed with setting update status because table 'update_status' does not exist in database : $update_management_database_name" >&2
        return 1
    fi
    local get_record_count_statement="SELECT count(*) FROM \`$update_management_database_name\`.update_status"
    if ! execute_sql_statement_via_clickhouse_client "$get_record_count_statement" "$record_count_filepath" ; then
        echo "Error : could not validate process_state table. ClickHouse statement failed to execute properly : $get_record_count_statement" >&2
        return 1
    fi
    set_clickhouse_sql_data_array_from_file "$record_count_filepath" 0
    local rowcount="${sql_data_array[0]}"
    if [[ "$rowcount" -ne 1 ]] ; then
        echo "Error : database $update_management_database_name contains $rowcount rows instead of exactly 1 row as expected." >&2
        return 1
    fi
    return 0
}

function get_current_process_status() {
    local get_status_statement="SELECT update_process_status FROM \`$update_management_database_name\`.update_status"
    if ! execute_sql_statement_via_clickhouse_client "$get_status_statement" "$update_status_filepath" ; then
        echo "Error : failed to read current update_process_status from database $update_management_database_name" >&2
        return 1
    fi
    set_clickhouse_sql_data_array_from_file "$update_status_filepath" 0
    echo "${sql_data_array[0]}"
}

function set_state_in_status_table() {
    local state=$1
    local current_status
    current_status="$(get_current_process_status)"
    if [ $? -ne 0 ] ; then
        return 1
    fi
    local update_statement=""
    if [ "$state" == "running" ] ; then
        if [ "$current_status" != "complete" ] ; then
            echo "Error : cannot set process status to running because it appears to already be in a running status. You should determine which database is actually in production currently, and manually correct the update_status table in database $update_management_database_name" >&2
            return 1
        fi
        update_statement="ALTER TABLE \`$update_management_database_name\`.update_status UPDATE update_process_status = 'running' WHERE update_process_status = 'complete' SETTINGS mutations_sync = 1"
    else
        if [ "$current_status" != "running" ] ; then
            echo "Warning : cannot set process status to $state because it appears to already be complete. You should determine which database is actually in production currently, and validate the update_status table in database $update_management_database_name" >&2
            return 0
        fi
        if [ "$state" == "complete" ] ; then
            update_statement="ALTER TABLE \`$update_management_database_name\`.update_status UPDATE update_process_status = 'complete', time_of_last_update_process_completion = now(), current_database_in_production = if(current_database_in_production = 'blue', 'green', 'blue') WHERE update_process_status = 'running' SETTINGS mutations_sync = 1"
        else
            # handle abandoned attempts to import (keep previous color and timestamp but return to state "completed")
            update_statement="ALTER TABLE \`$update_management_database_name\`.update_status UPDATE update_process_status = 'complete' WHERE update_process_status = 'running' SETTINGS mutations_sync = 1"
        fi
    fi
    if ! execute_sql_statement_via_clickhouse_client "$update_statement" "$update_status_filepath" ; then
        echo "Error : failed to execute SQL statement \"$update_statement\"" >&2
        return 1
    fi
    echo "Status table updated"
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
