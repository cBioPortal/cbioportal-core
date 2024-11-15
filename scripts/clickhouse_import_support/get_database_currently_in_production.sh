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
unset database_currently_in_production
database_currently_in_production=""
declare -A my_properties
record_count_filepath="$(pwd)/update_status_record_count.txt"
current_production_database_filepath="$(pwd)/current_production_database.txt"

function usage() {
    echo "usage: get_database_currently_in_production.sh properties_filepath" >&2
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
    ### TODO : fix this
    ###  remove_credentials_from_properties my_properties # no longer needed - remove for security
    return 0
}

function delete_output_stream_files() {
    rm -f "$record_count_filepath"
    rm -f "$current_production_database_filepath"
}

function shutdown_main_and_clean_up() {
    shutdown_mysql_command_line_functions
    delete_output_stream_files
    unset my_properties
    unset record_count_filepath
    unset current_production_database_filepath
}

function process_state_table_is_valid() {
    if ! database_exists "$update_management_database_name" ; then
        echo "Error : could not proceed with getting production database because database does not exist: $update_management_database_name" >&2
        return 1
    fi
    if ! table_exists "$update_management_database_name" "update_status" ; then
        echo "Error : could not proceed with getting production database because table 'update_status' does not exist in database : $update_management_database_name" >&2
        return 1
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

function get_database_currently_in_production() {
    local get_current_database_statement="SELECT current_database_in_production FROM \`$update_management_database_name\`.update_status;"
    if ! execute_sql_statement_via_mysql "$get_current_database_statement" "$current_production_database_filepath" ; then
        echo "Error : could not retrieve the current database in production. Mysql statement failed to execute properly : $get_current_database_statement" >&2
        return 1
    fi
    set_mysql_sql_data_array_from_file "$current_production_database_filepath" 0
    database_currently_in_production="${sql_data_array[0]}"
    return 0
}

function output_database_currently_in_production() {
    echo "$database_currently_in_production : current production database"
}

function main() {
    local properties_filepath=$1
    local exit_status=0
    if ! initialize_main "$properties_filepath" ||
            ! process_state_table_is_valid ||
            ! get_database_currently_in_production ; then
        exit_status=1
    fi
    output_database_currently_in_production
    shutdown_main_and_clean_up
    return $exit_status
}

main "$1"
