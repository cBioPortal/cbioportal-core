#!/usr/bin/env bash

# load dependencies
unset this_script_dir
this_script_dir="$(dirname "$(readlink -f $0)")"
if ! source "$this_script_dir/parse_property_file_functions.sh" ; then
    echo "Error : unable to load dependency : $this_script_dir/parse_property_file_functions.sh" >&2
    exit 1
fi
if ! source "$this_script_dir/sling_command_line_functions.sh" ; then
    echo "Error : unable to load dependency : $this_script_dir/sling_command_line_functions.sh" >&2
    exit 1
fi
unset this_script_dir

function usage() {
    echo "usage: copy_mysql_database_tables_to_clickhouse.sh properties_file database"
}

# other non-local environment variables in use
unset my_properties
unset database_table_list
unset clickhouse_destination_database_name
unset mysql_source_database_name
declare -A my_properties
declare -a database_table_list
declare -A table_has_been_copied_and_verified
clickhouse_destination_database_name=""
mysql_source_database_name=""
database_table_list_filepath="$(pwd)/cmd_database_table_list.txt"

function initialize_main() {
    if ! [ "$database_to_transfer" == "blue" ] && ! [ "$database_to_transfer" == "green" ] ; then
        echo "Error : argument for database_to_transfer must be either 'blue' or 'green'" >&2
        return 1
    fi
    if ! parse_property_file "$properties_filepath" my_properties ; then
        usage
        return 1
    fi
    if [ "$database_to_transfer" == "blue" ] ; then
        clickhouse_destination_database_name="${my_properties['clickhouse_blue_database_name']}"
        mysql_source_database_name="${my_properties['mysql_blue_database_name']}"
    else
        clickhouse_destination_database_name="${my_properties['clickhouse_green_database_name']}"
        mysql_source_database_name="${my_properties['mysql_green_database_name']}"
    fi
    if ! initialize_sling_command_line_functions "$database_to_transfer" ; then
        usage
        return 1
    fi
    remove_credentials_from_properties my_properties
}

function destination_database_exists_and_is_empty() {
    if ! clickhouse_database_exists "$clickhouse_destination_database_name" ; then
        echo "Error : could not proceed with database copying because destination database does not exist: $clickhouse_destination_database_name" >&2
        return 1
    fi
    if ! clickhouse_database_is_empty "$clickhouse_destination_database_name" ; then
        echo "Error : could not proceed with database copying because destination database is not empty: $clickhouse_destination_database_name" >&2
        return 2
    fi
    return 0
}

function set_database_table_list() {
    local statement="SELECT table_name FROM INFORMATION_SCHEMA.tables WHERE table_type='BASE TABLE' AND table_schema='$mysql_source_database_name'"
    rm -f "$database_table_list_filepath"
    if ! execute_sql_statement_via_sling "$statement" "mysql" "$database_table_list_filepath" ; then
        echo "Warning : failed to execute mysql statement : $statement" >&2
        return 1
    fi
    unset sql_data_array
    if ! set_sql_data_array_from_file "$database_table_list_filepath" 0 ; then
        return 1
    fi
    database_table_list=(${sql_data_array[@]})
    return 0
}

function shutdown_main_and_clean_up() {
    #TODO restore
    #shutdown_sling_command_line_functions
    delete_output_stream_files
    unset my_properties
    unset database_table_list
    unset table_has_been_copied_and_verified
    unset database_table_list_filepath
    unset record_count_comparison_filepath
}

function successful_copy_verified_flag_has_been_set() {
    local table_name=$1
    if [ "${table_has_been_copied_and_verified[$table_name]}" == "true" ] ; then
        return 0
    fi
    return 1
}

function set_successful_copy_verified_flag() {
    local table_name=$1
    table_has_been_copied_and_verified[$table_name]="true"
}

function copy_all_database_tables_with_sling() {
    local pos=0
    local exit_status=0
    while [ $pos -lt ${#database_table_list[@]} ] ; do
        table_name="${database_table_list[$pos]}"
        if successful_copy_verified_flag_has_been_set "$table_name" ; then
            # table successfully copied on a previous pass
            continue
        fi
        echo "attempting to copy data in table $table_name using sling"
        if ! transfer_table_data_via_sling "$mysql_source_database_name" "$clickhouse_destination_database_name" "$table_name" "TODOdeletefile" ; then
            echo "Warning : failure to copy table $table_name" >&2
            exit_status=1 # any failed table copies cause an eventual failure status to be returned
        else
            if ! destination_table_matches_source_table "$table_name" ; then
                echo "Warning : failure to verify copy of table $table_name" >&2
                exit_status=1 # any failed table copies cause an eventual failure status to be returned
            else
                set_successful_copy_verified_flag "$table_name"
            fi
        fi
        pos=$(($pos+1))
    done
    return $exit_status
}

function copy_all_database_tables_with_sling_allow_retry() {
    local remaining_try_count=3
    while [ $remaining_try_count -ne 0 ] ; do
        #TODO record iteration start timestamp
        if copy_all_database_tables_with_sling ; then
            return 0
        fi
        #TODO pause for the minimum try duration (5 minutes?)
        remaining_try_count=$((remaining_try_count-1))
    done
    return 1
}

function main() {
    local properties_filepath=$1
    local database_to_transfer=$2
    local exit_status=0
    if ! initialize_main "$properties_filepath" "$database_to_transfer" ||
            ! destination_database_exists_and_is_empty ||
            ! set_database_table_list ||
            ! copy_all_database_tables_with_sling_allow_retry ; then
        exit_status=1
    fi
    shutdown_main_and_clean_up
    return $exit_status
}

main "$1" "$2"

exit 0
