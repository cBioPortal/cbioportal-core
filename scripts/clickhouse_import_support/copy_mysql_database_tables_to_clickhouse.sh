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
    echo "usage:"
}

# other non-local environment variables in use
unset my_properties
unset database_table_list
unset clickhouse_destination_database_name
unset mysql_source_database_name
declare -A my_properties
declare -a database_table_list
declare -A table_has_been_copied_and_verified
declare -A mysql_database_table_record_count
mysql_table_record_count=""
clickhouse_destination_database_name=""
mysql_source_database_name=""
database_table_list_filepath="$(pwd)/cmd_database_table_list.txt"
mysql_table_record_count_filepath="$(pwd)/cmd_mysql_table_record_count.txt"
clickhouse_table_record_count_filepath="$(pwd)/cmd_clickhouse_table_record_count.txt"
copy_table_contents_with_sling_filepath="$(pwd)/cmd_copy_table_contents_with_sling.txt"
clickhouse_is_responsive_filepath="$(pwd)/cmd_clickhouse_is_responsive.txt"
SECONDS_BETWEEN_RESPONSIVENESS_RETRY=$((60))
SECONDS_BETWEEN_COPY_RETRY=$((15*60))

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

function clickhouse_is_responding() {
    local remaining_try_count=3
    local statement="SELECT 1"
    while [ $remaining_try_count -ne 0 ] ; do
        if execute_sql_statement_via_sling "$statement" "clickhouse" "$clickhouse_is_responsive_filepath" ; then
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

function set_mysql_table_record_count() {
    local table_name=$1
    local statement="SELECT count(*) FROM \`$table_name\`"
    rm -f "$mysql_table_record_count_filepath"
    if ! execute_sql_statement_via_sling "$statement" "mysql" "$mysql_table_record_count_filepath" ; then
        echo "Warning : failed to execute mysql statement : $statement" >&2
        return 1
    fi
    unset sql_data_array
    if ! set_sql_data_array_from_file "$mysql_table_record_count_filepath" 0 ; then
        return 1
    fi
    mysql_table_record_count=${sql_data_array[0]}
    return 0
    
}

function set_database_table_record_counts() {
    local pos=0
    while [ $pos -lt ${#database_table_list[@]} ] ; do
        table_name="${database_table_list[$pos]}"
        if ! set_mysql_table_record_count "$table_name" ; then
            echo "Error : could not determine the record count for table $table_name, so validation is not possible."
            return 1
        fi
        mysql_database_table_record_count[$table_name]=$mysql_table_record_count
        pos=$(($pos+1))
    done
    return 0
}

function set_database_table_list_and_record_counts() {
    set_database_table_list &&
    set_database_table_record_counts
}

function delete_output_stream_files() {
    #TODO : implement
    rm -f "$mysql_table_record_count_filepath"
    rm -f "$clickhouse_table_record_count_filepath"
    rm -f "$copy_table_contents_with_sling_filepath"
    rm -f "$clickhouse_is_responsive_filepath"
    return 0
}

function shutdown_main_and_clean_up() {
    shutdown_sling_command_line_functions
    delete_output_stream_files
    unset my_properties
    unset database_table_list
    unset table_has_been_copied_and_verified
    unset mysql_database_table_record_count
    unset mysql_table_record_count
    unset database_table_list_filepath
    unset mysql_table_record_count_filepath
    unset clickhouse_table_record_count_filepath
    unset copy_table_contents_with_sling_filepath
    unset clickhouse_is_responsive_filepath
    unset SECONDS_BETWEEN_RESPONSIVENESS_RETRY
    unset SECONDS_BETWEEN_COPY_RETRY
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

function destination_table_matches_source_table() {
    local table_name=$1
    local statement="SELECT COUNT(*) FROM \`$table_name\`"
    rm -f "$database_table_list_filepath"
    if ! execute_sql_statement_via_sling "$statement" "clickhouse" "$clickhouse_table_record_count_filepath" ; then
        echo "Warning : failed to execute mysql statement : $statement" >&2
        return 1
    fi
    unset sql_data_array
    if ! set_sql_data_array_from_file "$clickhouse_table_record_count_filepath" 0 ; then
        return 1
    fi
    local clickhouse_table_record_count=${sql_data_array[0]}
    local mysql_table_record_count=${mysql_database_table_record_count[$table_name]}
echo "comparing record counts for table $table_name : mysql=$mysql_table_record_count clickhouse=$clickhouse_table_record_count"
    if [ "$clickhouse_table_record_count" != "$mysql_table_record_count" ] ; then
        return 1
    fi
    return 0
}

function copy_all_database_tables_with_sling() {
    local pos=0
    local exit_status=0
    while [ $pos -lt ${#database_table_list[@]} ] ; do
        table_name="${database_table_list[$pos]}"
        if successful_copy_verified_flag_has_been_set "$table_name" ; then
            # table successfully copied on a previous pass
            pos=$(($pos+1))
            continue
        fi
        echo "attempting to copy data in table $table_name using sling"
        if ! transfer_table_data_via_sling "$mysql_source_database_name" "$clickhouse_destination_database_name" "$table_name" "$copy_table_contents_with_sling_filepath" ; then
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
        if copy_all_database_tables_with_sling ; then
            return 0
        fi
        remaining_try_count=$((remaining_try_count-1))
        if [ $remaining_try_count -gt 0 ] ; then
            sleep $SECONDS_BETWEEN_COPY_RETRY
        fi
    done
    return 1
}

function main() {
    local properties_filepath=$1
    local database_to_transfer=$2
    local exit_status=0
    if ! initialize_main "$properties_filepath" "$database_to_transfer" ||
            ! clickhouse_is_responding ||
            ! destination_database_exists_and_is_empty ||
            ! set_database_table_list_and_record_counts ||
            ! copy_all_database_tables_with_sling_allow_retry ; then
        exit_status=1
    fi
    shutdown_main_and_clean_up
    return $exit_status
}

main "$1" "$2"
