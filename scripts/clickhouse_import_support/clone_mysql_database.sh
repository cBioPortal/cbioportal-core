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
unset database_table_list
unset source_database_name
unset destination_database_name
unset source_database_create_table_statement_list
declare -A my_properties
declare -a database_table_list
declare -a source_database_create_table_statement_list
database_table_list_filepath="$(pwd)/cmd_database_table_list.txt"
create_table_statement_filepath="$(pwd)/cmd_create_table_statement.txt"
create_table_result_filepath="$(pwd)/cmd_create_database_result.txt"
insert_table_data_result_filepath="$(pwd)/cmd_copy_table_data.txt"
record_count_comparison_filepath="$(pwd)/cmd_table_record_count.txt"

function usage() {
    echo "usage: clone_mysql_database.sh properties_filepath database_to_clone_tables_from database_to_clone_tables_to" >&2
    echo "         databases (from/to) must be in {blue, green}" >&2
    echo "         This tool is used to clone one mysql database to another mysql database on the same server for dual-database installations." >&2
}

function initialize_main() {
    local properties_filepath=$1
    local database_to_clone_tables_from=$2
    local database_to_clone_tables_to=$3
    if ! parse_property_file "$properties_filepath" my_properties ; then
        usage
        return 1
    fi
    if ! initialize_mysql_command_line_functions ; then
        usage
        return 1
    fi
    remove_credentials_from_properties my_properties # no longer needed - remove for security
    if [ "$database_to_clone_tables_from" == "blue" ] ; then
        source_database_name="${my_properties['mysql_blue_database_name']}"
    else
        if [ "$database_to_clone_tables_from" == "green" ] ; then
            source_database_name="${my_properties['mysql_green_database_name']}"
        else
            echo "Error : database_to_clone_tables_from must be one of {blue, green}" >&2
            usage
            return 1
        fi
    fi
    if [ "$database_to_clone_tables_to" == "blue" ] ; then
        destination_database_name="${my_properties['mysql_blue_database_name']}"
    else
        if [ "$database_to_clone_tables_to" == "green" ] ; then
            destination_database_name="${my_properties['mysql_green_database_name']}"
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
    shutdown_mysql_command_line_functions
    delete_output_stream_files
    unset my_properties
    unset database_table_list
    unset source_database_name
    unset destination_database_name
    unset source_database_create_table_statement_list
    unset database_table_list_filepath
    unset create_table_statement_filepath
    unset create_table_result_filepath
    unset insert_table_data_result_filepath
    unset record_count_comparison_filepath
}

function destination_database_exists_and_is_empty() {
    if ! database_exists "$destination_database_name" ; then
        echo "Error : could not proceed with database cloning because destination database does not exist: $destination_database_name" >&2
        return 1
    fi
    if ! database_is_empty "$destination_database_name" ; then
        echo "Error : could not proceed with database cloning because destination database is not empty: $destination_database_name" >&2
        return 2
    fi
    return 0
}

function set_database_table_list() {
    local statement="SHOW TABLES IN $source_database_name"
    rm -f "$database_table_list_filepath"
    if ! execute_sql_statement_via_mysql "$statement" "$database_table_list_filepath" ; then
        echo "Warning : failed to execute mysql statement : $statement" >&2
        return 1
    fi
    unset sql_data_array
    if ! set_mysql_sql_data_array_from_file "$database_table_list_filepath" 0 ; then
        return 1
    fi
    database_table_list=(${sql_data_array[@]})
    return 0
}

function print_database_table_list() {
    local pos=0
    local num_tables=${#database_table_list[@]}
    while [ $pos -lt $num_tables ] ; do
        echo "${database_table_list[$pos]}"
        pos=$(($pos+1))
    done
}

function set_source_database_create_table_statement_list() {
    source_database_create_table_statement_list=()
    local pos=0
    local num_tables=${#database_table_list[@]}
    while [ $pos -lt $num_tables ] ; do
        local table_name="\`$source_database_name\`.\`${database_table_list[$pos]}\`"
        local statement="SHOW CREATE TABLE $table_name"
        rm -f "$create_table_statement_filepath"
        if ! execute_sql_statement_via_mysql "$statement" "$create_table_statement_filepath" ; then
            echo "Warning : failed to execute mysql statement : $statement" >&2
            return 1
        fi
        if ! set_mysql_sql_data_array_from_file "$create_table_statement_filepath" 1 ; then
            return 1
        fi
        source_database_create_table_statement_list+=("${sql_data_array[0]}")
        pos=$(($pos+1))
    done
    return 0
}

function print_source_database_create_table_statement_list() {
    local pos=0
    local num_tables=${#source_database_create_table_statement_list[@]}
    while [ $pos -lt $num_tables ] ; do
        echo "for table ${database_table_list[$pos]} : ${source_database_create_table_statement_list[$pos]}"
        pos=$(($pos+1))
    done
}

function create_destination_database_table_schema_only() {
    local pos=$1
    local create_source_table_statement="${source_database_create_table_statement_list[$pos]}"
    local create_destination_table_statement="SET FOREIGN_KEY_CHECKS=0; USE \`$destination_database_name\`; ${source_database_create_table_statement_list[$pos]};"
    if ! execute_sql_statement_via_mysql "$create_destination_table_statement" "$create_table_result_filepath" ; then
        return 1
    fi
    return 0
}

function copy_source_database_table_data_to_destination() {
    local table_name=$1
    local source_table_full_name="\`$source_database_name\`.\`$table_name\`"
    local destination_table_full_name="\`$destination_database_name\`.\`$table_name\`"
    local copy_data_from_source_to_destination_table_statement="SET FOREIGN_KEY_CHECKS=0; INSERT INTO $destination_table_full_name TABLE $source_table_full_name;"
    if ! execute_sql_statement_via_mysql "$copy_data_from_source_to_destination_table_statement" "$insert_table_data_result_filepath" ; then
        return 1
    fi
    return 0
}

function destination_table_matches_source_table() {
    local table_name=$1
    local source_table_full_name="\`$source_database_name\`.\`$table_name\`"
    local destination_table_full_name="\`$destination_database_name\`.\`$table_name\`"
    local get_record_counts_statement="SELECT count(*) AS record_count from $destination_table_full_name UNION DISTINCT SELECT count(*) as record_count from $source_table_full_name;"
    execute_sql_statement_via_mysql "$get_record_counts_statement" "$record_count_comparison_filepath"
    # if recourd_counts match, only one distinct value will be returned. If they differ, there will be 2 values
    set_mysql_sql_data_array_from_file "$record_count_comparison_filepath" 0
    if [[ "${#sql_data_array[@]}" -ne 1 ]] ; then
        local record_count_0="${sql_data_array[0]}"
        local record_count_1="${sql_data_array[1]}"
        echo "Error : when cloning data from table $table_name, source database and destination database tables contain different record counts ($record_count_0 v. $record_count_1)" >&2
        return 1
    fi
    return 0
}

function clone_all_source_database_tables_to_destination_database() {
    pos=0
    local num_tables=${#source_database_create_table_statement_list[@]}
    while [ "$pos" -lt "$num_tables" ] ; do
        local table_name="${database_table_list[$pos]}"
        echo "cloning $table_name"
        if ! create_destination_database_table_schema_only "$pos" ; then
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
            ! set_source_database_create_table_statement_list ||
            ! clone_all_source_database_tables_to_destination_database ; then
        exit_status=1
    fi
    shutdown_main_and_clean_up
    return $exit_status
}

main "$1" "$2" "$3"
