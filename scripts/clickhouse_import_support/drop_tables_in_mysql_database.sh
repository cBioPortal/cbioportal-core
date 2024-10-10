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

# non-local environment variables in use
unset my_properties
unset database_table_list
unset database_name
declare -A my_properties
declare -a database_table_list
database_name=""
database_table_list_filepath="$(pwd)/dtmd_database_table_list.txt"
drop_table_result_filepath="$(pwd)/dtmd_drop_table_result.txt"

function usage() {
    echo "usage: drop_tables_in_mysql_database.sh properties_filepath database" >&2
    echo "         database must be in {blue, green}" >&2
}

function initialize_main() {
    local properties_filepath=$1
    local database_to_drop_tables_from=$2
    if ! parse_property_file "$properties_filepath" my_properties ; then
        usage
        return 1
    fi
    if ! initialize_mysql_command_line_functions ; then # this also purges the mysql credentials from the environment for security
        usage
        return 1
    fi
    remove_credentials_from_properties my_properties # no longer needed - remove for security
    if [ "$database_to_drop_tables_from" == "blue" ] ; then
        database_name="${my_properties['mysql_blue_database_name']}"
    else
        if [ "$database_to_drop_tables_from" == "green" ] ; then
            database_name="${my_properties['mysql_green_database_name']}"
        else
            echo "Error : database must be one of {blue, green}" >&2
            usage
            return 1
        fi
    fi
    return 0
}

function delete_output_stream_files() {
    rm -f "$database_table_list_filepath"
    rm -f "$drop_table_result_filepath"
}

function shutdown_main_and_clean_up() {
    shutdown_mysql_command_line_functions
    delete_output_stream_files
    unset my_properties
    unset database_table_list
    unset database_name
    unset database_table_list_filepath
    unset drop_table_result_filepath
}

function selected_database_exists() {
    if ! database_exists "$database_name" ; then
        echo "Error : could not proceed with database table dropping because database does not exist: $database_name" >&2
        return 1
    fi
    return 0
}

function set_database_table_list() {
    local statement="SHOW TABLES IN $database_name"
    rm -f "$database_table_list_filepath"
    if ! execute_sql_statement_via_mysql "$statement" "$database_table_list_filepath" ; then
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

function print_database_table_list() {
    local pos=0
    local num_tables=${#database_table_list[@]}
    while [ $pos -lt $num_tables ] ; do
        echo "${database_table_list[$pos]}"
        pos=$(($pos+1))
    done
}

function drop_database_table() {
    local pos=$1
    local drop_table_statement="SET FOREIGN_KEY_CHECKS=0; USE \`$database_name\`; DROP TABLE ${database_table_list[$pos]};"
    if ! execute_sql_statement_via_mysql "$drop_table_statement" "$drop_table_result_filepath" ; then
        return 1
    fi
    return 0
}

function drop_all_database_tables() {
    local pos=0
    local num_tables=${#database_table_list[@]}
    while [ "$pos" -lt "$num_tables" ] ; do
        local table_name="${database_table_list[$pos]}"
        echo "dropping $table_name"
        if ! drop_database_table "$pos" ; then
            echo "Error : could not drop database table $table_name" >&2
            return 1
        fi
        pos=$(($pos+1))
    done
    return 0
}

function selected_database_is_empty() {
    if ! database_is_empty "$database_name" ; then
        echo "Error : table dropping failed to drop all tables in database : $database_name" >&2
        return 1
    fi
    return 0
}

function main() {
    local properties_filepath=$1
    local database_to_drop_tables_from=$2
    local exit_status=0
    if ! initialize_main "$properties_filepath" "$database_to_drop_tables_from" ||
            ! selected_database_exists ||
            ! set_database_table_list ||
            ! drop_all_database_tables ||
            ! selected_database_is_empty ; then
        exit_status=1
    fi
    shutdown_main_and_clean_up
    return $exit_status
}

main "$1" "$2"
