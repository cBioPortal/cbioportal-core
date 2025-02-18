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
if ! source "$this_script_dir/clickhouse_client_command_line_functions.sh" ; then
    echo "Error : unable to load dependency : $this_script_dir/clickhouse_client_command_line_functions.sh" >&2
    exit 1
fi
unset this_script_dir

# other non-local environment variables in use
unset my_properties
unset mysql_source_database_name
unset mysql_destination_database_name
unset clickhouse_destination_database_name
declare -A my_properties
missing_user_records_result_filepath="$(pwd)/missing_user_records.txt"
missing_authority_records_result_filepath="$(pwd)/missing_authority_records.txt"
insert_user_record_result_filepath="$(pwd)/insert_user_result.txt"
insert_authority_record_result_filepath="$(pwd)/insert_authority_result.txt"
clickhouse_is_responsive_filepath="$(pwd)/cdtcd_cmd_clickhouse_is_responsive.txt"
SECONDS_BETWEEN_RESPONSIVENESS_RETRY=$((60))

function usage() {
    echo "usage: synchronize_user_tables_between_databases.sh properties_filepath database_to_synchronize_from database_to_synchronize_to" >&2
    echo "         database_to_synchronize_from and database_to_synchronize_to must be in {blue, green}" >&2
    echo "         This tool copies over any user records added to the active/production database during the time it took to update the non-production database." >&2
}

function initialize_main() {
    local properties_filepath=$1
    local database_to_synchronize_from=$2
    local database_to_synchronize_to=$3
    if ! parse_property_file "$properties_filepath" my_properties ; then
        usage
        return 1
    fi
    if ! initialize_mysql_command_line_functions ; then
        usage
        return 1
    fi
    if ! initialize_clickhouse_client_command_line_functions "$database_to_synchronize_to" ; then
        usage
        return 1
    fi
    remove_credentials_from_properties my_properties # no longer needed - remove for security
    if [ "$database_to_synchronize_from" == "blue" ] ; then
        mysql_source_database_name="${my_properties['mysql_blue_database_name']}"
    else
        if [ "$database_to_synchronize_from" == "green" ] ; then
            mysql_source_database_name="${my_properties['mysql_green_database_name']}"
        else
            echo "Error : database_to_synchronize_from must be one of {blue, green}" >&2
            usage
            return 1
        fi
    fi
    if [ "$database_to_synchronize_to" == "blue" ] ; then
        mysql_destination_database_name="${my_properties['mysql_blue_database_name']}"
        clickhouse_destination_database_name="${my_properties['clickhouse_blue_database_name']}"
    else
        if [ "$database_to_synchronize_to" == "green" ] ; then
            mysql_destination_database_name="${my_properties['mysql_green_database_name']}"
            clickhouse_destination_database_name="${my_properties['clickhouse_green_database_name']}"
        else
            echo "Error : database_to_synchronize_to must be one of {blue, green}" >&2
            usage
            return 1
        fi
    fi
    if [ "$database_to_synchronize_to" == "$database_to_synchronize_from" ] ; then
        echo "Error : database_to_synchronize_to cannot be the same as database_to_synchronize_from" >&2
        return 1
    fi
    return 0
}

function delete_output_stream_files() {
    rm -f "$missing_user_records_result_filepath"
    rm -f "$missing_authority_records_result_filepath"
    rm -f "$insert_user_record_result_filepath"
    rm -f "$insert_authority_record_result_filepath"
    rm -f "$clickhouse_is_responsive_filepath"
}

function shutdown_main_and_clean_up() {
    shutdown_mysql_command_line_functions
    shutdown_clickhouse_client_command_line_functions
    delete_output_stream_files
    unset my_properties
    unset mysql_source_database_name
    unset mysql_destination_database_name
    unset clickhouse_destination_database_name
    unset missing_user_records_result_filepath 
    unset missing_authority_records_result_filepath
    unset insert_user_record_result_filepath
    unset insert_authority_record_result_filepath
    unset clickhouse_is_responsive_filepath
    unset SECONDS_BETWEEN_RESPONSIVENESS_RETRY
}

function mysql_destination_database_exists() {
    if ! database_exists "$mysql_destination_database_name" ; then
        echo "Error : could not proceed with database cloning because destination database does not exist: $mysql_destination_database_name" >&2
        return 1
    fi
    return 0
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

function clickhouse_destination_database_exists() {
    if ! clickhouse_database_exists "$clickhouse_destination_database_name"; then
        echo "Error : could not proceed with creation of derived tables because database does not exist: $clickhouse_destination_database_name" >&2
        return 1
    fi
    return 0
}

function find_records_missing_in_destination_database_users_table() {
    local table_name="users"
    local source_table_full_name="\`$mysql_source_database_name\`.\`$table_name\`"
    local destination_table_full_name="\`$mysql_destination_database_name\`.\`$table_name\`"
    local find_records_missing_in_destination_database_table_statement="SELECT $source_table_full_name.EMAIL, $source_table_full_name.NAME, $source_table_full_name.ENABLED FROM $source_table_full_name LEFT JOIN $destination_table_full_name ON $source_table_full_name.EMAIL=$destination_table_full_name.EMAIL WHERE $destination_table_full_name.EMAIL IS NULL;"
    if ! execute_sql_statement_via_mysql "$find_records_missing_in_destination_database_table_statement" "$missing_user_records_result_filepath" ; then
        return 1
    fi
    return 0
}

function find_records_missing_in_destination_database_authorities_table() {
    local table_name="authorities"
    local source_table_full_name="\`$mysql_source_database_name\`.\`$table_name\`"
    local destination_table_full_name="\`$mysql_destination_database_name\`.\`$table_name\`"
    local find_records_missing_in_destination_database_table_statement="SELECT $source_table_full_name.EMAIL, $source_table_full_name.AUTHORITY FROM $source_table_full_name LEFT JOIN $destination_table_full_name ON $source_table_full_name.EMAIL=$destination_table_full_name.EMAIL AND $source_table_full_name.AUTHORITY=$destination_table_full_name.AUTHORITY WHERE $destination_table_full_name.EMAIL IS NULL;"
    if ! execute_sql_statement_via_mysql "$find_records_missing_in_destination_database_table_statement" "$missing_authority_records_result_filepath" ; then
        return 1
    fi
    return 0
}

function insert_missing_user_record_into_mysql_destination_database() {
    local email=$1
    local name=$2
    local enabled=$3
    local table_name="users"
    local destination_table_full_name="\`$mysql_destination_database_name\`.\`$table_name\`"
    local insert_missing_user_record_into_mysql_destination_database_table_statement="INSERT INTO $destination_table_full_name (EMAIL, NAME, ENABLED) VALUES ('$email', '$name', '$enabled');"
    if ! execute_sql_statement_via_mysql "$insert_missing_user_record_into_mysql_destination_database_table_statement" "$insert_user_record_result_filepath" ; then
        return 1
    fi
    return 0
}

function insert_missing_authority_record_into_mysql_destination_database() {
    local email=$1
    local authority=$2
    local table_name="authorities"
    local destination_table_full_name="\`$mysql_destination_database_name\`.\`$table_name\`"
    local insert_missing_authority_record_into_mysql_destination_database_table_statement="INSERT INTO $destination_table_full_name (EMAIL, AUTHORITY) VALUES ('$email', '$authority');"
    if ! execute_sql_statement_via_mysql "$insert_missing_authority_record_into_mysql_destination_database_table_statement" "$insert_authority_record_result_filepath" ; then
        return 1
    fi
    return 0
}

function insert_missing_user_record_into_clickhouse_destination_database() {
    local email=$1
    local name=$2
    local enabled=$3
    local table_name="users"
    local destination_table_full_name="\`$clickhouse_destination_database_name\`.\`$table_name\`"
    local insert_missing_user_record_into_clickhouse_destination_database_table_statement="INSERT INTO $destination_table_full_name (email, name, enabled) VALUES ('$email', '$name', '$enabled');"
    if ! execute_sql_statement_via_clickhouse_client "$insert_missing_user_record_into_clickhouse_destination_database_table_statement" "$insert_user_record_result_filepath" ; then
        return 1
    fi
    return 0
}

function insert_missing_authority_record_into_clickhouse_destination_database() {
    local email=$1
    local authority=$2
    local table_name="authorities"
    local destination_table_full_name="\`$clickhouse_destination_database_name\`.\`$table_name\`"
    local insert_missing_authority_record_into_clickhouse_destination_database_table_statement="INSERT INTO $destination_table_full_name (email, authority) VALUES ('$email', '$authority');"
    if ! execute_sql_statement_via_clickhouse_client "$insert_missing_authority_record_into_clickhouse_destination_database_table_statement" "$insert_authority_record_result_filepath" ; then
        return 1
    fi
    return 0
}

function insert_missing_records_into_mysql_destination_database_users_table() {
    unset sql_data_array
    if ! set_mysql_sql_data_array_from_file "$missing_user_records_result_filepath" 0 ; then
        return 1
    fi
    local email_list=(${sql_data_array[@]})
    unset sql_data_array
    if ! set_mysql_sql_data_array_from_file "$missing_user_records_result_filepath" 1 ; then
        return 1
    fi
    local name_list=(${sql_data_array[@]})
    unset sql_data_array
    if ! set_mysql_sql_data_array_from_file "$missing_user_records_result_filepath" 2 ; then
        return 1
    fi
    local enabled_list=(${sql_data_array[@]})

    local pos=0
    local num_missing_records=${#email_list[@]}
    while [ $pos -lt $num_missing_records ]; do
        echo "Adding user '${name_list[$pos]}' with email '${email_list[$pos]}' to $mysql_destination_database_name users table"
        insert_missing_user_record_into_mysql_destination_database ${email_list[$pos]} ${name_list[$pos]} ${enabled_list[$pos]}
        insert_missing_user_record_into_clickhouse_destination_database ${email_list[$pos]} ${name_list[$pos]} ${enabled_list[$pos]}
        pos=$(($pos+1))
    done
    return 0
}

function insert_missing_records_into_mysql_destination_database_authorities_table() {
    unset sql_data_array
    if ! set_mysql_sql_data_array_from_file "$missing_authority_records_result_filepath" 0 ; then
        return 1
    fi
    local email_list=(${sql_data_array[@]})
    unset sql_data_array
    if ! set_mysql_sql_data_array_from_file "$missing_authority_records_result_filepath" 1 ; then
        return 1
    fi
    local authority_list=(${sql_data_array[@]})

    local pos=0
    local num_missing_records=${#email_list[@]}
    while [ $pos -lt $num_missing_records ]; do
        echo "Adding email '${email_list[$pos]}' with authority '${authority_list[$pos]}' to $mysql_destination_database_name authorities table"
        insert_missing_authority_record_into_mysql_destination_database ${email_list[$pos]} ${authority_list[$pos]}
        insert_missing_authority_record_into_clickhouse_destination_database ${email_list[$pos]} ${authority_list[$pos]}
        pos=$(($pos+1))
    done
    return 0
}

function main() {
    local properties_filepath=$1
    local database_to_synchronize_from=$2
    local database_to_synchronize_to=$3
    local exit_status=0
    if ! initialize_main "$properties_filepath" "$database_to_synchronize_from" "$database_to_synchronize_to" ||
            ! clickhouse_is_responding ||
            ! mysql_destination_database_exists ||
            ! clickhouse_destination_database_exists ||
            ! find_records_missing_in_destination_database_users_table ||
            ! find_records_missing_in_destination_database_authorities_table ||
            ! insert_missing_records_into_mysql_destination_database_users_table ||
            ! insert_missing_records_into_mysql_destination_database_authorities_table; then
        exit_status=1
    fi
    shutdown_main_and_clean_up
    return $exit_status
}

main "$1" "$2" "$3"
