#!/usr/bin/env bash

unset configured_sling_env_dir_path
unset configured_sling_env_file_path
unset sql_data_field_value
unset sql_data_array
configured_sling_env_dir_path=""
configured_sling_env_file_path=""
sql_data_field_value=""
declare -a sql_data_array
sling_database_exists_filepath="$(pwd)/sclf_database_exists.txt"
sling_database_table_list_filepath="$(pwd)/sclf_database_table_list.txt"

function write_selected_mysql_connection_to_env_file() {
    local env_file=$1
    local database_to_transfer=$2
    local db_name=""
    if [ -z "$database_to_transfer" ] ; then
        db_name="${my_properties['mysql_database_name']}"
    else
        if [ "$database_to_transfer" == "blue" ] ; then
            db_name="${my_properties['mysql_blue_database_name']}"
        else
            db_name="${my_properties['mysql_green_database_name']}"
        fi 
    fi
    echo "  MYSQL_DATABASE_CONNECTION:" >> "$env_file"
    echo "    type: mysql" >> "$env_file"
    echo "    database: $db_name" >> "$env_file"
    echo "    host: ${my_properties['mysql_server_host_name']}" >> "$env_file"
    echo "    password: ${my_properties['mysql_server_password']}" >> "$env_file"
    echo "    port: \"${my_properties['mysql_server_port']}\"" >> "$env_file"
    echo "    user: ${my_properties['mysql_server_username']}" >> "$env_file"
    echo >> "$env_file"
}

function write_selected_clickhouse_connection_to_env_file() {
    local env_file=$1
    local database_to_transfer=$2
    local db_name=""
    if [ -z "$database_to_transfer" ] ; then
        db_name="${my_properties['mysql_database_name']}"
    else
        if [ "$database_to_transfer" == "blue" ] ; then
            db_name="${my_properties['mysql_blue_database_name']}"
        else
            db_name="${my_properties['mysql_green_database_name']}"
        fi 
    fi
    local uname="${my_properties['clickhouse_server_username']}"
    local pw="${my_properties['clickhouse_server_password']}"
    local clickhost="${my_properties['clickhouse_server_host_name']}"
    local clickport="${my_properties['clickhouse_server_port']}"
    local additional_args="${my_properties['clickhouse_server_additional_args']}"
    echo "  CLICKHOUSE_DATABASE_CONNECTION:" >> "$env_file"
    echo "    type: clickhouse" >> "$env_file"
    echo "    database: $db_name$additional_args" >> "$env_file"
    echo "    host: ${my_properties['clickhouse_server_host_name']}" >> "$env_file"
    echo "    password: ${my_properties['clickhouse_server_password']}" >> "$env_file"
    echo "    port: \"${my_properties['clickhouse_server_port']}\"" >> "$env_file"
    echo "    user: ${my_properties['clickhouse_server_username']}" >> "$env_file"
    echo >> "$env_file"
}

function write_sling_env_file() {
    local database_to_transfer=$1
    configured_sling_env_dir_path="$(pwd)/sling_env_$(date "+%Y-%m-%d-%H-%M-%S")"
    configured_sling_env_file_path="$configured_sling_env_dir_path/env.yaml"
    local env_dir="$configured_sling_env_dir_path"
    local env_file="$configured_sling_env_file_path"
    if ! rm -rf "$env_dir" || ! mkdir "$env_dir" ; then
        echo "Error : unable to create sling env.yaml subdirectory $env_dir"
        return 1
    fi
    chmod 700 "$env_dir"
    echo "# Environment Credentials for Sling CLI" > "$env_file"
    chmod 600 "$env_file"
    echo >> "$env_file"
    echo "# See https://docs.slingdata.io/sling-cli/environment" >> "$env_file"
    echo >> "$env_file"
    echo "connections:" >> "$env_file"
    write_selected_mysql_connection_to_env_file "$env_file" "$database_to_transfer"
    write_selected_clickhouse_connection_to_env_file "$env_file" "$database_to_transfer"
    echo "variables: {}" >> "$env_file"
    if ! [ "$(cat $env_file | wc -l)" == "22" ] ; then
        echo "Error : could not successfully write default mysql properties to file $env_file" >&2
        return 1
    fi
    return 0
}

function initialize_sling_command_line_functions() {
    local database_to_transfer=$1
    write_sling_env_file "$database_to_transfer"
}

function shutdown_sling_command_line_functions() {
    rm -f "$configured_sling_env_file_path"
    rm -f "$configured_sling_env_dir_path/.sling"*
    rmdir "$configured_sling_env_dir_path"
    rm -f "$sling_database_exists_filepath"
    rm -f "$sling_database_table_list_filepath"
    unset configured_sling_env_dir_path
    unset configured_sling_env_file_path
    unset sql_data_field_value
    unset sql_data_array
    unset sling_database_exists_filepath
    unset sling_database_table_list_filepath
}

function execute_sql_statement_via_sling() {
    local statement=$1
    local db_server=$2 # must be 'mysql' or 'clickhouse'
    local output_filepath=$3
    local sling_connection=""
    if [ "$db_server" == "mysql" ] ; then
        sling_connection="MYSQL_DATABASE_CONNECTION"
    else
        if [ "$db_server" == "clickhouse" ] ; then
            sling_connection="CLICKHOUSE_DATABASE_CONNECTION"
        else
            echo "Error : db_server argument to execute_sql_statement_via_list must be 'mysql' or 'clickhouse'. Received : $db_server"
        fi
    fi
    if [ -e "$output_filepath" ] && ! rm -f "$output_filepath" ; then
        echo "Error : could not overwrite existing output file $output_filepath when executing mysql statment $statement" >&2
    fi
    (
        export DBUS_SESSION_BUS_ADDRESS=/dev/null ;
        export SLING_HOME_DIR="$configured_sling_env_dir_path" ;
        sling run --src-conn "$sling_connection" --src-stream "$statement" --stdout > "$output_filepath"
    )
}

function transfer_table_data_via_sling() {
    local mysql_source_database_name=$1
    local clickhouse_destination_database_name=$2
    local table_name=$3
    local output_filepath=$4
    if [ "$table_name" == "genetic_alteration" ] && [ "$SLING_GENETIC_ALTERATION_DATA_IN_CHUNKS" == "yes" ] ; then
        transfer_table_data_via_sling_chunks "$mysql_source_database_name" "$clickhouse_destination_database_name" "$table_name" "$output_filepath"
    else
    (
        export DBUS_SESSION_BUS_ADDRESS=/dev/null ;
        export SLING_HOME_DIR="$configured_sling_env_dir_path" ;
        export SLING_ALLOW_EMPTY="TRUE" ;
        sling run \
            --src-conn MYSQL_DATABASE_CONNECTION \
            --src-stream "$mysql_source_database_name.$table_name" \
            --tgt-conn CLICKHOUSE_DATABASE_CONNECTION \
            --tgt-object "$clickhouse_destination_database_name.$table_name" \
            --stdout \
            > "$output_filepath"
    )
    fi
}

function transfer_table_data_via_sling_chunks() {
    local mysql_source_database_name=$1
    local clickhouse_destination_database_name=$2
    local table_name=$3
    local output_filepath=$4
    (
        export DBUS_SESSION_BUS_ADDRESS=/dev/null ;
        export SLING_HOME_DIR="$configured_sling_env_dir_path" ;
        export SLING_ALLOW_EMPTY="TRUE" ;
        sling run \
            --src-conn MYSQL_DATABASE_CONNECTION \
            --src-stream "$mysql_source_database_name.$table_name" \
            --tgt-conn CLICKHOUSE_DATABASE_CONNECTION \
            --tgt-object "$clickhouse_destination_database_name.$table_name" \
            --mode backfill \
            --primary-key "genetic_profile_id,genetic_entity_id" \
            --update-key genetic_entity_id \
            --range '-9999999999,15000' \
            > "$output_filepath"
        status=$?
        if [ $status -eq 0 ] ; then
            sling run \
                --src-conn MYSQL_DATABASE_CONNECTION \
                --src-stream "$mysql_source_database_name.$table_name" \
                --tgt-conn CLICKHOUSE_DATABASE_CONNECTION \
                --tgt-object "$clickhouse_destination_database_name.$table_name" \
                --mode backfill \
                --primary-key "genetic_profile_id,genetic_entity_id" \
                --update-key genetic_entity_id \
                --range '15001,30000' \
                >> "$output_filepath"
            status=$?
        fi
        if [ $status -eq 0 ] ; then
            sling run \
                --src-conn MYSQL_DATABASE_CONNECTION \
                --src-stream "$mysql_source_database_name.$table_name" \
                --tgt-conn CLICKHOUSE_DATABASE_CONNECTION \
                --tgt-object "$clickhouse_destination_database_name.$table_name" \
                --mode backfill \
                --primary-key "genetic_profile_id,genetic_entity_id" \
                --update-key genetic_entity_id \
                --range '30001,9999999999' \
                >> "$output_filepath"
            status=$?
        fi
        return $status
    )
}

# set_sling_sql_data_field_value_from_record
# This function currently assumes that all queries are going to produce simple results
# which do not cause the quotation of value fields. Output from sling is in comma separated
# value format, so any value which contains a comma, or which contains a quotation mark or
# any of the other reserved characters (such as embedded newlines, embedded quotation marks, etc)
# Currently the only anticipated use of this function is for querying cBioPortal table names
# or record counts from tables. If use of this function expands to more general value retrieval
# it may be necessary to add proper parsing of quoted values. The presence of quotation marks
# on the line currently cuases a failure to parse.
function set_sling_sql_data_field_value_from_record() {
    local record_string=$1
    local column_number=$2
    unset sql_data_field_value
    local record_string_length=${#record_string}
    local LF=$'\n'
    local pos=0
    local field_index=0
    local parsed_value=""
    while [ $pos -lt $record_string_length ] ; do
        local character_at_position="${record_string:$pos:1}"
        # no quoted values allowed
        if [ "$character_at_position" == "\"" ] ; then
            echo "Error : encountered quotation mark (not yet handled) while looking for column $column_number during parsing returned database record : $record_string" >&2
            return 1
        fi
        # a newline should occur at the end of the read line, and only there.
        if [ "$character_at_position" == "$LF" ] ; then
            field_index=$((field_index+1))
            if [ "$field_index" -gt "$column_number" ] ; then
                # field has been completely parsed
                sql_data_field_value="$parsed_value"
                return 0
            fi
            echo "Error : unable to locate column $column_number while parsing returned database record : $record_string" >&2
            return 1
        fi
        # a comma character delimits the beginning of a new field, and is not part of the field.
        if [ "$character_at_position" == "," ] ; then
            field_index=$((field_index+1))
            if [ "$field_index" -gt "$column_number" ] ; then
                # field has been completely parsed
                sql_data_field_value="$parsed_value"
                return 0
            fi
            pos=$(($pos+1))
            continue
        fi
        # pass over the current (plain) character
        pos=$(($pos+1))
        if [ "$field_index" -eq "$column_number" ] ; then
            parsed_value+="$character_at_position"
        fi
    done
    #TODO : add removing of flanking whitespace
    sql_data_field_value="$parsed_value"
}

function set_sling_sql_data_array_from_file() {
    local filepath=$1
    local column_number=$2
    unset sql_data_array
    if ! [ -r "$filepath" ] ; then
        echo "Error : could not read output mysql query results from file : $filepath" >&2
        return 1
    fi
    local headers_have_been_parsed=0
    sql_data_array=()
    while IFS='' read -r line ; do
        if [ "$headers_have_been_parsed" -eq 0 ] ; then
            headers_have_been_parsed=1
        else
            set_sling_sql_data_field_value_from_record "$line" "$column_number"
            sql_data_array+=("$sql_data_field_value")
        fi
    done < "$filepath"
}

function clickhouse_database_exists() {
    local database_name=$1
    local statement="SELECT COUNT(*) FROM system.databases WHERE name = '$database_name'"
    if ! execute_sql_statement_via_sling "$statement" "clickhouse" "$sling_database_exists_filepath" ; then
        echo "Warning : unable to determine if database $database_name exists using : $statement" >&2
        return 1
    fi
    set_sling_sql_data_array_from_file "$sling_database_exists_filepath" 0
    if [[ "${sql_data_array[0]}" -ne 1 ]] ; then
        echo "Warning : database $database_name not present on database server, or there are multiple listings for that name" >&2
        return 2
    fi
    return 0
}

function clickhouse_database_is_empty() {
    local database_name=$1
    #local statement="SELECT COUNT(*) FROM INFORMATION_SCHEMA.tables WHERE table_schema='$database_name'"
    local statement="SELECT COUNT(*) FROM system.tables WHERE database = '$database_name'"
    if ! execute_sql_statement_via_sling "$statement" "clickhouse" "$sling_database_table_list_filepath" ; then
        echo "Warning : unable to retrieve table/view list from database $database_name using : $statement" >&2
        return 1
    fi
    set_sling_sql_data_array_from_file "$sling_database_table_list_filepath" 0
    if [[ "${sql_data_array[0]}" -ne 0 ]] ; then
        echo "Warning : database $database_name has tables or views (is not empty as required)" >&2
        return 2
    fi
    return 0
}
