#!/usr/bin/env bash

unset configured_clickhouse_config_file_path
unset sql_data_field_value
unset sql_data_array
configured_clickhouse_config_file_path=""
sql_data_field_value=""
declare -a sql_data_array
clickhouse_client_database_exists_filepath="$(pwd)/ccclf_database_exists.txt"
clickhouse_client_database_table_list_filepath="$(pwd)/ccclf_database_table_list.txt"

function write_clickhouse_config_file() {
    local selected_database=$1
    configured_clickhouse_config_file_path="$(pwd)/clickhouse_client_config_$(date "+%Y-%m-%d-%H-%M-%S").yaml"
    if ! rm -f "$configured_clickhouse_config_file_path" || ! touch "$configured_clickhouse_config_file_path" ; then
        echo "Error : unable to create clickhouse_client_config file $configured_clickhouse_config_file_path" >&2
        return 1
    fi
    chmod 600 "$configured_clickhouse_config_file_path"
    local db_name
    if [ "$selected_database" == "blue" ] ; then
        db_name="${my_properties['clickhouse_blue_database_name']}"
    else
        if [ "$selected_database" == "green" ] ; then
            db_name="${my_properties['clickhouse_green_database_name']}"
        else
            echo "Error : selected_database must be passed as either 'blue' or 'green'. The argument passed was : '$selected_database'" >&2
            return 1
        fi
    fi
    echo "user: ${my_properties['clickhouse_server_username']}" >> "$configured_clickhouse_config_file_path"
    echo "password: ${my_properties['clickhouse_server_password']}" >> "$configured_clickhouse_config_file_path"
    echo "host: ${my_properties['clickhouse_server_host_name']}" >> "$configured_clickhouse_config_file_path"
    echo "port: ${my_properties['clickhouse_server_port']}" >> "$configured_clickhouse_config_file_path"
    echo "database: $db_name" >> "$configured_clickhouse_config_file_path"
    if ! [ "$(cat $configured_clickhouse_config_file_path | wc -l)" == "5" ] ; then
        echo "Error : could not successfully write clickhouse_client config properties to file $configured_clickhouse_config_file_path" >&2
        return 1
    fi
    return 0
}

function initialize_clickhouse_client_command_line_functions() {
    local selected_database=$1
    write_clickhouse_config_file "$selected_database"
}

function shutdown_clickhouse_client_command_line_functions() {
    rm -f "$configured_clickhouse_config_file_path"
    rm -f "$clickhouse_client_database_exists_filepath"
    rm -f "$clickhouse_client_database_table_list_filepath"
    unset configured_clickhouse_config_file_path
    unset sql_data_field_value
    unset sql_data_array
    unset clickhouse_client_database_exists_filepath
    unset clickhouse_client_database_table_list_filepath
}

function execute_sql_statement_via_clickhouse_client() {
    local statement=$1
    local output_filepath=$2
    if [ -e "$output_filepath" ] && ! rm -f "$output_filepath" ; then
        echo "Error : could not overwrite existing output file $output_filepath when executing mysql statment $statement" >&2
    fi
    (
        clickhouse client --config-file="$configured_clickhouse_config_file_path" --format=TabSeparatedWithNames <<< "$statement" > "$output_filepath"
    )
}

function execute_sql_statement_from_file_via_clickhouse_client() {
    local statement_filepath=$1
    local output_filepath=$2
    if [ -e "$output_filepath" ] && ! rm -f "$output_filepath" ; then
        echo "Error : could not overwrite existing output file $output_filepath when executing mysql statments from file $statement_filepath" >&2
    fi
    (
        clickhouse client --config-file="$configured_clickhouse_config_file_path" --format=TabSeparatedWithNames --queries-file="$statement_filepath" > "$output_filepath"
    )
}

function set_clickhouse_sql_data_field_value_from_record() {
    local record_string=$1
    local column_number=$2
    unset sql_data_field_value
    local record_string_length=${#record_string}
    local LF=$'\n'
    local TAB=$'\t'
    local BACKSLASH=$'\\'
    local NULL_MARKER='NULL_CHARACTER_CANNOT_BE_REPRESENTED'
    local BACKSPACE=$'\b'
    local FF=$'\f'
    local CR=$'\r'
    local APOSTROPHE="'"
    local ENCODED_LF='\n'
    local ENCODED_TAB='\t'
    local ENCODED_BACKSLASH='\\'
    local ENCODED_NULL='\0'
    local ENCODED_BACKSPACE='\b'
    local ENCODED_FF='\f'
    local ENCODED_CR='\r'
    local ENCODED_APOSTROPHE="\'"
    local pos=0
    local field_index=0
    local parsed_value=""
    while [ $pos -lt $record_string_length ] ; do
        local character_at_position="${record_string:$pos:1}"
        # a newline should occur at the end of the read line, and only there. Embedded newlines are encoded with '\n'
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
        # a tab character delimits the beginning of a new field, and is not part of the field. Embedded tabs are encoded with '\t'
        if [ "$character_at_position" == "$TAB" ] ; then
            field_index=$((field_index+1))
            if [ "$field_index" -gt "$column_number" ] ; then
                # field has been completely parsed
                sql_data_field_value="$parsed_value"
                return 0
            fi
            pos=$(($pos+1))
            continue
        fi
        # a backslash must begin one of 8 possible escape sequences, all of which are made up of 2 characters : {'\n', '\t', '\\', '\0', '\b', '\f', '\r', "\'"}. No "plain" backslashes should be encountered.
        if [ "$character_at_position" == "$BACKSLASH" ] ; then
            local candidate_escape_string="${record_string:$pos:2}"
            local decoded_character=""
            if [ "$candidate_escape_string" == "$ENCODED_LF" ] ; then
                decoded_character="$LF"
            fi
            if [ "$candidate_escape_string" == "$ENCODED_TAB" ] ; then
                decoded_character="$TAB"
            fi
            if [ "$candidate_escape_string" == "$ENCODED_BACKSLASH" ] ; then
                decoded_character="$BACKSLASH"
            fi
            if [ "$candidate_escape_string" == "$ENCODED_NULL" ] ; then
                decoded_character="$NULL_MARKER"
            fi
            if [ "$candidate_escape_string" == "$ENCODED_BACKSPACE" ] ; then
                decoded_character="$BACKSPACE"
            fi
            if [ "$candidate_escape_string" == "$ENCODED_FF" ] ; then
                decoded_character="$FF"
            fi
            if [ "$candidate_escape_string" == "$ENCODED_CR" ] ; then
                decoded_character="$CR"
            fi
            if [ "$candidate_escape_string" == "$ENCODED_APOSTROPHE" ] ; then
                decoded_character="$APOSTROPHE"
            fi
            # pass over the escape sequence
            pos=$(($pos+2))
            if [ "$field_index" -eq "$column_number" ] ; then
                if [ "$decoded_character" == "$NULL_MARKER" ] ; then
                    echo "Warning : discarding encoded NULL character (\\0) encountered at position $pos while parsing returned database record : $record_string" >&2
                    continue
                fi
                if [ -z "$decoded_character" ] ; then
                    echo "Error : unrecoginzed backslash escape sequence encountered at position $pos while parsing returned database record : $record_string" >&2
                    return 1
                fi
                parsed_value+="$decoded_character"
            fi
            continue
        fi
        # pass over the current (plain) character
        pos=$(($pos+1))
        if [ "$field_index" -eq "$column_number" ] ; then
            parsed_value+="$character_at_position"
        fi
    done
    sql_data_field_value="$parsed_value"
}

function set_clickhouse_sql_data_array_from_file() {
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
            set_clickhouse_sql_data_field_value_from_record "$line" "$column_number"
            sql_data_array+=("$sql_data_field_value")
        fi
    done < "$filepath"
}

function clickhouse_database_exists() {
    local database_name=$1
    local statement="SELECT COUNT(*) FROM system.databases WHERE name = '$database_name'"
    if ! execute_sql_statement_via_clickhouse_client "$statement" "$clickhouse_client_database_exists_filepath" ; then
        echo "Warning : unable to determine if database $database_name exists using : $statement" >&2
        return 1
    fi
    set_clickhouse_sql_data_array_from_file "$clickhouse_client_database_exists_filepath" 0
    if [[ "${sql_data_array[0]}" -ne 1 ]] ; then
        echo "Warning : database $database_name not present on database server, or there are multiple listings for that name" >&2
        return 2
    fi
    return 0
}

function clickhouse_database_is_empty() {
    local database_name=$1
    local statement="SELECT COUNT(*) FROM INFORMATION_SCHEMA.tables WHERE table_schema='$database_name'"
    if ! execute_sql_statement_via_clickhouse_client "$statement" "$clickhouse_client_database_table_list_filepath" ; then
        echo "Warning : unable to retrieve table/view list from database $database_name using : $statement" >&2
        return 1
    fi
    set_clickhouse_sql_data_array_from_file "$clickhouse_client_database_table_list_filepath" 0
    if [[ "${sql_data_array[0]}" -ne 0 ]] ; then
        echo "Warning : database $database_name has tables or views (is not empty as required)" >&2
        return 2
    fi
    return 0
}
