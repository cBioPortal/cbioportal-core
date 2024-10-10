#!/usr/bin/env bash

unset configured_mysql_defaults_config_file_path
unset sql_data_field_value
unset sql_data_array
configured_mysql_defaults_config_file_path=""
sql_data_field_value=""
declare -a sql_data_array
database_exists_filepath="$(pwd)/mclf_database_exists.txt"
table_exists_filepath="$(pwd)/mclf_table_exists.txt"
database_table_list_filepath="$(pwd)/mclf_database_table_list.txt"

function purge_mysql_credentials_from_environment_variables() {
    unset my_properties['mysql_server_username']
    unset my_properties['mysql_server_password']
    unset my_properties['mysql_server_host_name']
}

function write_mysql_defaults_config_file() {
    configured_mysql_defaults_config_file_path="$(pwd)/mclf_mysql_defaults_$(date "+%Y-%m-%d-%H-%M-%S").cnf"
    echo "[client]" > "$configured_mysql_defaults_config_file_path"
    echo "user=\"${my_properties['mysql_server_username']}\"" >> "$configured_mysql_defaults_config_file_path"
    echo "password=\"${my_properties['mysql_server_password']}\"" >> "$configured_mysql_defaults_config_file_path"
    echo "host=\"${my_properties['mysql_server_host_name']}\"" >> "$configured_mysql_defaults_config_file_path"
    # once written to the configuration file, drop the credential from the environment for security
    purge_mysql_credentials_from_environment_variables
    if ! [ "$(cat $configured_mysql_defaults_config_file_path | wc -l)" == "4" ] ; then
        echo "Error : could not successfully write default mysql properties to file $configured_mysql_defaults_config_file_path" >&2
        return 1
    fi
    return 0
}

function initialize_mysql_command_line_functions() {
    write_mysql_defaults_config_file
}

function shutdown_mysql_command_line_functions() {
    rm -f "$configured_mysql_defaults_config_file_path"
    rm -f "$database_exists_filepath"
    rm -f "$table_exists_filepath"
    rm -f "$database_table_list_filepath"
    unset configured_mysql_defaults_config_file_path
    unset sql_data_field_value
    unset sql_data_array
    unset database_exists_filepath
    unset table_exists_filepath
    unset database_table_list_filepath
}

function execute_sql_statement_via_mysql() {
    local statement=$1
    local output_filepath=$2
    if [ -e "$output_filepath" ] && ! rm -f "$output_filepath" ; then
        echo "Error : could not overwrite existing output file $output_filepath when executing mysql statment $statement" >&2
    fi
    local extra_args="${my_properties['mysql_server_additional_args']}"
    mysql --defaults-extra-file="$configured_mysql_defaults_config_file_path" --batch $extra_args <<< "$statement" > $output_filepath 
}

function set_sql_data_field_value_from_record() {
    local record_string=$1
    local column_number=$2
    unset sql_data_field_value
    local record_string_length=${#record_string}
    local LF=$'\n'
    local TAB=$'\t'
    local BACKSLASH=$'\\'
    local NULL_MARKER='NULL_CHARACTER_CANNOT_BE_REPRESENTED'
    local ENCODED_LF='\n'
    local ENCODED_TAB='\t'
    local ENCODED_BACKSLASH='\\'
    local ENCODED_NULL='\0'
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
        # a backslash must begin one of 4 possible escape sequences, all of which are made up of 2 characters : {'\n', '\t', '\\', '\0'}. No "plain" backslashes should be encountered.
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

function set_sql_data_array_from_file() {
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
            set_sql_data_field_value_from_record "$line" "$column_number"
            sql_data_array+=("$sql_data_field_value")
        fi
    done < "$filepath"
}

function database_exists() {
    local database_name=$1
    local statement="SHOW DATABASES LIKE '$database_name'"
    if ! execute_sql_statement_via_mysql "$statement" "$database_exists_filepath" ; then
        echo "Warning : unable to determine if database $database_name exists using : $statement" >&2
        return 1
    fi
    set_sql_data_array_from_file "$database_exists_filepath" 0
    if [[ "${#sql_data_array[@]}" -ne 1 ]] ; then
        echo "Warning : database $database_name not present on database server, or there are multiple listings for that name" >&2
        return 2
    fi
    return 0
}

function table_exists() {
    local database_name=$1
    local table_name=$2
    local statement="DESCRIBE TABLE \`$database_name\`.\`$table_name\`"
    if ! execute_sql_statement_via_mysql "$statement" "$table_exists_filepath" ; then
        echo "Warning : unable to find table $table_name in  $database_name using : $statement" >&2
        return 1
    fi
    return 0
}

function database_is_empty() {
    local database_name=$1
    local statement="SHOW TABLES IN \`$database_name\`"
    if ! execute_sql_statement_via_mysql "$statement" "$database_table_list_filepath" ; then
        echo "Warning : unable to retrieve table list from database $database_name using : $statement" >&2
        return 1
    fi
    set_sql_data_array_from_file "$database_table_list_filepath" 0
    if [[ "${#sql_data_array[@]}" -ne 0 ]] ; then
        echo "Warning : database $database_name has tables (is not empty as required)" >&2
        return 2
    fi
    return 0
}
