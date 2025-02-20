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
create_derived_tables_by_profile_script_filepath="${this_script_dir}/create_derived_tables_in_clickhouse_database_by_profile.py"
if ! [ -r ${create_derived_tables_by_profile_script_filepath} ] ; then
    echo "Error : unable to read/find required python script at this location: $create_derived_tables_by_profile_script_filepath" >&2
    exit 1
fi
unset this_script_dir

# non-local environment variables in use
unset my_properties
unset database_name
unset properties_filepath
unset database_to_create_derived_tables_in
unset clickhouse_max_memory_use_target
unset derived_table_composite_sql_filepaths
unset derived_table_simple_sql_filepaths
unset sql_tmpdir_path
declare -A my_properties
database_name=""
properties_filepath=""
database_to_create_derived_tables_in=""
clickhouse_max_memory_use_target=""
declare -a derived_table_composite_sql_filepaths
declare -a derived_table_simple_sql_filepaths
DERIVED_TABLE_SIMPLE_FILENAME_PREFIX="derived_table_sql_statement"
create_derived_table_result_filepath="$(pwd)/cdtcd_create_derived_tables_result.txt"
clickhouse_is_responsive_filepath="$(pwd)/cdtcd_cmd_clickhouse_is_responsive.txt"
SECONDS_BETWEEN_RESPONSIVENESS_RETRY=$((60))
zero_padded_string=""
sql_tmpdir_path=""
EXPECTED_GENETIC_ALTERATION_INSERT_STATEMENT_START="INSERT INTO TABLE genetic_alteration_derived"
EXPECTED_GENERIC_ASSAY_INSERT_STATEMENT_START="INSERT INTO TABLE generic_assay_data_derived"

function usage() {
    echo "usage: create_derived_tables_in_clickhouse_database.sh properties_filepath [database] create_derived_sql_filepath_1..." >&2
    echo "         database should be omitted for single-database installations, or else must be in {blue, green} for dual-database installations" >&2
    echo "         create_derived_sql_filepath_1... is one or more file paths to composite sql files for creating derived tables in clickhouse" >&2
}

function initialize_main() {
    if ! parse_property_file "$properties_filepath" my_properties ; then
        usage
        return 1
    fi
    if ! initialize_clickhouse_client_command_line_functions "$database_to_create_derived_tables_in" ; then
        usage
        return 1
    fi
    remove_credentials_from_properties my_properties # no longer needed - remove for security
    clickhouse_max_memory_use_target="${my_properties['clickhouse_max_memory_use_target']}"
    if [ -z "$database_to_create_derived_tables_in" ] ; then
        database_name="${my_properties['clickhouse_database_name']}"
    else
        if [ "$database_to_create_derived_tables_in" == "blue" ] ; then
            database_name="${my_properties['clickhouse_blue_database_name']}"
        else
            if [ "$database_to_create_derived_tables_in" == "green" ] ; then
                database_name="${my_properties['clickhouse_green_database_name']}"
            else
                echo "Error : when provided, database must be one of {blue, green}" >&2
                usage
                return 1
            fi
        fi
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

function selected_database_exists() {
    if ! clickhouse_database_exists "$database_name" ; then
        echo "Error : could not proceed with creation of derived tables because database does not exist: $database_name" >&2
        return 1
    fi
    return 0
}

function set_zero_padded_string() {
    string=$1
    field_width=$2
    if [ ${#string} -ge $field_width ] ; then
        zero_padded_string="$string"
        return 0
    fi
    pad=""
    local total_length=${#string}
    while [ $total_length -lt $field_width ] ; do
        pad="0$pad"
        total_length=$(($total_length+1))
    done
    zero_padded_string="$pad""$string"
    return 0
}

function reformat_composite_sql_files_to_simple_sql_files() {
    local semicolon_count=0
    local pos=0
    while [ $pos -lt ${#derived_table_composite_sql_filepaths[@]} ] ; do
        local inputfile="${derived_table_composite_sql_filepaths[$pos]}"
        local file_semicolon_count=$( cat $inputfile | grep -c ';' )
        semicolon_count=$(($semicolon_count+$file_semicolon_count))
        pos=$(($pos+1))
    done
    output_filecount_field_width=${#semicolon_count}

    local MULTI_SEMICOLON_RE=".*;.*;.*"
    local EARLY_SEMICOLON_RE=".*;.*[[:graph:]]"
    local HAS_SEMICOLON_RE=".*;.*"
    local output_file_index=1
    local pos=0
    set_zero_padded_string "$output_file_index" "$output_filecount_field_width"
    local outputfile="${sql_tmpdir_path}/${DERIVED_TABLE_SIMPLE_FILENAME_PREFIX}_${zero_padded_string}.sql"
    rm -f "$outputfile"
    while [ $pos -lt ${#derived_table_composite_sql_filepaths[@]} ] ; do
        inputfile="${derived_table_composite_sql_filepaths[$pos]}"
        IFS=''; while read line ; do
            if [[ $line =~ $MULTI_SEMICOLON_RE ]] ; then
                echo "Error : line encountered in file $inputfile with multiple semicolons (unparsable) : $line" >&2
                exit 1
            fi
            if [[ $line =~ $EARLY_SEMICOLON_RE ]] ; then
                echo "Error : line encountered in file $inputfile with content after the semicolon (unparsable) : $line" >&2
                exit 1
            fi
            echo "$line" >> "$outputfile"
            if [[ $line =~ $HAS_SEMICOLON_RE ]] ; then
                # this is the final line of the output file
                derived_table_simple_sql_filepaths+=("$outputfile")
                output_file_index=$(($output_file_index+1))
                set_zero_padded_string "$output_file_index" "$output_filecount_field_width"
                outputfile="${sql_tmpdir_path}/${DERIVED_TABLE_SIMPLE_FILENAME_PREFIX}_${zero_padded_string}.sql"
                rm -f "$outputfile"
            fi
        done < "$inputfile"
        if [ -e "$outputfile" ] && ! [ -s "$outputfile" ] ; then
            # if we have written anything to the current output file, it is now done (even without a terminating semicolon)
            derived_table_simple_sql_filepaths+=("$outputfile")
            output_file_index=$(($output_file_index+1))
            set_zero_padded_string "$output_file_index" "$output_filecount_field_width"
            outputfile="${sql_tmpdir_path}/${DERIVED_TABLE_SIMPLE_FILENAME_PREFIX}_${zero_padded_string}.sql"
            rm -f "$outputfile"
        fi
        pos=$(($pos+1))
    done
}

function create_sql_tmpdir_and_set_sql_tmpdir_path() {
    date_suffix="-$(date +%Y-%m-%d-%H-%M-%S)"
    sql_tmpdir_path="$(pwd)/sql_tmpdir$date_suffix"
    if ! mkdir "$sql_tmpdir_path" ; then
        return 1
    fi
    return 0
}

function prepare_simple_sql_files() {
    if ! create_sql_tmpdir_and_set_sql_tmpdir_path ; then
        echo "Error : could not create tmpdir for sql : $sql_tmpdir_path" >&2
        return 1
    fi
    if ! reformat_composite_sql_files_to_simple_sql_files ; then
        echo "Error : could not convert composite sql files into sample sql files in $sql_tmpdir_path" >&2
        return 1
    fi
}

function simple_sql_file_inserts_into_genetic_alteration_derived() {
    sql_filepath="$1"
    if grep -q "$EXPECTED_GENETIC_ALTERATION_INSERT_STATEMENT_START" "$sql_filepath"; then
        return 0
    fi
    return 1
}

function simple_sql_file_inserts_into_generic_assay_data_derived() {
    sql_filepath="$1"
    if grep -q "$EXPECTED_GENERIC_ASSAY_INSERT_STATEMENT_START" "$sql_filepath"; then
        return 0
    fi
    return 1
}

function process_genetic_alteration_insertion_per_profile() {
    sql_filepath="$1"
    (
        ${create_derived_tables_by_profile_script_filepath} genetic_alteration_derived "$configured_clickhouse_config_file_path" "$sql_filepath" "--max-memory-target=$clickhouse_max_memory_use_target"
    ) 
}

function process_generic_assay_data_insertion_per_profile() {
    sql_filepath="$1"
    (
        ${create_derived_tables_by_profile_script_filepath} generic_assay_data_derived "$configured_clickhouse_config_file_path" "$sql_filepath" "--max-memory-target=$clickhouse_max_memory_use_target"
    )
}

function create_all_derived_tables() {
    local pos=0
    while [ $pos -lt ${#derived_table_simple_sql_filepaths[@]} ] ; do
        sql_filepath="${derived_table_simple_sql_filepaths[$pos]}"
        if simple_sql_file_inserts_into_genetic_alteration_derived "$sql_filepath" ; then
            if ! process_genetic_alteration_insertion_per_profile "$sql_filepath" ; then
                echo "Error : could not process genetic alteration insertions per profile" >&2
                return 1
            fi
            pos=$(($pos+1))
            continue
        fi
        if simple_sql_file_inserts_into_generic_assay_data_derived "$sql_filepath" ; then
            if ! process_generic_assay_data_insertion_per_profile "$sql_filepath" ; then
                echo "Error : could not process generic assay data insertions per profile" >&2
                return 1
            fi
            pos=$(($pos+1))
            continue
        fi
        if ! execute_sql_statement_from_file_via_clickhouse_client "$sql_filepath" "create_derived_table_result_filepath" ; then
            echo "Error : failure occurred during execution of sql statements in file $sql_filepath" >&2
            echo "    beginning with:" >&2
            head -n 4 $sql_filepath >&2
            return 1
        fi
        pos=$(($pos+1))
    done
    return 0
}

function delete_sql_tmpdir() {
    if [[ -d "$sql_tmpdir_path" && "$sql_tmpdir_path" != "/" ]]; then
        rm -rf "$sql_tmpdir_path"
    fi
}

function delete_output_stream_files() {
    rm -f "$create_derived_table_result_filepath"
    rm -f "$clickhouse_is_responsive_filepath"
}

function shutdown_main_and_clean_up() {
    shutdown_clickhouse_client_command_line_functions
    delete_sql_tmpdir
    delete_output_stream_files
    unset my_properties
    unset database_name
    unset create_derived_table_result_filepath
    unset clickhouse_max_memory_use_target
    unset clickhouse_is_responsive_filepath
    unset SECONDS_BETWEEN_RESPONSIVENESS_RETRY
    unset properties_filepath
    unset database_to_create_derived_tables_in
    unset derived_table_composite_sql_filepaths
    unset derived_table_simple_sql_filepaths
    unset DERIVED_TABLE_SIMPLE_FILENAME_PREFIX
    unset zero_padded_string
    unset create_derived_tables_by_profile_script_filepath
}

function main() {
    local exit_status=0
    properties_filepath=$1
    shift 1 # remove the properties_filepath argument from the argument list
    database_to_create_derived_tables_in=$1
    if [ "$database_to_create_derived_tables_in" == "blue" ] || [ "$database_to_create_derived_tables_in" == "green" ] ; then
        shift 1 # recognized value selecting between blue and green databases, remove it from the argument list
    else
        database_to_create_derived_tables_in="" # not recognized, so blank out the color selection and do not remove the arguemnt from the list
    fi
    derived_table_composite_sql_filepaths=()
    while [ $# -gt 0 ] ; do
        derived_table_composite_sql_filepaths+=($1)
        shift 1
    done
    if [ ${#derived_table_composite_sql_filepaths[@]} -lt 1 ] ; then
        # at least 1 sql file must have been provided (in addition to the other arguments)
        usage
        exit 1
    fi
    if ! initialize_main ||
            ! clickhouse_is_responding ||
            ! selected_database_exists ||
            ! prepare_simple_sql_files ||
            ! create_all_derived_tables ; then
        exit_status=1
    fi
    shutdown_main_and_clean_up
    return $exit_status
}

main "$@"
