#!/usr/bin/env python3

import argparse
import datetime
import time
import math
import os
import re
import subprocess
import sys

EXPECTED_GENETIC_ALTERATION_INSERT_STATEMENT_START = "INSERT INTO TABLE genetic_alteration_derived"
EXPECTED_GENETIC_ALTERATION_WHERE_FOR_PROFILE_TYPE = "gp.genetic_alteration_type NOT IN ('GENERIC_ASSAY', 'MUTATION_EXTENDED', 'MUTATION_UNCALLED', 'STRUCTURAL_VARIANT')"
EXPECTED_GENERIC_ASSAY_INSERT_STATEMENT_START = "INSERT INTO TABLE generic_assay_data_derived"
EXPECTED_GENERIC_ASSAY_WHERE_FOR_PROFILE_TYPE = "gp.generic_assay_type IS NOT NULL"
GET_GENETIC_PROFILE_ID_LIST_QUERY = "SELECT genetic_profile_id FROM genetic_profile WHERE genetic_alteration_type NOT IN ('GENERIC_ASSAY', 'MUTATION_EXTENDED', 'MUTATION_UNCALLED', 'STRUCTURAL_VARIANT')"
GET_GENERIC_ASSAY_PROFILE_ID_LIST_QUERY = "SELECT genetic_profile_id FROM genetic_profile WHERE generic_assay_type IS NOT NULL"

GET_GENETIC_PROFILE_PROFILE_ID_TO_STUDY_ID_QUERY = "SELECT genetic_profile_id, cancer_study_identifier FROM genetic_profile join cancer_study on genetic_profile.cancer_study_id = cancer_study.cancer_study_id"
GET_GENETIC_PROFILE_SAMPLE_COUNT_QUERY = "SELECT genetic_profile_id, countSubstrings(`ordered_sample_list`, ',') as item_count FROM genetic_profile_samples"
GET_GENETIC_PROFILE_VALUE_LIST_LENGTH_QUERY = "select genetic_profile_id, countSubstrings(`values`,',') as comma_count from genetic_alteration group by genetic_profile_id, comma_count";
EXCLUDE_PROFILES_CLAUSE_STRING = " AND gp.genetic_profile_id not in"

def process_was_successful(status):
    return status == 0

def get_list_of_profile_ids_from_clickhouse(yaml_config_filename, get_profile_id_list_query):
    query_argument = f"--query={get_profile_id_list_query}"
    config_filename_argument = f"--config-file={yaml_config_filename}"
    clickhouse_client_obtain_genetic_profile_id_list = ["clickhouse", "client", config_filename_argument, query_argument]
    profile_query_result = subprocess.run(clickhouse_client_obtain_genetic_profile_id_list, shell=False, capture_output=True)
    profile_id_list_string = profile_query_result.stdout.decode("utf-8")
    profile_id_list = profile_id_list_string.splitlines()
    return profile_id_list, profile_query_result.returncode

def convert_insert_to_be_async(original_insert_sql_statement, expected_where_for_profile_type, profiles_to_be_skipped, max_memory_target):
    one_line_insert_sql_statement = re.sub('\s+', ' ', original_insert_sql_statement)
    if one_line_insert_sql_statement.endswith(" "):
        one_line_insert_sql_statement = one_line_insert_sql_statement.strip()
    if  one_line_insert_sql_statement.endswith(";"):
        one_line_insert_sql_statement = one_line_insert_sql_statement[:-1] 
    exclude_profiles_term = ""
    if len(profiles_to_be_skipped) > 0:
        profiles_to_be_skipped_string = ",".join(profiles_to_be_skipped)
        exclude_profiles_term = f"{EXCLUDE_PROFILES_CLAUSE_STRING} ({profiles_to_be_skipped_string})"
    replacement_string = f"{expected_where_for_profile_type}{exclude_profiles_term}"
    suffix_string = f" SETTINGS async_insert=1, wait_for_async_insert=1, async_insert_busy_timeout_ms=300000, async_insert_max_data_size={max_memory_target}, async_insert_max_query_number=10000;"
    adjusted_insert_statement = one_line_insert_sql_statement.replace(expected_where_for_profile_type, replacement_string) + suffix_string
    return adjusted_insert_statement

def insert_event_records_into_derived_table(
        yaml_config_filename,
        genetic_profile_id_list,
        adjusted_sql_insert_statement):
    query_argument = f"--query={adjusted_sql_insert_statement}"
    config_filename_argument = f"--config-file={yaml_config_filename}"
    clickhouse_client_insert_records = ["clickhouse", "client", config_filename_argument, query_argument]
    insert_events_query_result = subprocess.run(clickhouse_client_insert_records, shell=False, capture_output=True)
    if insert_events_query_result.returncode != 0:
        return insert_events_query_result.returncode
    return 0

def create_arg_parser():
    usage = "usage: %prog derived_table_name yaml_properties_filepath sql_filepath"
    parser = argparse.ArgumentParser(
                    prog='create_derived_tables_in_clickhouse_database_by_profile.py',
                    description='Generates derived clickhouse tables')
    parser.add_argument('derived_table_name')
    parser.add_argument('yaml_config_filename')
    parser.add_argument('sql_filepath')
    parser.add_argument('-m','--max-memory-target', type=int, default=4000000000)
    return parser

def exit_if_args_are_invalid(args):
    if not args.derived_table_name in ('genetic_alteration_derived', 'generic_assay_data_derived'):
        print(f"Argument '{args.derived_table_name}' must be either 'genetic_alteration_derived' or 'generic_assay_data_derived'.", file=sys.stderr)
        sys.exit(1)
    if not os.path.isfile(args.yaml_config_filename):
        print(f"File '{args.yaml_config_filename}' does not exist or is not a file", file=sys.stderr)
        sys.exit(1)
    if not os.path.isfile(args.sql_filepath):
        print(f"File '{args.sql_filepath}' does not exist or is not a file", file=sys.stderr)
        sys.exit(1)

def insert_sql_statement_matches_expectations(original_insert_sql_statement, expected_insert_statement_start, expected_where_for_profile_type):
    # assume start and end are unchanged -- of course there is a risk that it will change (espcially to the end of the query)
    if not original_insert_sql_statement.startswith(expected_insert_statement_start):
        return False
    if not expected_where_for_profile_type in original_insert_sql_statement:
        return False
    return True

def create_sql_insert_statement(derived_table_name, sql_filepath, profiles_to_be_skipped, max_memory_target):
    if derived_table_name == 'genetic_alteration_derived':
        return create_sql_insert_statement_from_clickhouse(sql_filepath, EXPECTED_GENETIC_ALTERATION_INSERT_STATEMENT_START, EXPECTED_GENETIC_ALTERATION_WHERE_FOR_PROFILE_TYPE, profiles_to_be_skipped, max_memory_target)
    if derived_table_name == 'generic_assay_data_derived':
        return create_sql_insert_statement_from_clickhouse(sql_filepath, EXPECTED_GENERIC_ASSAY_INSERT_STATEMENT_START, EXPECTED_GENERIC_ASSAY_WHERE_FOR_PROFILE_TYPE, profiles_to_be_skipped, max_memory_target)
    # We should never get here if args were properly validated
    print(f"Internal Error : derived_table_name argument had unexpected value : {derived_table_name}", file=sys.stderr)
    sys.exit(1)

def create_sql_insert_statement_from_clickhouse(sql_filepath, expected_insert_statement_start, expected_where_for_profile_type, profiles_to_be_skipped, max_memory_target):
    with open(sql_filepath, 'r') as sql_file:
        original_insert_sql_statement = sql_file.read().strip()
        if (not insert_sql_statement_matches_expectations(original_insert_sql_statement, expected_insert_statement_start, expected_where_for_profile_type)):
            print(f"Error: original insert query '{sql_filepath}' does not have the expected format", file=sys.stderr)
            print(f"\tExpected query start is '{expected_insert_statement_start}'", file=sys.stderr)
            print(f"\tExpected query contains '{expected_where_for_profile_type}'", file=sys.stderr)
            sys.exit(1)
    insert_by_profile_statement = convert_insert_to_be_async(original_insert_sql_statement, expected_where_for_profile_type, profiles_to_be_skipped, max_memory_target)
    print(f"executing : {insert_by_profile_statement}")
    return insert_by_profile_statement

def get_list_of_profile_ids(derived_table_name, yaml_config_filename):
    if derived_table_name == 'genetic_alteration_derived':
        return get_list_of_profile_ids_from_clickhouse(yaml_config_filename, GET_GENETIC_PROFILE_ID_LIST_QUERY)
    if derived_table_name == 'generic_assay_data_derived':
        return get_list_of_profile_ids_from_clickhouse(yaml_config_filename, GET_GENERIC_ASSAY_PROFILE_ID_LIST_QUERY)
    # We should never get here if args were properly validated
    print(f"Internal Error : derived_table_name argument had unexpected value : {derived_table_name}", file=sys.stderr)
    sys.exit(1)

def get_profile_id_to_study_id(yaml_config_filename):
    profile_id_to_study_id_map = {}
    query_argument = f"--query={GET_GENETIC_PROFILE_PROFILE_ID_TO_STUDY_ID_QUERY}"
    config_filename_argument = f"--config-file={yaml_config_filename}"
    clickhouse_client_obtain_genetic_profile_stable_id_list = ["clickhouse", "client", config_filename_argument, query_argument]
    profile_query_result = subprocess.run(clickhouse_client_obtain_genetic_profile_stable_id_list, shell=False, capture_output=True)
    if profile_query_result.returncode == 0:
        profile_stable_id_list_string = profile_query_result.stdout.decode("utf-8")
        profile_to_study_id_string_list = profile_stable_id_list_string.splitlines()
        profile_id_to_study_id_pair_list = list(map(lambda s: s.split("\t"), profile_to_study_id_string_list))
        profile_id_to_study_id_map = dict(profile_id_to_study_id_pair_list)
    return profile_id_to_study_id_map, profile_query_result.returncode

def get_profile_id_to_sample_list(yaml_config_filename):
    profile_id_to_sample_count_map = {}
    query_argument = f"--query={GET_GENETIC_PROFILE_SAMPLE_COUNT_QUERY}"
    config_filename_argument = f"--config-file={yaml_config_filename}"
    clickhouse_client_obtain_genetic_profile_sample_count_list = ["clickhouse", "client", config_filename_argument, query_argument]
    profile_query_result = subprocess.run(clickhouse_client_obtain_genetic_profile_sample_count_list, shell=False, capture_output=True)
    if profile_query_result.returncode == 0:
        profile_sample_count_list_string = profile_query_result.stdout.decode("utf-8")
        profile_to_sample_count_string_list = profile_sample_count_list_string.splitlines()
        profile_id_to_sample_count_pair_list = list(map(lambda s: s.split("\t"), profile_to_sample_count_string_list))
        profile_id_to_sample_count_map = dict(profile_id_to_sample_count_pair_list)
    return profile_id_to_sample_count_map, profile_query_result.returncode

def get_profile_id_to_value_list_length(yaml_config_filename):
    profile_id_to_value_list_length_map = {}
    query_argument = f"--query={GET_GENETIC_PROFILE_VALUE_LIST_LENGTH_QUERY}"
    config_filename_argument = f"--config-file={yaml_config_filename}"
    clickhouse_client_obtain_genetic_profile_value_list_length_list = ["clickhouse", "client", config_filename_argument, query_argument]
    profile_query_result = subprocess.run(clickhouse_client_obtain_genetic_profile_value_list_length_list, shell=False, capture_output=True)
    if profile_query_result.returncode == 0:
        profile_value_list_length_list_string = profile_query_result.stdout.decode("utf-8")
        profile_to_value_list_length_string_list = profile_value_list_length_list_string.splitlines()
        # construct list with either the value list length (for consistent/rectangular profiles) or -1 for inconsistent
        profile_id_to_value_list_length_map = {}
        for profile_value_count_pair in profile_to_value_list_length_string_list:
            profile_id_value_count_pair = profile_value_count_pair.split("\t")
            if len(profile_id_value_count_pair) != 2:
                print(f"Error : query with two columns in output could not be parsed. Value encountered : '{profile_value_count_pair}'", file=sys.stderr)
                sys.exit(1)
            profile_id = profile_id_value_count_pair[0]
            if profile_id in profile_id_to_value_list_length_map:
                # new, different value list count encountered for this profile
                profile_id_to_value_list_length_map[profile_id] = -1
            else:
                profile_id_to_value_list_length_map[profile_id] = profile_id_value_count_pair[1]
    return profile_id_to_value_list_length_map, profile_query_result.returncode

def get_profiles_to_be_skipped_list(profile_id_list, profile_id_to_study_id, profile_id_to_sample_list_count, profile_id_to_value_list_length):
    profiles_to_be_skipped = []
    for profile_id in profile_id_list:
        if profile_id not in profile_id_to_sample_list_count:
            # every profile needs a sample list to be defined. This should never happen.
            study_id = profile_id_to_study_id[profile_id]
            print(f"Warning : profile {profile_id} in study {study_id} somehow had no defined sample id list for the profile. Skipping this profile.", file=sys.stderr)
            profiles_to_be_skipped.append(profile_id)
        if profile_id in profile_id_to_value_list_length:
            if profile_id_to_value_list_length[profile_id] == -1:
                study_id = profile_id_to_study_id[profile_id]
                print(f"Warning : profile {profile_id} in study {study_id} has inconsistent value list lengths across entities. Skipping this profile.", file=sys.stderr)
                profiles_to_be_skipped.append(profile_id)
                continue
            if profile_id_to_value_list_length[profile_id] != profile_id_to_sample_list_count[profile_id]:
                study_id = profile_id_to_study_id[profile_id]
                print(f"Warning : profile {profile_id} in study {study_id} has a length of values not equal to the length of samples. Skipping this profile.", file=sys.stderr)
                profiles_to_be_skipped.append(profile_id)
                continue
    return profiles_to_be_skipped

def main():
    parser = create_arg_parser()
    args = parser.parse_args()
    exit_if_args_are_invalid(args)
    print(time.asctime() + " : program start")
    profile_id_list, returncode = get_list_of_profile_ids(args.derived_table_name, args.yaml_config_filename)
    if not process_was_successful(returncode):
        print(f"ERROR: Failed to get list of profile ids.  Return code was '{returncode}'. Please check the properties file containing your database credentials.", file=sys.stderr)
        sys.exit(1)
    print(time.asctime() + " : profile ids fetched")
    profile_id_to_study_id, returncode = get_profile_id_to_study_id(args.yaml_config_filename)
    if not process_was_successful(returncode):
        print(f"ERROR: Failed to get a stable_id for all genetic_alteration profiles.  Return code was '{returncode}'.", file=sys.stderr)
        sys.exit(1)
    print(time.asctime() + " : profile id to study id fetched")
    profile_id_to_sample_list_count, returncode = get_profile_id_to_sample_list(args.yaml_config_filename)
    if not process_was_successful(returncode):
        print(f"ERROR: Failed to get a count of profile samples for all genetic_alteration profiles.  Return code was '{returncode}'.", file=sys.stderr)
        sys.exit(1)
    print(time.asctime() + " : profile id to sample count fetched")
    profile_id_to_value_list_length, returncode = get_profile_id_to_value_list_length(args.yaml_config_filename)
    if not process_was_successful(returncode):
        print(f"ERROR: Failed to get a count of value list items for all genetic_alteration profiles.  Return code was '{returncode}'.", file=sys.stderr)
        sys.exit(1)
    print(time.asctime() + " : profile id to value list length fetched")
    profiles_to_be_skipped_list = get_profiles_to_be_skipped_list(profile_id_list, profile_id_to_study_id, profile_id_to_sample_list_count, profile_id_to_value_list_length)
    adjusted_sql_insert_statement = create_sql_insert_statement(args.derived_table_name, args.sql_filepath, profiles_to_be_skipped_list, args.max_memory_target)
    insert_event_records_into_derived_table(
            args.yaml_config_filename,
            profile_id_list,
            adjusted_sql_insert_statement)

if __name__ == '__main__':
    main()
