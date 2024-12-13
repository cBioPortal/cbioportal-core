#!/usr/bin/env python3

import argparse
import datetime
import os
import re
import subprocess
import sys

EXPECTED_GENETIC_ALTERATION_INSERT_STATEMENT_START = "INSERT INTO TABLE genetic_alteration_derived"
EXPECTED_GENETIC_ALTERATION_WHERE_FOR_PROFILE_TYPE = "gp.genetic_alteration_type NOT IN ('GENERIC_ASSAY', 'MUTATION_EXTENDED', 'STRUCTURAL_VARIANT')"
EXPECTED_GENERIC_ASSAY_INSERT_STATEMENT_START = "INSERT INTO TABLE generic_assay_data_derived"
EXPECTED_GENERIC_ASSAY_WHERE_FOR_PROFILE_TYPE = "gp.generic_assay_type IS NOT NULL"
GET_GENETIC_PROFILE_ID_LIST_QUERY = "SELECT genetic_profile_id FROM genetic_profile WHERE genetic_alteration_type NOT IN ('GENERIC_ASSAY', 'MUTATION_EXTENDED', 'STRUCTURAL_VARIANT')"
GET_GENERIC_ASSAY_PROFILE_ID_LIST_QUERY = "SELECT genetic_profile_id FROM genetic_profile WHERE generic_assay_type IS NOT NULL"
PROFILE_WHERE_REPLACEMENT_STRING = "gp.genetic_profile_id={0}"
GET_FAILED_PROFILE_STUDY_ID_QUERY = "SELECT genetic_profile.stable_id, cancer_study.cancer_study_identifier FROM genetic_profile JOIN cancer_study ON genetic_profile.cancer_study_id=cancer_study.cancer_study_id WHERE genetic_profile.genetic_profile_id={0}"

def process_was_successful(status):
    return status == 0

def get_failed_profile_study_id_info(yaml_config_filename, profile_id):
    failed_profile_query = GET_FAILED_PROFILE_STUDY_ID_QUERY.format(profile_id)
    query_argument = f"--query={failed_profile_query}"
    config_filename_argument = f"--config-file={yaml_config_filename}"
    clickhouse_client_obtain_failed_profile_name = ["clickhouse", "client", config_filename_argument, query_argument]
    failed_profile_query_result = subprocess.run(clickhouse_client_obtain_failed_profile_name, shell=False, capture_output=True)
    failed_profile_result_string = failed_profile_query_result.stdout.decode("utf-8")
    result_list = failed_profile_result_string.split()
    return result_list[0], result_list[1], failed_profile_query_result.returncode

def get_list_of_profile_ids_from_clickhouse(yaml_config_filename, get_profile_id_list_query):
    query_argument = f"--query={get_profile_id_list_query}"
    config_filename_argument = f"--config-file={yaml_config_filename}"
    clickhouse_client_obtain_genetic_profile_id_list = ["clickhouse", "client", config_filename_argument, query_argument]
    profile_query_result = subprocess.run(clickhouse_client_obtain_genetic_profile_id_list, shell=False, capture_output=True)
    profile_id_list_string = profile_query_result.stdout.decode("utf-8")
    profile_id_list = profile_id_list_string.splitlines()
    return profile_id_list, profile_query_result.returncode

def convert_insert_to_be_by_genetic_profile(original_insert_sql_statement, expected_where_for_profile_type):
    insert_query_by_profile_template = original_insert_sql_statement.replace(expected_where_for_profile_type, PROFILE_WHERE_REPLACEMENT_STRING)
    return re.sub('\s+', ' ', insert_query_by_profile_template)

def insert_event_records_for_profile_into_derived_table(yaml_config_filename, insert_query_by_profile_template, genetic_profile_id):
    insert_events_query = insert_query_by_profile_template.format(genetic_profile_id)
    query_argument = f"--query={insert_events_query}"
    config_filename_argument = f"--config-file={yaml_config_filename}" 
    clickhouse_client_insert_records = ["clickhouse", "client", config_filename_argument, query_argument]
    insert_events_query_result = subprocess.run(clickhouse_client_insert_records, shell=False, capture_output=True)
    return insert_events_query_result.returncode

def create_arg_parser():
    usage = "usage: %prog derived_table_name yaml_properties_filepath sql_filepath"
    parser = argparse.ArgumentParser(
                    prog='create_derived_tables_in_clickhouse_database_by_profile.py',
                    description='Generates derived clickhouse tables')
    parser.add_argument('derived_table_name')
    parser.add_argument('yaml_config_filename')
    parser.add_argument('sql_filepath')
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

def create_sql_insert_statement_template(derived_table_name, sql_filepath):
    if derived_table_name == 'genetic_alteration_derived':
        return create_sql_insert_statement_template_from_clickhouse(sql_filepath, EXPECTED_GENETIC_ALTERATION_INSERT_STATEMENT_START, EXPECTED_GENETIC_ALTERATION_WHERE_FOR_PROFILE_TYPE)
    if derived_table_name == 'generic_assay_data_derived':
        return create_sql_insert_statement_template_from_clickhouse(sql_filepath, EXPECTED_GENERIC_ASSAY_INSERT_STATEMENT_START, EXPECTED_GENERIC_ASSAY_WHERE_FOR_PROFILE_TYPE)
    # We should never get here if args were properly validated
    print(f"Internal Error : derived_table_name argument had unexpected value : {derived_table_name}", file=sys.stderr)
    sys.exit(1)

def create_sql_insert_statement_template_from_clickhouse(sql_filepath, expected_insert_statement_start, expected_where_for_profile_type):
    with open(sql_filepath, 'r') as sql_file:
        original_insert_sql_statement = sql_file.read().strip()
        if (not insert_sql_statement_matches_expectations(original_insert_sql_statement, expected_insert_statement_start, expected_where_for_profile_type)):
            print(f"Error: original insert query '{sql_filepath}' does not have the expected format", file=sys.stderr)
            print(f"\tExpected query start is '{expected_insert_statement_start}'", file=sys.stderr)
            print(f"\tExpected query contains '{expected_where_for_profile_type}'", file=sys.stderr)
            sys.exit(1)
    insert_by_profile_statement_template = convert_insert_to_be_by_genetic_profile(original_insert_sql_statement, expected_where_for_profile_type)
    return insert_by_profile_statement_template

def insert_event_records_for_all_profiles(yaml_config_filename, genetic_profile_id_list, sql_insert_statement_template):
    successful_profile_count = 0
    for genetic_profile_id in genetic_profile_id_list:
        returncode = insert_event_records_for_profile_into_derived_table(yaml_config_filename, sql_insert_statement_template, genetic_profile_id)
        if returncode != 0:
            profile_stable_id, cancer_study_identifier, select_returncode = get_failed_profile_study_id_info(yaml_config_filename, genetic_profile_id)
            if select_returncode != 0:
                print("WARNING: Error occurred during insertion of record for profile id '{genetic_profile_id}'. We could not retrieve the cancer_study_identifier. The derived table records for this profile are missing or incomplete.", file=sys.stderr) 
            else:
                print(f"WARNING: Error occurred during insertion of record for profile '{profile_stable_id}' in study '{cancer_study_identifier}'. The derived table records for this profile are missing or incomplete.", file=sys.stderr)
        else:
            print('.', end='')
            sys.stdout.flush()
            successful_profile_count += 1
    print(f"\nSuccessfully processed {successful_profile_count} out of {len(genetic_profile_id_list)} profiles.")

def get_list_of_profile_ids(derived_table_name, yaml_config_filename):
    if derived_table_name == 'genetic_alteration_derived':
        return get_list_of_profile_ids_from_clickhouse(yaml_config_filename, GET_GENETIC_PROFILE_ID_LIST_QUERY)
    if derived_table_name == 'generic_assay_data_derived':
        return get_list_of_profile_ids_from_clickhouse(yaml_config_filename, GET_GENERIC_ASSAY_PROFILE_ID_LIST_QUERY)
    # We should never get here if args were properly validated
    print(f"Internal Error : derived_table_name argument had unexpected value : {derived_table_name}", file=sys.stderr)
    sys.exit(1)

def main():
    parser = create_arg_parser()
    args = parser.parse_args()
    exit_if_args_are_invalid(args)
    profile_id_list, returncode = get_list_of_profile_ids(args.derived_table_name, args.yaml_config_filename)
    if not process_was_successful(returncode):
        print(f"ERROR: Failed to get list of profile ids.  Return code was '{returncode}'. Please check the properties file containing your database credentials.")
        sys.exit(1)
    sql_insert_statement_template = create_sql_insert_statement_template(args.derived_table_name, args.sql_filepath)
    insert_event_records_for_all_profiles(args.yaml_config_filename, profile_id_list, sql_insert_statement_template)

if __name__ == '__main__':
    main()
