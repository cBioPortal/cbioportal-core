#!/usr/bin/env python3

import argparse
import datetime
import time
import math
import os
import re
import subprocess
import sys

ESTIMATED_SAMPLE_ID_LENGTH = 45 # largest average size of sample id in a study currently in genie database
ESTIMATED_HUGO_GENE_SYMBOL_LENGTH = 5 # current average in genie database
ESTIMATED_PROFILE_TYPE_LENGTH = 3 # current average in genie database
ESTIMATED_VALUE_LENGTH = 20 # based on a longish floating point representation such as what might occur in a log-scale expression profile
EXPECTED_GENETIC_ALTERATION_INSERT_STATEMENT_START = "INSERT INTO TABLE genetic_alteration_derived"
EXPECTED_GENETIC_ALTERATION_WHERE_FOR_PROFILE_TYPE = "gp.genetic_alteration_type NOT IN ('GENERIC_ASSAY', 'MUTATION_EXTENDED', 'STRUCTURAL_VARIANT')"
EXPECTED_GENERIC_ASSAY_INSERT_STATEMENT_START = "INSERT INTO TABLE generic_assay_data_derived"
EXPECTED_GENERIC_ASSAY_WHERE_FOR_PROFILE_TYPE = "gp.generic_assay_type IS NOT NULL"
GET_GENETIC_PROFILE_ID_LIST_QUERY = "SELECT genetic_profile_id FROM genetic_profile WHERE genetic_alteration_type NOT IN ('GENERIC_ASSAY', 'MUTATION_EXTENDED', 'STRUCTURAL_VARIANT')"
GET_GENERIC_ASSAY_PROFILE_ID_LIST_QUERY = "SELECT genetic_profile_id FROM genetic_profile WHERE generic_assay_type IS NOT NULL"
GET_GENETIC_PROFILE_PROFILE_ID_TO_STUDY_ID_QUERY = "SELECT genetic_profile_id, cancer_study_identifier FROM genetic_profile join cancer_study on genetic_profile.cancer_study_id = cancer_study.cancer_study_id"
GET_GENETIC_PROFILE_PROFILE_ID_TO_ENTITY_COUNT_QUERY = "SELECT genetic_profile_id, COUNT(*) FROM genetic_alteration GROUP BY genetic_profile_id"
GET_GENETIC_PROFILE_SAMPLE_COUNT_QUERY = "SELECT genetic_profile_id, LENGTH(ordered_sample_list) - LENGTH(REPLACE(ordered_sample_list,',','')) as item_count FROM genetic_profile_samples"
PROFILE_WHERE_REPLACEMENT_STRING = "gp.genetic_profile_id={0} AND sipHash64(ga.genetic_entity_id)%{1}={2}"
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

def insert_event_records_for_profile_into_derived_table(yaml_config_filename, insert_query_by_profile_template, genetic_profile_id, batch_count):
    for batch in range(batch_count):
        print(time.asctime() + " : starting batch " + str(batch) + " of " + str(batch_count) + " of profile " + str(genetic_profile_id))
        if batch > 0:
            print('b', end='')
        insert_events_query = insert_query_by_profile_template.format(genetic_profile_id, batch_count, batch)
        query_argument = f"--query={insert_events_query}"
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
    parser.add_argument('-m','--max-memory-target', type=int)
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
    print("executing : insert_by_profile_statement_template ")
    return insert_by_profile_statement_template

def compute_batch_count(genetic_profile_id, profile_id_to_study_id, profile_id_to_entity_count, profile_id_to_sample_list_count, max_memory_target):
    if not genetic_profile_id in profile_id_to_study_id or not genetic_profile_id in profile_id_to_entity_count or not genetic_profile_id in profile_id_to_sample_list_count:
        return 1 # we should never reach here, but hope for the best with a single batch rather than failing without trying
    record_count = int(profile_id_to_entity_count[genetic_profile_id]) * int(profile_id_to_sample_list_count[genetic_profile_id])
    study_identifier_length = len(profile_id_to_study_id[genetic_profile_id])
    estimated_average_record_size = (
            study_identifier_length * 2 +       # study_id string appears in two fields
            ESTIMATED_SAMPLE_ID_LENGTH +        # sample_id appears in one field
            ESTIMATED_HUGO_GENE_SYMBOL_LENGTH + # hugo_gene_symbol is one field
            ESTIMATED_PROFILE_TYPE_LENGTH +     # profile_type is one field
            ESTIMATED_VALUE_LENGTH )            # alteration_value is one field
    estimated_data_size = record_count * estimated_average_record_size
    batch_count_ratio = float(estimated_data_size) / float(max_memory_target)
    return int(math.ceil(batch_count_ratio))
    
def insert_event_records_for_all_profiles(
        yaml_config_filename,
        genetic_profile_id_list,
        sql_insert_statement_template,
        profile_id_to_study_id,
        profile_id_to_entity_count,
        profile_id_to_sample_list_count,
        max_memory_target):
    successful_profile_count = 0
    for genetic_profile_id in genetic_profile_id_list:
        batch_count = compute_batch_count(genetic_profile_id, profile_id_to_study_id, profile_id_to_entity_count, profile_id_to_sample_list_count, max_memory_target)
        returncode = insert_event_records_for_profile_into_derived_table(yaml_config_filename, sql_insert_statement_template, genetic_profile_id, batch_count)
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

def get_profile_id_to_entity_count(yaml_config_filename):
    profile_id_to_entity_count_map = {}
    query_argument = f"--query={GET_GENETIC_PROFILE_PROFILE_ID_TO_ENTITY_COUNT_QUERY}"
    config_filename_argument = f"--config-file={yaml_config_filename}"
    clickhouse_client_obtain_genetic_profile_entity_count_list = ["clickhouse", "client", config_filename_argument, query_argument]
    profile_query_result = subprocess.run(clickhouse_client_obtain_genetic_profile_entity_count_list, shell=False, capture_output=True)
    if profile_query_result.returncode == 0:
        profile_entity_count_list_string = profile_query_result.stdout.decode("utf-8")
        profile_to_entity_count_string_list = profile_entity_count_list_string.splitlines()
        profile_id_to_entity_count_pair_list = list(map(lambda s: s.split("\t"), profile_to_entity_count_string_list))
        profile_id_to_entity_count_map = dict(profile_id_to_entity_count_pair_list) 
    return profile_id_to_entity_count_map, profile_query_result.returncode

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
    profile_id_to_entity_count, returncode = get_profile_id_to_entity_count(args.yaml_config_filename)
    if not process_was_successful(returncode):
        print(f"ERROR: Failed to get a count of entity profiles for all genetic_alteration profiles.  Return code was '{returncode}'.", file=sys.stderr)
        sys.exit(1)
    print(time.asctime() + " : profile id to entity count fetched")
    profile_id_to_sample_list_count, returncode = get_profile_id_to_sample_list(args.yaml_config_filename)
    if not process_was_successful(returncode):
        print(f"ERROR: Failed to get a count of profile samples for all genetic_alteration profiles.  Return code was '{returncode}'.", file=sys.stderr)
        sys.exit(1)
    print(time.asctime() + " : profile id to sample count fetched")
    sql_insert_statement_template = create_sql_insert_statement_template(args.derived_table_name, args.sql_filepath)
    insert_event_records_for_all_profiles(
            args.yaml_config_filename,
            profile_id_list,
            sql_insert_statement_template,
            profile_id_to_study_id,
            profile_id_to_entity_count,
            profile_id_to_sample_list_count,
            args.max_memory_target)

if __name__ == '__main__':
    main()
