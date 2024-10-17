#!/usr/bin/env python3

import datetime
import re
import subprocess

def get_list_of_genetic_profile_ids():
    get_genetic_profile_id_list_query="SELECT genetic_profile_id FROM genetic_profile WHERE genetic_alteration_type NOT IN ('GENERIC_ASSAY', 'MUTATION_EXTENDED', 'STRUCTURAL_VARIANT')"
    query_argument_template="--query={0}"
    query_argument = query_argument_template.format(get_genetic_profile_id_list_query)
    clickhouse_client_obtain_genetic_profile_id_list = ["clickhouse", "client", "--config-file=clickhouse_client_config_2024-10-14-09-03-02.yaml", query_argument]
    #TODO remove hardcode of clickhouse config file and accept (or generate) this file based on command like arguments
    genetic_profile_query_result = subprocess.run(clickhouse_client_obtain_genetic_profile_id_list, shell=False, capture_output=True)#, stderr=stderr_file, stdout=stdout_file)
    genetic_profile_id_list_string = genetic_profile_query_result.stdout.decode("utf-8")
    genetic_profile_id_list = genetic_profile_id_list_string.splitlines()
    return genetic_profile_id_list, genetic_profile_query_result.returncode

#TODO read SQL statement templates from external files rather than hardcoding
INSERT_EVENTS_INTO_GENETIC_ALTERATION_DERIVED_QUERY_TEMPLATE = '''
INSERT INTO TABLE genetic_alteration_derived
    SELECT
        sample_unique_id,
        cancer_study_identifier,
        hugo_gene_symbol,
        replaceOne(stable_id, concat(sd.cancer_study_identifier, '_'), '') as profile_type,
        alteration_value
    FROM
        (SELECT
            sample_id,
            hugo_gene_symbol,
            stable_id,
            alteration_value
        FROM
            (SELECT
                g.hugo_gene_symbol AS hugo_gene_symbol,
                gp.stable_id as stable_id,
                arrayMap(x -> (x = '' ? NULL : x), splitByString(',', assumeNotNull(substring(ga.values,1,-1)))) AS alteration_value,
                arrayMap(x -> (x = '' ? NULL : toInt32(x)), splitByString(',', assumeNotNull(substring(gps.ordered_sample_list,1,-1)))) AS sample_id
            FROM
                genetic_profile gp
                JOIN genetic_profile_samples gps ON gp.genetic_profile_id = gps.genetic_profile_id
                JOIN genetic_alteration ga ON gp.genetic_profile_id = ga.genetic_profile_id
                JOIN gene g ON ga.genetic_entity_id = g.genetic_entity_id
            WHERE
                gp.genetic_profile_id={0})
            ARRAY JOIN alteration_value, sample_id
        WHERE alteration_value != 'NA') AS subquery
        JOIN sample_derived sd ON sd.internal_id = subquery.sample_id'''
INSERT_EVENTS_INTO_GENETIC_ALTERATION_DERIVED_QUERY_TEMPLATE_1LINE = re.sub('\s+',' ', INSERT_EVENTS_INTO_GENETIC_ALTERATION_DERIVED_QUERY_TEMPLATE).strip()

#TODO add generic assay query too and allow selection of which to run by command line argument

def insert_event_records_for_profile_into_derived_table(derived_table_name, genetic_profile_id):
    insert_events_query = INSERT_EVENTS_INTO_GENETIC_ALTERATION_DERIVED_QUERY_TEMPLATE_1LINE.format(genetic_profile_id)
    query_argument_template="--query={0}"
    query_argument = query_argument_template.format(insert_events_query)
    clickhouse_client_insert_records = ["clickhouse", "client", "--config-file=clickhouse_client_config_2024-10-14-09-03-02.yaml", query_argument]
    insert_events_query_result = subprocess.run(clickhouse_client_insert_records, shell=False, capture_output=True)#, stderr=stderr_file, stdout=stdout_file)
    print("stderr from insert:")
    print(insert_events_query_result.stderr.decode("utf-8"))
    return insert_events_query_result.returncode

def main():
    print(datetime.datetime.now())
    genetic_profile_id_list, returncode = get_list_of_genetic_profile_ids()
    print(genetic_profile_id_list)
    print("return code was {0}".format(returncode))
    for genetic_profile_id in genetic_profile_id_list:
        print(datetime.datetime.now())
        returncode = insert_event_records_for_profile_into_derived_table("genetic_alteration", genetic_profile_id)
        if returncode != 0:
            print("Error occurred during insertion of record for profile {0}".format(genetic_profile_id))
            #TODO add a step where genetic profiles are mapped to cancer study stable id, and print that instead
    print(datetime.datetime.now())
    print("stop")

if __name__ == '__main__':
    main()
