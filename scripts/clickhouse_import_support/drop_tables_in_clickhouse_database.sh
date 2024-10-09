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
unset this_script_dir

# non-local environment variables in use
unset my_properties
unset database_table_list
unset database_name
declare -A my_properties
declare -a database_table_list
database_name=""
database_table_list_filepath="$(pwd)/dtcd_database_table_list.txt"
drop_table_result_filepath="$(pwd)/dtcd_drop_table_result.txt"

function usage() {
    echo "usage: drop_tables_in_clickhouse_database.sh properties_filepath database" >&2
    echo "         database must be in {blue, green}" >&2
}

function initialize_main() {
    local properties_filepath=$1
    local database_to_drop_tables_from=$2
    if ! parse_property_file "$properties_filepath" my_properties ; then
        usage
        return 1
    fi
    if ! initialize_clickhouse_client_command_line_functions ; then
        usage
        return 1
    fi
    remove_credentials_from_properties my_properties # no longer needed - remove for security
    if [ "$database_to_drop_tables_from" == "blue" ] ; then
        database_name="${my_properties['clickhouse_blue_database_name']}"
    else
        if [ "$database_to_drop_tables_from" == "green" ] ; then
            database_name="${my_properties['clickhouse_green_database_name']}"
        else
            echo "Error : database must be one of {blue, green}" >&2
            usage
            return 1
        fi
    fi
    return 0
}

DESTINATION_DATABASE="name_of_clickhouse_blue_database"
read -p 'enter clickhouse password: ' password
echo "password was $password"

#TODO : read the table name list out of clickhouse and include any constructed views
table_name=()
table_name+=('allele_specific_copy_number')
table_name+=('alteration_driver_annotation')
table_name+=('authorities')
table_name+=('cancer_study')
table_name+=('cancer_study_tags')
table_name+=('clinical_attribute_meta')
table_name+=('clinical_event')
table_name+=('clinical_event_data')
table_name+=('clinical_patient')
table_name+=('clinical_sample')
table_name+=('cna_event')
table_name+=('copy_number_seg')
table_name+=('copy_number_seg_file')
table_name+=('cosmic_mutation')
table_name+=('data_access_tokens')
table_name+=('fraction_genome_altered')
table_name+=('gene')
table_name+=('gene_alias')
table_name+=('gene_panel')
table_name+=('gene_panel_list')
table_name+=('generic_entity_properties')
table_name+=('geneset')
table_name+=('geneset_gene')
table_name+=('geneset_hierarchy_leaf')
table_name+=('geneset_hierarchy_node')
table_name+=('genetic_alteration')
table_name+=('genetic_entity')
table_name+=('genetic_profile')
table_name+=('genetic_profile_link')
table_name+=('genetic_profile_samples')
table_name+=('gistic')
table_name+=('gistic_to_gene')
table_name+=('info')
table_name+=('mut_sig')
table_name+=('mutation')
table_name+=('mutation_count')
table_name+=('mutation_count_by_keyword')
table_name+=('mutation_event')
table_name+=('patient')
table_name+=('reference_genome')
table_name+=('reference_genome_gene')
table_name+=('resource_definition')
table_name+=('resource_patient')
table_name+=('resource_sample')
table_name+=('resource_study')
table_name+=('sample')
table_name+=('sample_cna_event')
table_name+=('sample_list')
table_name+=('sample_list_list')
table_name+=('sample_profile')
table_name+=('structural_variant')
table_name+=('type_of_cancer')
table_name+=('users')
table_name+=('sample_to_gene_panel_derived')
table_name+=('gene_panel_to_gene_derived')
table_name+=('sample_derived')
table_name+=('genomic_event_derived')
table_name+=('clinical_data_derived')
table_name+=('clinical_event_derived')
table_name+=('genetic_alteration_cna_derived')
table_name+=('genetic_alteration_numerical_derived')
table_name+=('generic_assay_data_derived')
table_name+=('sample_list_columnstore')
#TODO a separate command is needed for dropping views, so make sure to keep the table list and the view list distinct (read both out of the clickhouse service)
#view_name+=('sample_list_columnstore_mv")
pos=0
while [ $pos -lt 63 ] ; do
    clickhouse client --host clickhouse_hostname_goes_here --port clickhouse_port_goes_here --user username_goes_here --password="$password" <<< "DROP TABLE $DESTINATION_DATABASE.${table_name[$pos]}" 
    pos=$(($pos+1))
done
while [ $pos -lt 1 ] ; do
    clickhouse client --host clickhouse_hostname_goes_here --port clickhouse_port_goes_here --user username_goes_here --password="$password" <<< "DROP MATERIALIZED VIEW $DESTINATION_DATABASE.${view_name[$pos]}" 
    pos=$(($pos+1))
done


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
