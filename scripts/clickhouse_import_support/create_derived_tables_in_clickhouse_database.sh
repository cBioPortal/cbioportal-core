#!/usr/bin/env bash

DESTINATION_DATABASE_BLUE="cgds_genie_blue"
DESTINATION_DATABASE_GREEN="cgds_genie_green"
DERIVED_TABLE_STATEMENT_FILE='derived_table_construction_commands.sql'
properties_arg=$1
database_arg=$2
chosen_database_name=""

#TODO : generalize this code and add proper error handling

if [ "$database_arg" == "blue" ] ; then
    chosen_database_name="$DESTINATION_DATABASE_BLUE"
else
    if [ "$database_arg" == "green" ] ; then
        chosen_database_name="$DESTINATION_DATABASE_GREEN"
    else
        echo "Error : database argument must be either 'blue' or 'green'"
        exit 1
    fi
fi

read -p 'enter clickhouse password: ' password

#TODO read file
statement_list=()
while IFS='' read -r line ; do
    statement_list+=( "$line" )
done < "$DERIVED_TABLE_STATEMENT_FILE"

statement_list_length=${#statement_list[@]}
pos=0
while [ $pos -lt $statement_list_length ] ; do
    #TODO do not pass password on the command line like this. Use a configuration file instead
    clickhouse client --host clickhouse_service_hostname_goes_here --port clickhouse_service_port_goes_here --database="$chosen_database_name" --user clickhouse_username_goes_here --password="$password" <<< "${statement_list[$pos]}"
    pos=$(($pos+1))
done

