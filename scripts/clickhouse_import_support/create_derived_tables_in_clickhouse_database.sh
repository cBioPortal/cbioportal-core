#!/usr/bin/env bash

DESTINATION_DATABASE_BLUE="cgds_genie_blue"
DESTINATION_DATABASE_GREEN="cgds_genie_green"
DERIVED_TABLE_STATEMENT_FILE='derived_table_construction_commands.sql'
properties_arg=$1
database_arg=$2
chosen_database_name=""


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
    ~/rob/setting_up_clickhouse/clickhouse client --host ip-10-0-7-23.ec2.internal --port 9000 --database="$chosen_database_name" --user cgds_admin --password="$password" <<< "${statement_list[$pos]}"
    pos=$(($pos+1))
done

