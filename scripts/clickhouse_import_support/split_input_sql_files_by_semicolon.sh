#!/usr/bin/env bash

output_filename_prefix="$1"
shift
unset input_filename
declare -a input_filename
input_filename=()
while [ $# -gt 0 ] ; do
    next_arg="$1"
    if ! [ -r "$next_arg" ] ; then
        echo "Error : cannot read from file '$next_arg'" >&2
        exit 1
    fi
    input_filename+=($next_arg)
    shift
done

zero_padded_string=""

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

semicolon_count=0
pos=0
while [ $pos -lt ${#input_filename[@]} ] ; do
    inputfile="${input_filename[$pos]}"
    file_semicolon_count=$( cat $inputfile | grep -c ';' )
    semicolon_count=$(($semicolon_count+$file_semicolon_count))
    pos=$(($pos+1))
done
output_filecount_field_width=${#semicolon_count}

MULTI_SEMICOLON_RE=".*;.*;.*"
EARLY_SEMICOLON_RE=".*;.*[[:graph:]]"
HAS_SEMICOLON_RE=".*;.*"
output_file_index=1
pos=0
set_zero_padded_string "$output_file_index" "$output_filecount_field_width"
outputfile="${output_filename_prefix}_${zero_padded_string}.sql"
rm -f "$outputfile"
while [ $pos -lt ${#input_filename[@]} ] ; do
    inputfile="${input_filename[$pos]}"
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
            output_file_index=$(($output_file_index+1))
            set_zero_padded_string "$output_file_index" "$output_filecount_field_width"
            outputfile="${output_filename_prefix}_${zero_padded_string}.sql"
            rm -f "$outputfile"
        fi
    done < "$inputfile"
    if [ -e "$outputfile" ] && ! [ -s "$outputfile" ] ; then
        # if we have written anything to the current output file, it is now done (even without a terminating semicolon)
        output_file_index=$(($output_file_index+1))
        set_zero_padded_string "$output_file_index" "$output_filecount_field_width"
        outputfile="${output_filename_prefix}_${zero_padded_string}.sql"
        rm -f "$outputfile"
    fi
    pos=$(($pos+1))
done


