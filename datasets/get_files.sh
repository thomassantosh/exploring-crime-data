#!/bin/bash
#Script to provision a new Azure ML workspace
grn=$'\e[1;32m'
end=$'\e[0m'

# Start of script
SECONDS=0
printf "${grn}GETTING FILES FROM DBFS......${end}\n"

# Source URL file
source ./../setup/url.env
dbworkspace_url=$URL

# Processed file
file_path=$(curl --netrc -X GET $dbworkspace_url/api/2.0/dbfs/read --data @processed.json | jq .data)
echo $file_path > "encoded_processed.txt"
printf "Processed file downloaded. \n"

# Correlation matrix file
file_path=$(curl --netrc -X GET $dbworkspace_url/api/2.0/dbfs/read --data @correlation.json | jq .data)
echo $file_path > "encoded_correlation.txt"
printf "Correlation file downloaded. \n"
