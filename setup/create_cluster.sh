#!/bin/bash
#Script to provision a new Azure ML workspace
grn=$'\e[1;32m'
end=$'\e[0m'

# Start of script
SECONDS=0
printf "${grn}CREATING CLUSTER AS PER SPECIFICATION......${end}\n"

# Source URL file
source url.env
dbworkspace_url=$URL

#db_url='https://adb-668283484324415.15.azuredatabricks.net'
cluster_config=$(curl --netrc -X POST $dbworkspace_url/api/2.0/clusters/create --data @cluster-config.json)
printf "Result of cluster configuration:\n $cluster_config \n"
