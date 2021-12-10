#!/bin/bash
#Script to provision a new Azure ML workspace
grn=$'\e[1;32m'
end=$'\e[0m'

# Start of script
SECONDS=0
printf "${grn}STARTING CREATION OF DATABRICKS WORKSPACE......${end}\n"

# Source subscription ID, and prep config file
source sub.env
sub_id=$SUB_ID

# Set the default subscription 
az account set -s $sub_id

# Source unique name for RG, workspace creation
number=$[ ( $RANDOM % 10000 ) + 1 ]
resourcegroup='db'$number
workspacename='db'$number'workspace'
location='westus'

# Create a resource group
printf "${grn}STARTING CREATION OF RESOURCE GROUP...${end}\n"
rg_create=$(az group create --name $resourcegroup --location $location)
printf "Result of resource group create:\n $rg_create \n"

# Create workspace through CLI
printf "${grn}STARTING CREATION OF DATABRICKS WORKSPACE...${end}\n"
ws_result=$(az databricks workspace create\
	--location $location \
	--name $workspacename \
	-g $resourcegroup \
	--sku 'standard')
printf "Result of workspace create:\n $ws_result \n"

# Get the databricks workspace url
printf "${grn}GET DATABRICKS WORKSPACE URL...${end}\n"
url_result=$(az databricks workspace list --query [0].workspaceUrl)
printf "URL: $url_result \n"

# Create URL config file
url_modified=`sed -e 's/^"//' -e 's/"$//' <<<"$url_result"`
printf "${grn}WRITING OUT DATABRICKS URL...${end}\n"
configFile='url.env'
printf "URL=https://$url_modified \n"> $configFile

printf "${grn}10 SECOND BREAK......${end}\n"
sleep 10 # just to give time for artifacts to settle in the system, and be accessible
