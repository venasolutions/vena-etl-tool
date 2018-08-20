#!/usr/bin/env bash

function usage ()
{
  echo 'Usage:' $0 '<prod_sqlstaging_public_ip>' '<etl_tool_version>'
  echo 'This script releases the new etl tool to https:etl.vena.io/release/etl.jar'
  exit
}

if [  $# -ne 2 ]
then
  usage
  exit 1
fi

export BOTO_CONFIG=/home/jenkins/.boto_provision

PROD_SQLSTAGING_PUBLIC_IP=$1
ETL_TOOL_VERSION=$2
ETL_TOOL_URL="etl.vena.io/release/etl.jar"

mkdir -p release/

cp $WORKSPACE/target/cmdline-etl-tool-*.jar release/etl.jar

aws s3 cp release/etl.jar s3://$ETL_TOOL_URL
scp release/etl.jar Administrator@$PROD_SQLSTAGING_PUBLIC_IP:/cygdrive/c/ETL/etl.jar

wget https://$ETL_TOOL_URL
java -jar etl.jar -version | grep "version: $ETL_TOOL_VERSION"

# If the grep fails, it will have a non-zero exit code.
exit $?
