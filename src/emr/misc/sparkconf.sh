#!/usr/bin/bash

function sparkConfs() {
    confs=$1
    echo $confs
    
    STR=""
    KEYS=$(echo $confs | jq -r 'keys')
    for KEY in $KEYS
    do
        echo $KEY
        #VALUE=$(echo $confs|jq ".${KEY}")
        #STR="--conf $KEY=${VALUE} "
    done
    echo $STR
}

DEBUG=true

if ! $DEBUG
then
    . /home/hadoop/lib/functions.sh
    PNP_ETL_PATH="/home/hadoop"
else
    . $(pwd)/lib/functions.sh
    PNP_ETL_PATH=$(pwd)
fi

DT=$(date +"%Y%m%d")
echo -n "on ${DT} "

DATASET=$1
DATASET_NAME=$(echo $DATASET |sed 's/^[a-z]/\U&/')   # This is used to determine the STZ destination, logging and more
echo "for ${DATASET_NAME}"

JOBCONFIG=$(jq ".$DATASET" ${PNP_ETL_PATH}/conf/jobconfig.json)

$DEBUG && echo "------ DEBUG MODE for dataset ${DATASET_NAME} ------"
echo $JOBCONFIG | jq -M '.'

SPARKCONF=""
echo ${JOBCONFIG} | jq -er "has(\"spark\")" && SPARKCONF=$(echo $JOBCONFIG | jq -r ".spark.conf")        # If this is a dev stage AND snowflaks is set, then set this variable

sparkConfs "$SPARKCONF"

