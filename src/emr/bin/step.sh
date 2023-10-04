#!/bin/bash
set -x

# General plan:
#   Get the code from the repo (S3)
#   Get the config from the jobconf.json using python code
#   if dataset == dev
#       copy files not older than X days to the dev bucket
#       
DATASET=$1

# In production on the cluster, turn off dedugging
# Debugging set to TRUE will mean no spark jobs are launched and everything
# will look like it works, but doesn't.
# Set to "true" line if you want to debug

# IF you set an environment variable of
# export DEBUG=false then things in this script will happen for real
# Such as copying PROD data to DEV

# Set DEBUG=false to make this happen for real. DEBUG=true or DEBUG unset will mean we're in DEBUG mode
# and no operations will happen for real.
DEBUG=false

$DEBUG && DEBUG=true

# We get the stage here to know which scripts to download from which bucket
# since there are scripts in the dev bootstrap bucket for dev and prod scripts
# in the prod bootstrap bucket

if ! $DEBUG
then
    . /home/hadoop/lib/functions.sh
    PNP_ETL_PATH="/home/hadoop"
    #aws s3 sync s3://pnp-data-lake-dev-bi-emr-bootstrap-euw1/feature-1-scripts/ . --exact-timestamps
else
    . $(pwd)/lib/functions.sh
    PNP_ETL_PATH=$(pwd)
#    copy_code pnp-data-lake-prod-bi-emr-bootstrap-euw1
fi

echo "Sync'ing conf to the cluster - always from the PROD bucket"
# Get the config separate from the code and always pull from PROD for config
copy_conf pnp-data-lake-prod-bi-emr-bootstrap-euw1

# We get the stage here to know which scripts to download from which bucket
# since there are scripts in the dev bootstrap bucket for dev and prod scripts
# in the prod bootstrap bucket
STAGE=$(jq -rM --arg i "$DATASET" '.[$i].dataset_stage' ${PNP_ETL_PATH}/conf/jobconfig.json)

echo "\n===== Stage is ${STAGE} =====\n"

#echo "Sync'ing code to the cluster"
# Get the config separate from the code
copy_code pnp-data-lake-prod-bi-emr-bootstrap-euw1

DT=$(date +"%Y%m%d")
echo "===== Beginning STEP on ${DT} ====="

# A python program to read the JSON and translate these things into environmental variables to be used in this script

eval `python3 ${PNP_ETL_PATH}/lib/jobconf.py -j ${PNP_ETL_PATH}/conf/jobconfig.json -d ${DATASET}`

CONF="{ \"datasetName\":\"${DATASET_NAME}\",
        \"destSTZ\": \"${STZ_S3}\",
        \"sourceURI\": \"${ACZ_S3}\",
        \"database\": \"${STZ_DB}\",
        \"tableName\": \"${STZ_TABLE}\" }"

PATH_TO_PROCESS="${ACZ_S3}to_process/"

echo -e "Job config\n\t${CONF} for ${DATASET_NAME}"

$DEBUG && echo "------ DEBUG MODE for dataset ${DATASET_NAME} ------"

echo "${DATASET_NAME} stage is ${STAGE} "

if [ ${STAGE} = 'dev' ]
then
    # If we're running in DEV, we want to copy the newest files from PROD to DEV for the days
    # until it turns into PROD In PROD we're operating on the PROD ACZ bucket, so no need to
    # copy anything anywhere

    # Data are copied from PROD to DEV for a testing period. During this time we only 
    # want to compare the PROD and DEV buckets back in time by this (DAYS) many days
    # otherwise it will potentially re-copy data from PROD that's been processed
    # and removed from the DEV bucket from some weeks/months ago

    # If DEBUG=true or it's UNSET then it will do a dry-run copy of the files so you
    # can see what's happening
    if ! $DEBUG
    then
         #This is not a dry-run. This WILL copy the files from PROD to DEV buckets
        # Set DEBUG=false to make this happen for real
        echo "Running a getNewestFiles.py on ${DATASET_NAME} from bucket ${SRC_ACZ_S3_URI}"
        /usr/bin/python3 ${PNP_ETL_PATH}/lib/getNewestFiles.py \
                        --sourceURI ${SRC_ACZ_S3_URI}/ \
                        --destinationURI ${DST_ACZ_S3_URI}/ \
                        --age ${DAYS} \
                        --dryrun True | parallel --silent --colsep ' ' -j0 aws s3 cp {1} {2}
        echo "Command was:\n"
        cat <<EOT
            /usr/bin/python3 ${PNP_ETL_PATH}/lib/getNewestFiles.py
                            --sourceURI ${SRC_ACZ_S3_URI}/
                            --destinationURI ${DST_ACZ_S3_URI}/
                            --age ${DAYS} --dryrun True | parallel --silent --colsep ' ' -j0 aws s3 cp {1} {2}
EOT
    else
        echo "------ DEBUG MODE ------"
        echo "Running a getNewestFiles.py on ${DATASET_NAME} from bucket ${SRC_ACZ_S3_URI}"
        echo "Command would have been:\n"
        cat <<EOT
            /usr/bin/python3 ${PNP_ETL_PATH}/lib/getNewestFiles.py
                            --sourceURI ${SRC_ACZ_S3_URI}/
                            --destinationURI ${DST_ACZ_S3_URI}/
                            --age ${DAYS} --dryrun True | parallel --silent --colsep ' ' -j0 aws s3 cp {1} {2} --dryrun
EOT
    fi
fi

if [ $? -eq 0 ]
then

    # Keep a list of files that we'll be processing on this run in an array
    # TO_PROCESS so that we know which files to move at the end of the run


    echo "Keep a list of files to process on this run"
    # Removed this function for now since EMR does not have v5 of bash but still and old version (urgh!)
    #files_to_process $ACZ_S3 TO_PROCESS
    # Declare an array of files we need to process
    declare TO_PROCESS=($(aws s3 ls ${ACZ_S3} | grep .csv | tr -s ' ' | cut -d' ' -f4))
    echo -e "===== ${#TO_PROCESS[*]} files to move for processing\n${TO_PROCESS[*]}\n=====\n"

    echo "Copy files to process into a to_process prefix"
    if ! $DEBUG
    then
        [ ! -z $TO_PROCESS ] && parallel -j0 --silent aws s3 mv ${ACZ_S3}{} ${PATH_TO_PROCESS}{} ::: ${TO_PROCESS[*]}
    fi
    # append to_process on the end of the sourceURI so that Spark knows where to find the data
    CONF=$(echo $CONF | sed "s@\"sourceURI\": \"${ACZ_S3}\"@\"sourceURI\": \"${ACZ_S3}to_process/\"@")
    echo "===== Modified CONF =====" 
    echo $CONF

    echo "Starting Spark job for ${DATASET_NAME}"

    # This is not a dry-run. This WILL spark-submit. The command WILL be run on the data
    # Set DEBUG=false to make this happen for real. DEBUG=true or DEBUG unset will mean we're in DEBUG mode
    # and no operations will happen for real.
    if ! $DEBUG
    then
    cat <<EOF
            /usr/bin/spark-submit --name "${DATASET_NAME}Data_${DT} [ ${STAGE} ]" \
            --conf "spark.executor.extraJavaOptions=-XX:NewSize=1g -XX:SurvivorRatio=2 -XX:+UseCompressedOops -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:CMSInitiatingOccupancyFraction=70 -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime -XX:+PrintGCApplicationConcurrentTime -XX:+PrintTenuringDistribution -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp/hoodie-heapdump.hprof" \
            --conf spark.network.timeout=10000000 --conf spark.executor.heartbeatInterval=10000000 \
            $SPARKJARS \
            $SPARKCONF \
           --py-files ${PNP_ETL_PATH}/lib/pnp.zip \
           ${PNP_ETL_PATH}/${SPARK_JOB} -c "${CONF}"
EOF

        /usr/bin/spark-submit --name "${DATASET_NAME}Data_${DT} [ ${STAGE} ]" \
            $SPARKJARS \
            $SPARKCONF \
            --conf "spark.executor.extraJavaOptions=-XX:NewSize=1g -XX:SurvivorRatio=2 -XX:+UseCompressedOops -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:CMSInitiatingOccupancyFraction=70 -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime -XX:+PrintGCApplicationConcurrentTime -XX:+PrintTenuringDistribution -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp/hoodie-heapdump.hprof" \
            --conf spark.network.timeout=10000000 --conf spark.executor.heartbeatInterval=10000000 \
           --py-files ${PNP_ETL_PATH}/lib/pnp.zip \
           ${PNP_ETL_PATH}/${SPARK_JOB} -c "${CONF}"
        if [ $? -eq 0 ]
        then
            # Provided the Spark job completed without exiting - i.e. it executed fine
            # move files from the ACZ bucket to a sub-prefix "processed/yyyyMMdd/" in the ACZ bucket
            # While in dev testing, these resolve to the dev-acz buckets, but in prod will resolve to prod-acz buckets
            echo "Final move: moving processed files from ${PATH_TO_PROCESS} to ${ACZ_S3}processed/${DT}/"

            # Empty bucket because all the files are already in the to_process prefix?
            # Re-create the TO_PROCESS list from the to_process prefix otherwise parallel locks waiting for input
            declare TO_PROCESS=($(aws s3 ls ${PATH_TO_PROCESS} | grep .csv | tr -s ' ' | cut -d' ' -f4))

            [ ! -z $TO_PROCESS ] && parallel -j0 --silent aws s3 mv ${PATH_TO_PROCESS}{} ${ACZ_S3}processed/${DT}/{} ::: ${TO_PROCESS[*]} || echo "No files to move. Finishing"
        fi
    else
        echo "------ DEBUG MODE ------"
        echo "Running in DEBUG mode, so this is the spark-submit command that WOULD have been run"
        
        cat <<EOT
                /usr/bin/spark-submit --name "${DATASET_NAME}Data_${DT} [ ${STAGE} ]" \
                   $SPARKJARS \
                   $SPARKCONF \
                   --conf "spark.executor.extraJavaOptions=-XX:NewSize=1g -XX:SurvivorRatio=2 -XX:+UseCompressedOops -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:CMSInitiatingOccupancyFraction=70 -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime -XX:+PrintGCApplicationConcurrentTime -XX:+PrintTenuringDistribution -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp/hoodie-heapdump.hprof" \
                   --conf spark.network.timeout=10000000 --conf spark.executor.heartbeatInterval=10000000 \
                   --py-files ${PNP_ETL_PATH}/lib/pnp.zip \
                   ${PNP_ETL_PATH}/${SPARK_JOB} -c "${CONF}"
EOT
        echo "------ DEBUG MODE ------"
        declare TO_PROCESS=($(aws s3 ls ${PATH_TO_PROCESS} | grep .csv | tr -s ' ' | cut -d' ' -f4))
        echo "Move processed files to ${PATH_TO_PROCESS} ${ACZ_S3_PROCESSED}${DT}/ would have happened"
        [ ! -z $TO_PROCESS ] && echo "parallel -j0 aws s3 mv ${PATH_TO_PROCESS}{} ${ACZ_S3}processed/${DT}/{} --dryrun --silent ::: ${TO_PROCESS[*]}"
    fi

    echo "Spark job done for ${DATASET_NAME}"
    exit 0
else
    echo "The copy of the newest files for ${DATASET_NAME} did not work, so no spark job ran"
    exit 1
fi