#!/bin/bash
#set -x

# In production on the cluster, turn off dedugging
# Debugging set to TRUE will mean no spark jobs are launched and everything
# will look like it works, but doesn't.
# Set to "true" line if you want to debug
DEBUG=true

$DEBUG && DEBUG=true

if ! $DEBUG
then
    . /home/hadoop/lib/functions.sh
    PNP_ETL_PATH="/home/hadoop"
    copy_code pnp-data-lake-prod-bi-emr-bootstrap-euw1
else
    . $(pwd)/lib/functions.sh
    PNP_ETL_PATH=$(pwd)
fi

echo -n "Beginning STEP "
# IF you set an environment variable of
# export DEBUG=false then things in this script will happen for real
# Such as copying PROD data to DEV

# Set DEBUG=false to make this happen for real. DEBUG=true or DEBUG unset will mean we're in DEBUG mode
# and no operations will happen for real.


DT=$(date +"%Y%m%d")
echo -n "on ${DT} "

DATASET=$1
DATASET_NAME=$(echo $DATASET |sed 's/^[a-z]/\U&/')   # This is used to determine the STZ destination, logging and more
echo "for ${DATASET_NAME}"

JOBCONFIG=$(jq ".$DATASET" ${PNP_ETL_PATH}/conf/jobconfig.json)

$DEBUG && echo "------ DEBUG MODE for dataset ${DATASET_NAME} ------"
echo $JOBCONFIG | jq -M '.'

# ==== MODIFY VARIABLES BETWEEN THIS LINE AND THE SIMILAR LINE BELOW ====
DESC=$(echo $JOBCONFIG | jq -r '.description') 

STAGE=$(echo $JOBCONFIG | jq -r '.dataset_stage')        # This is the ACTUAL STAGE YOU WISH TO RUN
echo "${DATASET_NAME} stage is ${STAGE} "

SNOWFLAKE=""
[ `jq -er ".${DATASET}.${STAGE} | has(\"snowflake\")" ${PNP_ETL_PATH}/conf/jobconfig.json` == 'true' ] && SNOWFLAKE=$(echo $JOBCONFIG | jq -r ".${STAGE}.snowflake")        # If this is a dev stage AND snowflaks is set, then set this variable

CODE_TO_RUN=$(echo $JOBCONFIG | jq -r '.spark_job')      # This is the Spark job that will run via spark-submit

if [ ${STAGE} = 'dev' ]
then
    # If we're running in DEV, we want to copy the newest files from PROD to DEV for the days until it turns into PROD
    # In PROD we're operating on the PROD ACZ bucket, so no need to copy anything anywhere

    SRC_ACZ_BUCKET=$(echo $JOBCONFIG | jq -r '.prod.bucket.acz') # In DEV, we want to copy new files from the PROD bucket to the DEV bucket
                                                                 # so this (SRC_ACZ_BUCKET) must be the PROD bucket
    DST_ACZ_BUCKET=$(echo $JOBCONFIG | jq -r '.dev.bucket.acz') 
    ACZ_PREFIX=$(echo $JOBCONFIG | jq -r '.acz_prefix') 
    DAYS=$(echo $JOBCONFIG | jq -r ".${STAGE}.data_age")        # Data are copied from PROD to DEV for a testing period. During this time we only 
                                                                # want to compare the PROD and DEV buckets back in time by this many days
                                                                # otherwise it will potentially re-copy data from PROD that's been processed
                                                                # and removed from the DEV bucket from some weeks/months ago

    # If DEBUG=true or it's UNSET then it will do a dry-run copy of the files so you
    # can see what's happening
    if ! $DEBUG
    then
         #This is not a dry-run. This WILL copy the files from PROD to DEV buckets
        # Set DEBUG=false to make this happen for real
        # echo "------------> Copy would have happened FOR REAL"
        echo "Running a getNewestFiles.py on ${DATASET_NAME} from bucket ${SRC_ACZ_BUCKET}/${ACZ_PREFIX}"
        /usr/bin/python3 ${PNP_ETL_PATH}/lib/getNewestFiles.py \
                        --sourceURI s3://${SRC_ACZ_BUCKET}/${ACZ_PREFIX}/ \
                        --destinationURI s3://${DST_ACZ_BUCKET}/${ACZ_PREFIX}/ \
                        --age ${DAYS} \
                        --dryrun True | parallel --colsep ' ' -j0 aws s3 cp {1} {2} --quiet
        cat <<EOT
            /usr/bin/python3 ${PNP_ETL_PATH}/lib/getNewestFiles.py --sourceURI s3://${SRC_ACZ_BUCKET}/${ACZ_PREFIX}/ --destinationURI s3://${DST_ACZ_BUCKET}/${ACZ_PREFIX}/ --age ${DAYS} --dryrun True | parallel --colsep ' ' -j0 aws s3 cp {1} {2} --quiet 
EOT
    else
        echo "------ DEBUG MODE ------"
        echo "Running a getNewestFiles.py on ${DATASET_NAME} from bucket ${SRC_ACZ_BUCKET}/${ACZ_PREFIX}"
        cat <<EOT
            /usr/bin/python3 ${PNP_ETL_PATH}/lib/getNewestFiles.py --sourceURI s3://${SRC_ACZ_BUCKET}/${ACZ_PREFIX}/ --destinationURI s3://${DST_ACZ_BUCKET}/${ACZ_PREFIX}/ --age ${DAYS} --dryrun True | parallel --colsep ' ' -j0 aws s3 cp {1} {2} --dryrun
EOT
    fi
fi

STZ_PREFIX=$(echo $JOBCONFIG | jq -r '.stz_prefix') 
STZ_BUCKET=$(echo $JOBCONFIG | jq -r ".${STAGE}.bucket.stz")
STZ_S3="s3://${STZ_BUCKET}/${STZ_PREFIX}/"

ACZ_PROCESSED_PREFIX=$(echo $JOBCONFIG | jq -r ".processed_prefix")
ACZ_PREFIX=$(echo $JOBCONFIG | jq -r ".acz_prefix")
ACZ_BUCKET=$(echo $JOBCONFIG | jq -r ".${STAGE}.bucket.acz")
ACZ_S3="s3://${ACZ_BUCKET}/${ACZ_PREFIX}/"
ACZ_S3_PROCESSED="s3://${ACZ_BUCKET}/${ACZ_PREFIX}/${ACZ_PROCESSED_PREFIX}"

STZ_DB=$(echo $JOBCONFIG | jq -r ".${STAGE}.db")
STZ_TABLE=$(echo $JOBCONFIG | jq -r ".table")

CONF="{ \"datasetName\":\"${DATASET_NAME}\",
        \"destSTZ\": \"${STZ_S3}\",
        \"sourceURI\": \"${ACZ_S3}\",
        \"database\": \"${STZ_DB}\",
        \"tableName\": \"${STZ_TABLE}\" }"

# Copy the code from S3 for execution locally
#>#copy_code

if [ $? -eq 0 ]
then
    SPARK_JOB=$(echo $JOBCONFIG | jq -r ".spark_job")

    echo -e "Job config\n\t${CONF}"
    echo "Starting Spark job for ${DATASET_NAME}"

    # This is not a dry-run. This WILL spark-submit command WILL be run on the data
    # Set DEBUG=false to make this happen for real. DEBUG=true or DEBUG unset will mean we're in DEBUG mode
    # and no operations will happen for real.
    if ! $DEBUG
    then
        #echo "------------> Spark Job would have happened FOR REAL"
        /usr/bin/spark-submit --name ${DATASET_NAME}Data_${DT} \
            $SPARKJARS \
            $SPARKCONF \
#           --jars hdfs:///apps/hudi/lib/httpcore-4.4.11.jar,hdfs:///apps/hudi/lib/httpclient-4.5.9.jar,hdfs:///apps/hudi/lib/hudi-spark-bundle.jar,hdfs:///apps/hudi/lib/spark-avro.jar \
#           --conf spark.executor.memoryOverhead=2048 \
#           --conf spark.sql.shuffle.partitions=1200 \
#           --conf spark.sql.hive.convertMetastoreParquet=false \
#           --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
           --py-files ${PNP_ETL_PATH}/lib/pnp.zip \
           ${PNP_ETL_PATH}/${SPARK_JOB} -c "${CONF}"
#       --driver-memory=4g \
#       --executor-memory=34g \
#       --conf spark.sql.shuffle.partitions=400 \
        if [ $? -eq 0 ]
        then

            # Provided the Spark job completed without exiting - i.e. it executed fine
            # move files from the ACZ bucket to a sub-prefix "processed/yyyyMMdd/" in the ACZ bucket
            # While in dev testing, these resolve to the dev-acz buckets, but in prod will resolve to prod-acz buckets
            if ! $DEBUG
            then
                echo "Move processed files to ${ACZ_S3} ${ACZ_S3_PROCESSED}/${DT}/"
                move_files ${ACZ_S3} ${ACZ_S3_PROCESSED}/${DT}/ '*' '*processed*'
            else
                echo "------ DEBUG MODE ------"
                echo "Move processed files to ${ACZ_S3} ${ACZ_S3_PROCESSED}/${DT}/ would have happened"
            fi
        fi
    else
        echo "------ DEBUG MODE ------"
        echo "Running in DEBUG mode, so this is the spark-submit command that WOULD have been run"
        
        cat <<EOT
            /usr/bin/spark-submit --name ${DATASET_NAME}Data_${DT} --jars hdfs:///apps/hudi/lib/httpcore-4.4.11.jar,hdfs:///apps/hudi/lib/httpclient-4.5.9.jar,hdfs:///apps/hudi/lib/hudi-spark-bundle.jar,hdfs:///apps/hudi/lib/spark-avro.jar --conf spark.sql.hive.convertMetastoreParquet=false --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --py-files ${PNP_ETL_PATH}/lib/pnp.zip ${PNP_ETL_PATH}/${SPARK_JOB} -c ${CONF}"
EOT
    fi


    # If the snowflake flag set, we want to copy the PROD bucket to the DEV bucket in the STZ zone
    if ${SNOWFLAKE}
    then
        # Remove STZ data from DEV
        DEV_STZ_BUCKET=$(echo $JOBCONFIG | jq -r ".dev.bucket.stz")
        DEV_STZ_PREFIX=$(echo $JOBCONFIG | jq -r ".stz_prefix")
        DEV_STZ_S3=${DEV_STZ_BUCKET}/${DEV_STZ_PREFIX}

        remove_stz_from_dev ${DEV_STZ_S3}

        # Sync PROD to DEV STZ for as long as it's needed (requested by Patrick for the Snowflake guys)
        sync_stz_prod_to_dev  ${STZ_S3} ${DEV_STZ_S3}
    fi

    echo "Spark job done for ${DATASET_NAME}"
    exit 0
else
    echo "The copy of the newest files for ${DATASET_NAME} did not work, so no spark job ran"
    exit 1
fi