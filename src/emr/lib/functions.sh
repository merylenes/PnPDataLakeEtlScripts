function files_to_process() {
    SOURCE_URI=$1
    local -n local_arr=$2
    local_arr=($(aws s3 ls ${SOURCE_URI} | grep .csv | cut -d' ' -f4))
}

function copy_conf() {

    PNP_ETL_PATH="/home/hadoop"

    # This function takes as a parameter the bootstrap bucket name that
    # the scripts reside in and will copy the configs from the bucket to the cluster

    SCRIPTS_BUCKET=$1

    echo "Sync configs to the cluster"
    #aws s3 sync s3://${SCRIPTS_BUCKET}/emr ${PNP_ETL_PATH} --exclude "*" --include "*conf/*" --exact-timestamps
    aws s3 sync s3://${SCRIPTS_BUCKET}/scripts ${PNP_ETL_PATH} --exclude "*" --include "*conf/*" --exact-timestamps
}

function copy_code() {

    PNP_ETL_PATH="/home/hadoop"

    # This function takes as a parameter the bootstrap bucket name that
    # the scripts reside in and will copy the scripts from the bucket to the cluster

    SCRIPTS_BUCKET=$1
    # This function tests whether the latest code is on the cluster
    # and copies it to the cluster for processing
    # It also copies the hudi libraries to the cluster for processing

    echo "Sync code to the cluster"
    #aws s3 sync s3://${SCRIPTS_BUCKET}/emr ${PNP_ETL_PATH} --exclude "*conf/*" --exact-timestamps
    aws s3 sync s3://${SCRIPTS_BUCKET}/scripts ${PNP_ETL_PATH} --exclude "*conf/*" --exact-timestamps

    # No need to copy the files to HDFS if they're there already
    if ! /usr/bin/hadoop fs -ls /apps/hudi/lib/hudi-spark-bundle.jar > /dev/null 2>&1
    then
        echo "Source the Hudi libs"
        source ${PNP_ETL_PATH}/lib/hudi_libs.sh
        hudi_libs
    fi
}

function move_files() {
    # This function moves file from one place to another on S3

    # Note that it EXCLUDES all files at the start of the pattern and then adds them back with includes
    # before exluding those files we're not interested in

    # There is an option to leave off the exlude pattern, but you MUST know what you're doing
    # If you choose to drop the EXCL_PATTERN or it will do things you don't expect

    # TESTING? exporting an ENV variable DRYRUN='--dryrun' will run this move in DRYRUN mode

    # It takes 4 parameters
    # (1) The SRC_URI which is both the SRC_BUCKET & SRC_PREFIX - e.g. s3://pnp-data-lake-prod-acz-pos-euw1/bw/td/hprcm08/
    # (2) The DST_URI which is both the DST_BUCKET & DST_PREFIX - e.g. s3://pnp-data-lake-prod-acz-pos-euw1/bw/td/hprcm08/processed/
    # (3) A single EXCL_PATTERN - e.g. "*" or "*processed*"
    # (4) A single INCL_PATTERN - e.g. "*BW_HPRCM082[01]_..._20200[34]*"

    SRC_URI=$1
    DST_URI=$2
    INCL_PAT=$3
    EXCL_PAT=$4

    if [ ! -z $EXCL_PAT ]
    then
        aws s3 mv ${SRC_URI} ${DST_URI} --recursive --exclude "*" --include "${INCL_PAT}" --exclude "${EXCL_PAT}" --dryrun | \
                    tr -s ' ' | \
                    cut -d' ' -f 3,5 | \
                    parallel -j10 --colsep ' ' aws s3 mv {1} {2} ${DRYRUN}
    else
        aws s3 mv ${SRC_URI} ${DST_URI} --recursive --exclude "*" --include "${INCL_PAT}" --dryrun | \
                    tr -s ' ' | \
                    cut -d' ' -f 3,5 | \
                    parallel -j10 --colsep ' ' aws s3 mv {1} {2} ${DRYRUN}
    fi
}

function remove_stz_from_dev() {

    # This is a terrible way of doing this. This trashes the DST_STZ_BUCKET since Hoodie will have duplicates if 
    # we simply sync PROD to DEV.

    # This function takes 2 params:
    #(1) The DST STZ S3 URI - this is usually DEV (which we're syncing from PROD)

    DEV_STZ_S3=$1

    echo -e "\n\t<-- About to remove data from ${DEV_STZ_S3} -->\n"
    aws s3 rm ${DEV_STZ_S3}/ --recursive --quiet
    
    [ $? -eq 0 ] && echo -e "\n\t<-- Removed s3://${DEV_STZ_S3}/ from DEV -->\n"
}

function sync_stz_prod_to_dev() {
    # This function takes 3 params:
    #(1) The SRC STZ BUCKET - this is usually PROD (which we're syncing to DEV)
    #(2) The DST STZ BUCKET - this is usually DEV (which we're syncing from PROD)

    # e.g   SRC_STZ_BUCKET='pnp-data-lake-prod-stz-retail-euw1'
    #       DST_STZ_BUCKET='pnp-data-lake-dev-stz-retail-euw1'

    PRD_STZ_S3=$1
    DEV_STZ_S3=$2

    echo -e "\n\t<-- About to move data from s3://${PRD_STZ_S3}/ to s3://${DEV_STZ_S3}/ -->\n"
    time aws s3 cp s3://${PRD_STZ_S3}/ s3://${DEV_STZ_S3}/ --recursive --dryrun |
        tr -s ' ' |
        cut -d' ' -f3,5 |
        parallel -j0 --colsep ' ' aws s3 cp {1} {2} --quiet

    [ $? -eq 0 ] && echo -e "\n\t<-- Done syncing s3://${PRD_STZ_S3}/ to s3://${DEV_STZ_S3}/ -->\n"
}