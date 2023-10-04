#!/bin/bash

# This step will copy the code from the bootstrap bucket to the
# cluster
STAGE='prod'

DT=$(date +"%Y%m%d")

SCRIPTS_BUCKET="pnp-data-lake-${STAGE}-bi-emr-bootstrap-euw1"

# Get the libraries from the bucket and put them on the cluster.
# Helps so as not to have to maintain two pieces of the same code
# specifically the copy_code bash function

aws s3 sync s3://${SCRIPTS_BUCKET}/scripts/lib /home/hadoop/lib

. /home/hadoop/lib/functions.sh

copy_code ${SCRIPTS_BUCKET}

copy_conf ${SCRIPTS_BUCKET}

exit 0