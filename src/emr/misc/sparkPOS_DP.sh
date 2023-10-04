#!/bin/bash 

#  '{ "destSTZ":"s3://pnp-data-lake-dev-stz-pos-euw1/td/pos/", "sourceURI": "s3://pnp-data-lake-dev-acz-pos-euw1/bw/td/nobal/", "destURI": "s3://pnp-data-lake-dev-acz-pos-euw1/bw/td/hprcm08/processed/", "database":  "stz-pos-dev", "tableName": "pos_bw_hprcm08"}' 

echo "Run this as a once-off, providing the config on the command line"

UNIQ=$(echo $RANDOM | md5sum | cut -c 1-6)
STAGE='dev'

dt=$(date +"%Y-%m-%d-%H%M")
echo $UNIQ

echo "Sync scripts from S3 to the local cluster"
mkdir -p /home/hadoop/bin/
aws s3 sync s3://pnp-data-lake-${STAGE}-bi-emr-bootstrap-euw1/scripts/ /home/hadoop/bin/

echo "Source the Hudi libs"
source /home/hadoop/bin/hudi_libs.sh
hudi_libs

echo -e "Config is:\n\t$1\n"

/usr/bin/spark-submit --name POSData_${dt}_$UNIQ \
       --jars hdfs:///apps/hudi/lib/httpcore-4.4.11.jar,hdfs:///apps/hudi/lib/httpclient-4.5.9.jar,hdfs:///apps/hudi/lib/hudi-spark-bundle.jar,hdfs:///apps/hudi/lib/spark-avro.jar \
       --conf spark.sql.shuffle.partitions=400 \
       --conf spark.sql.hive.convertMetastoreParquet=false \
       --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
       --py-files /home/hadoop/bin/pnp.zip \
       /home/hadoop/bin/pos.py -c "$1"

#if [ $? -eq 0 ]
#then
#   aws s3 mv s3://pnp-data-lake-dev-acz-pos-euw1/bw/td/hprcm08/ s3://pnp-data-lake-dev-acz-pos-euw1/bw/td/hprcm08/processed/$(date +"%Y%m%d")/ --recursive --exclude "*processed*" --dryrun | tr -s ' ' | cut -d' ' -f 3,5 | parallel -j0 -v --colsep ' ' aws s3 mv {1} {2}
#fi
