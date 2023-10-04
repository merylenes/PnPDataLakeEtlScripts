UNIQ=$(echo $RANDOM | md5sum | cut -c 1-6)
dt=$(date +"%Y-%m-%d-%H%M")
echo $UNIQ
/usr/bin/spark-submit --name SALES_Data_${dt}_$UNIQ \
       --jars hdfs:///apps/hudi/lib/httpcore-4.4.11.jar,hdfs:///apps/hudi/lib/httpclient-4.5.9.jar,hdfs:///apps/hudi/lib/hudi-spark-bundle.jar,hdfs:///apps/hudi/lib/spark-avro.jar \
       --executor-cores=6 \
       --driver-memory=4g \
       --executor-memory=12g \
       --conf spark.sql.shuffle.partitions=550 \
       --conf spark.sql.hive.convertMetastoreParquet=false \
       --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
       --py-files /home/hadoop/bin/pnp.zip \
       /home/hadoop/bin/sales.py -c '{ "destSTZ":"s3://pnp-data-lake-prod-stz-retail-euw1/td/sales/", "sourceURI": "s3://pnp-data-lake-prod-acz-retail-euw1/bw/td/hrsaw01/", "destURI": "s3://pnp-data-lake-prod-acz-retail-euw1/bw/td/hrsaw01/processed/", "database":  "stz-retail-prod", "tableName": "sales_bw_hrsaw01"}' > ${UNIQ}_SALES_data_${dt}-stdout 2> ${UNIQ}_SALES_data_${dt}-stderr 

if [ $? -eq 0 ]
then
   aws s3 mv s3://pnp-data-lake-prod-acz-retail-euw1/bw/td/hrsaw01/ s3://pnp-data-lake-prod-acz-retail-euw1/bw/td/hrsaw01/processed/$(date +"%Y%m%d")/ --recursive --exclude "*processed*" --dryrun | tr -s ' ' | cut -d' ' -f 3,5 | parallel -j0 -v --colsep ' ' aws s3 mv {1} {2}

   aws s3 rm s3://pnp-data-lake-prod-acz-retail-euw1/bw/td/hrsaw01/ --recursive --exclude "*processed*" --include "BW_*"
fi
