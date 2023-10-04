UNIQ=$(echo $RANDOM | md5sum | cut -c 1-6)
dt=$(date +"%Y-%m-%d-%H%M")
echo $UNIQ
/usr/bin/spark-submit --name POSData_${dt}_$UNIQ \
       --jars hdfs:///apps/hudi/lib/httpcore-4.4.11.jar,hdfs:///apps/hudi/lib/httpclient-4.5.9.jar,hdfs:///apps/hudi/lib/hudi-spark-bundle.jar,hdfs:///apps/hudi/lib/spark-avro.jar \
       --conf spark.sql.shuffle.partitions=400 \
       --conf spark.sql.hive.convertMetastoreParquet=false \
       --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
       --py-files /home/hadoop/bin/pnp.zip \
       /home/hadoop/bin/pos.py -c '{ "destSTZ":"s3://pnp-data-lake-dev-stz-pos-euw1/td/pos/", "sourceURI": "s3://pnp-data-lake-dev-acz-pos-euw1/bw/td/nobal/", "destURI": "s3://pnp-data-lake-dev-acz-pos-euw1/bw/td/hprcm08/processed/", "database":  "stz-pos-dev", "tableName": "pos_bw_hprcm08"}' > ${UNIQ}_POS_data_${dt}-stdout 2> ${UNIQ}_POS_data_${dt}-stderr 

#       --executor-cores=4 \
#       --driver-memory=4g \
#       --executor-memory=12g \

#if [ $? -eq 0 ]
#then
#   aws s3 mv s3://pnp-data-lake-dev-acz-pos-euw1/bw/td/hprcm08/ s3://pnp-data-lake-dev-acz-pos-euw1/bw/td/hprcm08/processed/$(date +"%Y%m%d")/ --recursive --exclude "*processed*" --dryrun | tr -s ' ' | cut -d' ' -f 3,5 | parallel -j0 -v --colsep ' ' aws s3 mv {1} {2}
#fi
