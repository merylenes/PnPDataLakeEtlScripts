/usr/bin/spark-submit \
    --name TestCropSales \
    --jars hdfs:///apps/hudi/lib/httpcore-4.4.11.jar,hdfs:///apps/hudi/lib/httpclient-4.5.9.jar,hdfs:///apps/hudi/lib/hudi-spark-bundle.jar,hdfs:///apps/hudi/lib/spark-avro.jar \
    --conf spark.sql.hive.convertMetastoreParquet=false \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --py-files /home/hadoop/lib/pnp.zip \
    /home/hadoop/misc/crop.py \
           -d s3://pnp-data-lake-dev-stz-retail-euw1/td/sales/test_sales_bw_hrsaw01/ \
           -s s3://pnp-data-lake-dev-stz-retail-euw1/td/sales/sales_bw_hrsaw01/ \
           -db stz-retail-dev \
           -t temp_sales_bw_hrsaw01 \
           -dn sales \
           -sv 1 \
           -hs 10.20.0.228