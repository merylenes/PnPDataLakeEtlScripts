function hudi_libs ( ) {

   which hdfs
    # This is done as the hadoop user
   if [ -x /usr/bin/hdfs ]
   then
      /usr/bin/hdfs dfs -mkdir -p /apps/hudi/lib
      /usr/bin/hdfs dfs -copyFromLocal /usr/lib/hudi/hudi-spark-bundle.jar /apps/hudi/lib/hudi-spark-bundle.jar
      /usr/bin/hdfs dfs -copyFromLocal /usr/lib/spark/external/lib/spark-avro.jar /apps/hudi/lib/spark-avro.jar
      /usr/bin/hdfs dfs -copyFromLocal /usr/lib/hudi/hudi-hadoop-mr-bundle-*-amzn-1.jar /apps/hudi/lib/hudi-hadoop-mr-bundle-0.6.0-amzn-0.jar
      /usr/bin/hdfs dfs -copyFromLocal /usr/lib/hadoop/lib/httpcore-4.4.11.jar /apps/hudi/lib/httpcore-4.4.11.jar
      /usr/bin/hdfs dfs -copyFromLocal /usr/lib/hadoop/lib/httpclient-4.5.9.jar /apps/hudi/lib/httpclient-4.5.9.jar
   elif [ -x /usr/bin/hadoop ]
   then
      /usr/bin/hadoop fs -mkdir -p /apps/hudi/lib 
      /usr/bin/hadoop fs -copyFromLocal /usr/lib/hudi/hudi-spark-bundle.jar /apps/hudi/lib/hudi-spark-bundle.jar
      /usr/bin/hadoop fs -copyFromLocal /usr/lib/spark/external/lib/spark-avro.jar /apps/hudi/lib/spark-avro.jar
      /usr/bin/hadoop fs -copyFromLocal /usr/lib/hudi/hudi-hadoop-mr-bundle-*-amzn-1.jar /apps/hudi/lib/hudi-hadoop-mr-bundle-0.6.0-amzn-0.jar
      /usr/bin/hadoop fs -copyFromLocal /usr/lib/hadoop/lib/httpcore-4.4.11.jar /apps/hudi/lib/httpcore-4.4.11.jar
      /usr/bin/hadoop fs -copyFromLocal /usr/lib/hadoop/lib/httpclient-4.5.9.jar /apps/hudi/lib/httpclient-4.5.9.jar
   else
      echo "Can't copy files to HDFS since can't find binaries to do it"
      echo "No /usr/bin/hadoop or /usr/bin/hdfs in the path"
   fi
}
