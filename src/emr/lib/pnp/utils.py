#import boto3
from builtins import staticmethod
from importlib.resources import path
import re, json
from datetime import datetime
#import pytz
from .logging import Log4j
from pyspark.sql import Window
from pyspark.sql.functions import input_file_name, current_timestamp, date_format, regexp_extract, collect_list, row_number, col, count
from pyspark.sql.types import StringType

class utils(object):

    def __init__(self):
        self.rows_read = 0
        self.fileType = ''

    def convertDateToPartition(self, spark, dataFrame, dateCol):
        """
        This method will take a datetime column and make it ready to use as a partition column
        """

        sparkLogger = Log4j(spark)
        sparkLogger.info(f"Converting date column {dateCol} to something we can partition by")

        newDf = dataFrame.withColumn(dateCol, date_format(col(dateCol), 'yyyy-MM-dd').cast(StringType()))
        #sparkLogger.info(newDf.show(2, truncate=False))
        #sparkLogger.info(f"{newDf.printSchema()}")

        return newDf

    def findDups(self, spark, dataFrame, allKeys):
        """
        This function will find the duplicates for the keys in the data
        and create a dataframe of those duplicates
        """
        sparkLogger = Log4j(spark)
        sparkLogger.info(f"Finding duplicate records...")

        w = Window.partitionBy(allKeys)
        return dataFrame.select('*', count("DI_REQTSN").over(w).alias('dupeCount')).where("dupeCount > 1").drop('dupeCount')

    def _splitBasePath(self, spark, path):

        """
        We want to take an S3 path and split it up into the various components
        We don't know whether we'll need the env, zone, category, etc. in the future, so we might as well spend the effort now
        to split it up appropriately. 
        """
        sparkLogger = Log4j(spark)
        writePath = f"{path}"

        # s3://pnp-data-lake-dev-stz-retail-euw1/td/sales/sales_bw_hrsam01/
        # We want to split this up into a bunch of tokens to be able to re-assemble it later as needed

        pathRe = re.compile(r's3://(?P<bucket>(?P<lake>.*)-(?P<env>dev|prod|qa)-(?P<zone>acz|stz)-(?P<cat>.*)-euw1)/(?P<prefix>.*)')
        pathMatch = pathRe.match(writePath)

        bucket = pathMatch['bucket']
        lake = pathMatch['lake'] # pnp-data-lake or box-data-lake or whatever
        env = pathMatch['env'] # dev or qa or prod
        functional_zone = pathMatch['zone'] # stz or acz
        dataCategory = pathMatch['cat'] # sales/retail/whatever
        s3Prefix = pathMatch['prefix'] # the rest: td/sales/del_sales_bw_hrsam01
        table = s3Prefix.split('/')[-2]
        
        sparkLogger.info(f"BasePath:\t{path}")
        sparkLogger.info(f"Bucket:\t{bucket}")
        sparkLogger.info(f"\ts3Prefix:\t{s3Prefix}")
        sparkLogger.info(f"Lake:\t{lake}")
        sparkLogger.info(f"\tEnvironment:\t{env}")
        sparkLogger.info(f"\tFunctional zone:\t{functional_zone}")
        sparkLogger.info(f"\tdataCategory:\t{dataCategory}")
        sparkLogger.info(f"\ttable:\t{table}")

        return {'basepath': path, \
                'bucket': bucket, \
                's3Prefix': s3Prefix, \
                'lake': lake, \
                'env': env, \
                'zone': functional_zone, \
                'cat': dataCategory, \
                'table': table}

    def writeDels(self, spark, conf, dataFrame, partitions=[], mode='append'):
        """
        This method will write a standard Spark dataframe partitioned if required

        TODO: At some future time, we want to combine the writeDups and writeDels into a single method
        """

        sparkLogger = Log4j(spark)
        newWritePath = f"{conf.basePath}_deleted/"
        sparkLogger.info(f"Writing deletes to {newWritePath} partitioned by {partitions}")

        if partitions != []:
            dataFrame \
                .cache() \
                .write \
                .partitionBy(*partitions) \
                .parquet(mode=mode, path=newWritePath, compression='snappy')
        else:
             dataFrame \
                .cache() \
                .write \
                .parquet(mode=mode, path=newWritePath, compression='snappy')
       
    def writeDups(self, spark, conf, dataFrame, mode='append'):

        sparkLogger = Log4j(spark)

        splitPath = self._splitBasePath(spark, conf.basePath)
        # We want to put 'dup' in the path of the output
        # but keep the rest the same.
        # TODO: Note, even the boxer stuff will go here, which is not right. It should go into the 
        # boxer-data-lake instead, but this might be something for the future.
        modifiedS3Path = f"pnp-data-lake-{splitPath['env']}-etl-emr-workspace-euw1"

        newWritePath = f"s3://{modifiedS3Path}/{splitPath['cat']}/{splitPath['s3Prefix']}/{conf.tableName}/"
        sparkLogger.info(f"Writing duplicates to {newWritePath}")
        
        dataFrame \
            .coalesce(1) \
            .cache() \
            .write \
            .parquet(mode=mode, path=newWritePath, compression='snappy')

    def writeData(self, spark, conf, dataFrame, mode='append'):

        sparkLogger = Log4j(spark)

        writePath = f"{conf.basePath}"
        sparkLogger.info(f"Writing data to {writePath}")
        sparkLogger.info(f"Hudi config options for this dataset {conf.hudi_options}")

        # We've kept everything until this point to help us with troubleshooting
        # but now we need to ditch the stuff we don't want to write to S3
        # In theory we could add this to the front of the dataframe similar to the 
        # precombine_key, but I don't think this is required any longer
        
        dataFrame.write. \
            format("org.apache.hudi"). \
            options(**conf.hudi_options). \
            mode(mode). \
            save(writePath)

    def readRebatesCSV(self, spark, path, schema=None, header=True, sep="|"):
        # It could be that rebates has empty HBILDOC fields which might make it skip rows
        # which would be unlike the normal SALES data where HBILDOC cannot be null
        sparkLogger = Log4j(spark)
   
        df = (
            spark.read.csv(
                path = path,
                mode = 'PERMISSIVE',
                schema = schema,
                header = header,
                sep = sep,
                nullValue = '',
                enforceSchema = True)
            .withColumn("date_ingested", current_timestamp())
        )

        self.rows_read = df.count()
        if (self.rows_read > 0):
    
            # We didn't read any data from the file, so we don't want to do all this logic
            # Just throw a warning and continue
            newDf = df.withColumn('bucket', regexp_extract(input_file_name(), 's3://([^\/]+)/([\w\/]+)/([\w]+\.csv)', 1)) \
                             .withColumn('key', regexp_extract(input_file_name(), 's3://([^\/]+)/([\w\/]+)/([\w]+\.csv)', 2)) \
                             .withColumn('fileName', regexp_extract(input_file_name(), 's3://([^\/]+)/([\w\/]+)/([\w]+\.csv)', 3))

            thisFile = newDf.select('fileName', 'bucket', 'key', 'date_ingested').limit(1).collect()
            fname, bucket, key, date_ingested = thisFile[0].__getitem__('fileName'), \
                                                thisFile[0].__getitem__('bucket'), \
                                                thisFile[0].__getitem__('key'), \
                                                thisFile[0].__getitem__('date_ingested')

            loadType = fname.split('_')[2]
    
            self.fileType = loadType
            self.fileName = fname
            self.bucket = bucket
            self.key = key

            if (loadType == 'DLT'):
                sparkLogger.info(f"Reading DELTA file {fname} from bucket s3://{bucket}/{key} on {date_ingested}")
            elif (loadType == 'FUL'):
                sparkLogger.info(f"Reading FULL file {fname} from bucket s3://{bucket}/{key} on {date_ingested}")
                
            sparkLogger.info(f"Rows read: {self.rows_read}")

            finDf = newDf.drop('loadType', 'bucket', 'key')

        else:
            sparkLogger.info(f"There were {self.rows_read} rows read in the path {path}")
            finDf = spark.createDataFrame(spark.SparkContext.emptyRDD(), schema)
        return(finDf) 

    def insertInto(self, ele, li, **kwargs):
        """
        We may need this when manipulating the list of columns
        We may want to add an element into a list at either a position or
        before or after some speficied element
        """
        if 'pos' in kwargs:
            p = kwargs['pos']
            newli = li[0:p] + [ele] + li[p:]
        elif 'where' in kwargs and 'thisEle' in kwargs:
            # If we're trying to put the element before or after some other element
            # kwargs should contain both "where" and "thisEle"
            # where "where" could be "before" or "after"
            p = li.index(kwargs['thisEle'])
            if kwargs['where'] == 'before':
                newli = li[0:p] + [ele] + li[p:]
            elif kwargs['where'] == 'after':
                newli = li[0:p+1] + [ele] + li[p+1:]

        return newli

    def countColumns(self, spark, path, header=True, sep="|"):

        """In order to try to figure out which schema we're going to require in the reading
           of this data, we read 2 rows from the path and we count the number of columns.
           We return the read dataFrame as well as the number of columns for this dataFrame
        """

        df = (
            spark.read.csv(
                path = path,
                mode = 'PERMISSIVE',
                inferSchema = True,
                header = header,
                sep = sep,
                nullValue = '')
            .limit(2)
        )

        # Check if _hoodie_is_deleted is a column in the list of columns
        if '_hoodie_is_deleted' in df.columns:
            hoodie_is_deleted = True
        else:
            hoodie_is_deleted = False

        numCols = len(df.columns)
            
        return (df, numCols, hoodie_is_deleted)

    def readCSV(self, spark, path, schema=None, header=True, sep="|"):
        sparkLogger = Log4j(spark)

        if schema == None:
            df = (
                spark.read.csv(
                    path = path,
                    mode = 'PERMISSIVE',
                    header = header,
                    sep = sep,
                    nullValue = '',
                    enforceSchema = True)
                .withColumn("date_ingested", current_timestamp())
            )
        else:   
            df = (
                spark.read.csv(
                    path = path,
                    mode = 'PERMISSIVE',
                    schema = schema,
                    header = header,
                    sep = sep,
                    nullValue = '',
                    enforceSchema = True)
                .withColumn("date_ingested", current_timestamp())
            )

        self.rows_read = df.count()

        sparkLogger.info(f"Rows read: {self.rows_read}")
        if (self.rows_read > 0):
    
            # We didn't read any data from the file, so we don't want to do all this logic
            # Just throw a warning and continue
            newDf = df.withColumn('bucket', regexp_extract(input_file_name(), 's3://([^\/]+)/([\w\/]+)/([\w]+\.csv)', 1)) \
                      .withColumn('key', regexp_extract(input_file_name(), 's3://([^\/]+)/([\w\/]+)/([\w]+\.csv)', 2)) \
                      .withColumn('fileName', regexp_extract(input_file_name(), 's3://([^\/]+)/([\w\/]+)/([\w]+\.csv)', 3))

            #newDf = df.withColumn('bucket', regexp_extract(input_file_name(), 's3://([^\/]+)/([\w\/]+)/([\w]+\.csv)', 1)) \
            #          .withColumn('key', regexp_extract(input_file_name(), 's3://([^\/]+)/([\w\/]+)/([\w]+\.csv)', 2)) \
            #          .withColumn('fileName', regexp_extract(input_file_name(), 's3://([^\/]+)/([\w\/]+)/([\w]+\.csv)', 3))

            thisFile = newDf.select('fileName', 'bucket', 'key', 'date_ingested').limit(1).collect()
            sparkLogger.warn(thisFile)

            fname, bucket, key, date_ingested = thisFile[0].__getitem__('fileName'), \
                                                thisFile[0].__getitem__('bucket'), \
                                                thisFile[0].__getitem__('key'), \
                                                thisFile[0].__getitem__('date_ingested')

            sparkLogger.warn(f'filename: {fname}\tbucket {bucket}\tkey {key}\tdate_ingested {date_ingested}')

            try:
                loadType = fname.split('_')[2]
            except IndexError as e:
                sparkLogger.warn('Index error: probably could not find the filename')
            sparkLogger.info(f"Dataframe Rows:\t{self.rows_read}\nFilename:\t{fname}\nBucket:\t\t{bucket}\nKey:\t\t{key}\nDate Ingested\t{date_ingested}\nLoad Type:\t{loadType}")
    
            self.fileType = loadType
            self.fileName = fname
            self.bucket = bucket
            self.key = key

            if (loadType == 'DLT'):
                sparkLogger.info(f"Reading DELTA file {fname} from bucket s3://{bucket}/{key} on {date_ingested}")
            elif (loadType == 'FUL'):
                sparkLogger.info(f"Reading FULL file {fname} from bucket s3://{bucket}/{key} on {date_ingested}")
                
            sparkLogger.info(f"Rows read: {self.rows_read}")

            finDf = newDf.drop('fn', 'loadType', 'bucket', 'key')

        else:
            sparkLogger.info(f"There were {self.rows_read} rows read in the path {path}")
            finDf = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
        return(finDf) 