import pyspark
import os, sys, re, json
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, DoubleType, LongType, TimestampType, DecimalType
from pyspark.sql.functions import countDistinct, col, lit, concat, substring, format_number, format_string, input_file_name, current_timestamp, split, regexp_extract, collect_list, row_number, to_date, date_format, count, desc
from functools import partial
import argparse

from pnp.logging import Log4j
from pnp.utils import utils
from pnp.etl import *

parser = argparse.ArgumentParser()

group1 = parser.add_argument_group('independent config',description='Specifying the config as independent parameters')

group1.add_argument("--tmpSTZ", "-d", help="The destination URI for the temp STZ zone for the data")
group1.add_argument("--sourceURI", "-s", help="The source URI for the raw data")
group1.add_argument("--database", "-db", help="The Glue catalog database to write data to")
group1.add_argument("--tableName", "-t", help="The Glue catalog table to write data to")
group1.add_argument("--datasetName", "-dn", help="The name of the dataset.")
group1.add_argument("--schemaVer", "-sv", help="The version of the Schema to use. Consult the class to work out which version of schema to use")
group1.add_argument("--hiveServer", "-hs", help="The IP/hostname of the Hive Server (used for EC2).")
    
args = parser.parse_args()

spark = SparkSession \
          .builder \
          .getOrCreate()

sparkLogger = Log4j(spark)
sparkLogger.info("PySpark script logger initialised")
dSet = args.datasetName.lower()
dSetData = dSet + "Data"

thisSet = globals()[dSet]
thisData = getattr(thisSet, dSetData)

sparkLogger.info(f"{thisData.hudi_options}")
datasetConfOrig = { "datasetName": f"{args.datasetName}",
            "destSTZ": f"{args.tmpSTZ}",
            "sourceURI": f"{args.sourceURI}",
            "database": f"{args.database}",
            "tableName": f"{args.tableName}",
            "schemaVer": args.schemaVer}

inputData = thisData(datasetConfOrig)

df1 = (spark \
        .read \
        .format('hudi') \
        .load(f"{args.sourceURI}"))

cols = df1.columns

hoodie_cols = ['_hoodie_commit_time', '_hoodie_commit_seqno', '_hoodie_record_key', '_hoodie_partition_path', '_hoodie_file_name']

# Remove our old metadata columns
for ele in ['fileName', 'date_ingested', 'precombine_key']:
    cols.remove(ele)

# Remove the hoodie columns from the list of columns
for ele in hoodie_cols:
    cols.remove(ele)

remaining_cols = cols    

print(cols)

order_cols = hoodie_cols + ['precombine_key', 'fileName', 'date_ingested'] + remaining_cols

print(order_cols)

# Now select the columns in the new order we want them in

rearrangedColsDf = df1.select(order_cols)

rearrangedColsDf.printSchema()

hudi_options = {'hoodie.datasource.write.keygenerator.class': 'org.apache.hudi.keygen.ComplexKeyGenerator',
                'hoodie.datasource.write.operation': 'upsert',
                'hoodie.datasource.write.precombine.field': 'precombine_key',
                'hoodie.datasource.hive_sync.enable': 'true',
                'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.MultiPartKeysValueExtractor',
                'hoodie.parquet.compression.codec': 'snappy',
                'hoodie.datasource.hive_sync.use_jdbc': 'true',
                'hoodie.memory.merge.fraction': '0.80',
                'hoodie.table.name': f"{args.tableName}",
                'hoodie.datasource.write.table.name': f"{args.tableName}",
                'hoodie.datasource.hive_sync.database': f"{args.database}",
                'hoodie.datasource.hive_sync.table': f"{args.tableName}",
                'hoodie.index.type': 'SIMPLE',
                'hoodie.datasource.write.recordkey.field': f"{inputData.hudi_options['hoodie.datasource.write.recordkey.field']}",
                'hoodie.datasource.write.partitionpath.field': f"{inputData.hudi_options['hoodie.datasource.write.partitionpath.field']}",
                'hoodie.datasource.hive_sync.partition_fields': f"{inputData.hudi_options['hoodie.datasource.hive_sync.partition_fields']}",
                "hoodie.datasource.hive_sync.jdbcurl": f"jdbc:hive2://{args.hiveServer}:10000/"}

print(hudi_options)

rearrangedColsDf \
    .write \
    .format("org.apache.hudi") \
    .options(**hudi_options) \
    .mode('append') \
    .save(f"{args.tmpSTZ}")

