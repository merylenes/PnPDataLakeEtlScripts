from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import countDistinct
import os, sys, re, json, argparse, socket
from pnp import logging, utils
from pnp.etl import *

from pyspark.sql import Window
from pyspark.sql.functions import input_file_name, current_timestamp, split, regexp_extract, collect_list, row_number, col, count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DoubleType, LongType, TimestampType, DecimalType


#    print(f"beginning time set to: {beginTime}")
#    return beginTime

if __name__ == '__main__':

    spark = SparkSession \
            .builder \
            .getOrCreate()
    
    sparkLogger = logging.Log4j(spark)

    try:
        schema = StructType([
            StructField("HMATERIAL", StringType(), False),
            StructField("HMATERIAL___T", StringType(), False),
            StructField("BASE_UOM1", StringType(), False),
            StructField("BASE_UOM___T", StringType(), False),
            StructField("CONT_UNIT", StringType(), False),
            StructField("CONT_UNIT___T", StringType(), False),
            StructField("PO_UNIT", StringType(), False),
            StructField("PO_UNIT___T", StringType(), False),
            StructField("SALES_UNIT1", StringType(), False),
            StructField("SALES_UNIT___T", StringType(), False),
            StructField("UNIT_DIM", StringType(), False),
            StructField("UNIT_DIM___T", StringType(), False),
            StructField("UNIT_OF_WT", StringType(), False),
            StructField("UNIT_OF_WT___T", StringType(), False),
            StructField("VOLUMEUNIT", StringType(), False),
            StructField("VOLUMEUNIT___T", StringType(), False),
            StructField("HADELIND", StringType(), False),
            StructField("HADJPROF", StringType(), False),
            StructField("HAH_CDT1", StringType(), False),
            StructField("HAH_CDT1___T", StringType(), False),
            StructField("HAH_CDT2", StringType(), False),
            StructField("HAH_CDT2___T", StringType(), False),
            StructField("HAH_CDT3", StringType(), False),
            StructField("HAH_CDT3___T", StringType(), False),
            StructField("HAH_CDT4", StringType(), False),
            StructField("HAH_CDT4___T", StringType(), False),
            StructField("HAH_CDT5", StringType(), False),
            StructField("HAH_CDT5___T", StringType(), False),
            StructField("HAH_CDT6", StringType(), False),
            StructField("HAH_CDT6___T", StringType(), False),
            StructField("HAH_CDT7", StringType(), False),
            StructField("HAH_CDT7___T", StringType(), False),
            StructField("HARTSTAT", StringType(), False),
            StructField("HARTSTAT___T", StringType(), False),
            StructField("HART_ICAT", StringType(), False),
            StructField("HART_ICAT___T", StringType(), False),
            StructField("HASA_GRP1", StringType(), False),
            StructField("HASA_GRP1___T", StringType(), False),
            StructField("HASA_GRP2", StringType(), False),
            StructField("HASA_GRP2___T", StringType(), False),
            StructField("HAVABC", StringType(), False),
            StructField("HAVIND", StringType(), False),
            StructField("HBMCGRP", StringType(), False),
            StructField("HBMCGRP___T", StringType(), False),
            StructField("HBOTANIC", StringType(), False),
            StructField("HBRANDNAM", StringType(), False),
            StructField("HBRANDNAM___T", StringType(), False),
            StructField("HBUHEAD", StringType(), False),
            StructField("HBUHEAD___T", StringType(), False),
            StructField("HCATHEAD", StringType(), False),
            StructField("HCATHEAD___T", StringType(), False),
            StructField("HCLAFSDAT", LongType(), False),
            StructField("HCLAINQDT", StringType(), False),
            StructField("HCLAINQDT___T", StringType(), False),
            StructField("HCLALCDAT", LongType(), False),
            StructField("HCLAOBRTP", StringType(), False),
            StructField("HCLAOBRTP___T", StringType(), False),
            StructField("HCLAPFLIO", StringType(), False),
            StructField("HCLAPFLIO___T", StringType(), False),
            StructField("HCLAPLDAT", LongType(), False),
            StructField("HCLASTRGY", StringType(), False),
            StructField("HCLASTRGY___T", StringType(), False),
            StructField("HCLATHEME", StringType(), False),
            StructField("HCLATHEME___T", StringType(), False),
            StructField("HCLPRIFID", StringType(), False),
            StructField("HCLPRIFID___T", StringType(), False),
            StructField("HCM_CDT1", StringType(), False),
            StructField("HCM_CDT1___T", StringType(), False),
            StructField("HCM_CDT2", StringType(), False),
            StructField("HCM_CDT2___T", StringType(), False),
            StructField("HCM_CDT3", StringType(), False),
            StructField("HCM_CDT3___T", StringType(), False),
            StructField("HCM_CDT4", StringType(), False),
            StructField("HCM_CDT4___T", StringType(), False),
            StructField("HCM_HIEID", StringType(), False),
            StructField("HCM_HIEID___T", StringType(), False),
            StructField("HCNTRYORI", StringType(), False),
            StructField("HCNTRYORI___T", StringType(), False),
            StructField("HCOMIMPCD", StringType(), False),
            StructField("HCOMIMPCD___T", StringType(), False),
            StructField("HCOUPXIND", StringType(), False),
            StructField("HCREATDAT", LongType(), False),
            StructField("HDIVHEAD", StringType(), False),
            StructField("HDIVHEAD___T", StringType(), False),
            StructField("HEANUPC", StringType(), False),
            StructField("HESENTIAL", StringType(), False),
            StructField("HESENTIAL___T", StringType(), False),
            StructField("HEXTMATGR", StringType(), False),
            StructField("HEXTMATGR___T", StringType(), False),
            StructField("HFABRIC", StringType(), False),
            StructField("HFABRIC___T", StringType(), False),
            StructField("HGENART", StringType(), False),
            StructField("HGENART___T", StringType(), False),
            StructField("HGRPLEVEL", StringType(), False),
            StructField("HGRPLEVEL___T", StringType(), False),
            StructField("HHANDIND", StringType(), False),
            StructField("HHANUNTP", StringType(), False),
            StructField("HHOUSEBEQ", StringType(), False),
            StructField("HHOUSEBEQ___T", StringType(), False),
            StructField("HHSEBCLS", StringType(), False),
            StructField("HIMPIND", StringType(), False),
            StructField("HINDAUTU", StringType(), False),
            StructField("HINDDAYM", StringType(), False),
            StructField("HINDDAYMA", StringType(), False),
            StructField("HINDEASL", StringType(), False),
            StructField("HINDHALAA", StringType(), False),
            StructField("HINDHRTMA", StringType(), False),
            StructField("HINDHSEBR", StringType(), False),
            StructField("HINDKOSH", StringType(), False),
            StructField("HINDKVI", StringType(), False),
            StructField("HINDLAUIT", StringType(), False),
            StructField("HINDLIVE", StringType(), False),
            StructField("HINDMOMUL", StringType(), False),
            StructField("HINDMORQ", StringType(), False),
            StructField("HINDNEGPR", StringType(), False),
            StructField("HINDNRECO", StringType(), False),
            StructField("HINDPERKG", StringType(), False),
            StructField("HINDRVAL", StringType(), False),
            StructField("HINDVEGE", StringType(), False),
            StructField("HINDWLESS", StringType(), False),
            StructField("HLABOR", StringType(), False),
            StructField("HLABOR___T", StringType(), False),
            StructField("HLAUNLQTY", StringType(), False),
            StructField("HLGUTIND", StringType(), False),
            StructField("HLOADGRP", StringType(), False),
            StructField("HLOADGRP___T", StringType(), False),
            StructField("HMATCOLOR", StringType(), False),
            StructField("HMATLTYPE", StringType(), False),
            StructField("HMATLTYPE___T", StringType(), False),
            StructField("HMATL_CAT", StringType(), False),
            StructField("HMATL_CAT___T", StringType(), False),
            StructField("HMATL_GRP", StringType(), False),
            StructField("HMATL_GRP___T", StringType(), False),
            StructField("HMATSIZE", StringType(), False),
            StructField("HMEATCLAS", StringType(), False),
            StructField("HMEATCLAS___T", StringType(), False),
            StructField("HNAPPI", StringType(), False),
            StructField("HNAPPICOD", StringType(), False),
            StructField("HOBJECTN", StringType(), False),
            StructField("HOLDARTNO", StringType(), False),
            StructField("HOPERSEG", StringType(), False),
            StructField("HOPERSEG___T", StringType(), False),
            StructField("HORDAPSI", StringType(), False),
            StructField("HORDROUTE", StringType(), False),
            StructField("HORDROUTE___T", StringType(), False),
            StructField("HPILFIND", StringType(), False),
            StructField("HPREPRICE", StringType(), False),
            StructField("HPRFAMGP", StringType(), False),
            StructField("HPRFAMGP___T", StringType(), False),
            StructField("HPRODDRVR", StringType(), False),
            StructField("HPRODDRVR___T", StringType(), False),
            StructField("HPURVAL", StringType(), False),
            StructField("HQINSGRP", StringType(), False),
            StructField("HQPNNUM", StringType(), False),
            StructField("HREGORIG", StringType(), False),
            StructField("HREGORIG___T", StringType(), False),
            StructField("HREQARTTP", StringType(), False),
            StructField("HREQARTTP___T", StringType(), False),
            StructField("HREQUESBY", StringType(), False),
            StructField("HREQUESBY___T", StringType(), False),
            StructField("HRTSEASA", StringType(), False),
            StructField("HRTSEASA___T", StringType(), False),
            StructField("HRTSEAYRA", StringType(), False),
            StructField("HSCHEDULE", StringType(), False),
            StructField("HSCHEDULE___T", StringType(), False),
            StructField("HSELLBY", StringType(), False),
            StructField("HSEWLABEL", StringType(), False),
            StructField("HSEWLABEL___T", StringType(), False),
            StructField("HSTORTYP", StringType(), False),
            StructField("HSTORTYP___T", StringType(), False),
            StructField("HTAXCLASS", StringType(), False),
            StructField("HTEMIND", StringType(), False),
            StructField("HTICKTYP", StringType(), False),
            StructField("HTICKTYP___T", StringType(), False),
            StructField("HTIM_UNIT", StringType(), False),
            StructField("HTIM_UNIT___T", StringType(), False),
            StructField("HUNISOLVE", StringType(), False),
            StructField("HUSEBY", StringType(), False),
            StructField("HVALFRAST", LongType(), False),
            StructField("HVALFRXDS", LongType(), False),
            StructField("HVALIDFRM", LongType(), False),
            StructField("HVALUTYP", StringType(), False),
            StructField("HVALUTYP___T", StringType(), False),
            StructField("HVARQTY", StringType(), False),
            StructField("HVARUOM", StringType(), False),
            StructField("HVARUOM___T", StringType(), False),
            StructField("HVARWGHT", StringType(), False),
            StructField("HVARWGHTU", StringType(), False),
            StructField("HVITALIND", StringType(), False),
            StructField("HWHSTCON", StringType(), False),
            StructField("HWMMERCAT", StringType(), False),
            StructField("HWMMERCAT___T", StringType(), False),
            StructField("HXDISSTAT", StringType(), False),
            StructField("HAVG_WT", DoubleType(), False),
            StructField("HBOMVRNTS", IntegerType(), False),
            StructField("HGROSS_CO", DoubleType(), False),
            StructField("HGROSS_WT", DoubleType(), False),
            StructField("HHEIGHT", DoubleType(), False),
            StructField("HLENGTH", DoubleType(), False),
            StructField("HMINSLR", DoubleType(), False),
            StructField("HNET_CO", DoubleType(), False),
            StructField("HNET_WT", DoubleType(), False),
            StructField("HVOLUME", DoubleType(), False),
            StructField("HWIDTH", DoubleType(), False)])

        df = (
            spark.read.csv(
                path = 's3://pnp-data-lake-dev-acz-retail-euw1/bw/md/article',
                mode = 'PERMISSIVE',
                schema = schema,
                header = True,
                sep = '|',
                nullValue = '',
                enforceSchema = True)
                .withColumn("date_ingested", current_timestamp())
        )

        df \
            .write \
            .parquet(mode='overwrite', path='s3://pnp-data-lake-dev-stz-retail-euw1/md/article/article_bw_article_01/', compression='snappy')

        sparkLogger.info(f"Record count = {df.count()}")
    except Exception as e:
        sparkLogger.error(f"There was an exception in trying to read the dataframe. Error {e}")