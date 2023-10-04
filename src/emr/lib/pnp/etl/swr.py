import re, sys, json

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DoubleType, LongType, TimestampType, DecimalType
from pyspark.sql.functions import col, concat, substring, format_string, regexp_replace

from pnp.etl.datasets import dataset

# ACZ: s3://pnp-data-lake-prod-acz-retail-euw1/bw/td/hrcfm01
# STZ: s3://pnp-data-lake-prod-stz-retail-euw1/td/swr/swr_bw_hrcfm01
class swrData(dataset):

    def __init__(self, configParams):
        super().__init__(configParams)

        # Specific hudi_options per dataset
        self.hudi_options = {
          "hoodie.datasource.write.recordkey.field": "HPLANT,HMATERIAL,HPYEQFLG",
          "hoodie.datasource.write.partitionpath.field": "CALDAY",
          "hoodie.datasource.hive_sync.partition_fields": "CALDAY"
        }

        # Join all the hudi_options for this dataset
        self.hudi_options.update(dataset.hudi_options)

        self.datasetSchemaEvo = True

        self.Schema = StructType([
            StructField("DI_REQTSN", StringType(), False), 
            StructField("DI_SEQ_NR", LongType(), False), 
            StructField("HPLANT", StringType(), False), 
            StructField("HMATERIAL", StringType(), False), 
            StructField("CALDAY", LongType(), False), 
            StructField("HPYEQFLG", IntegerType(), False),
            StructField("BASE_UOM", StringType(), True),
            StructField("LOC_CURRCY", StringType(), True),
            StructField("HDISC13LC", DecimalType(17,2), True),
            StructField("HDIS13LLC", DecimalType(17,2), True),
            StructField("HRTCASLC", DecimalType(17,2), True),
            StructField("HRTCASLLC", DecimalType(17,2), True),
            StructField("HCOSLC", DecimalType(17,2), True),
            StructField("HCOSLLC", DecimalType(17,2), True),
            StructField("HRTCQTYBU", DecimalType(17,3), True),
            StructField("HRTCQTLBU", DecimalType(17,3), True),
            StructField("HWAQTYBU", DecimalType(17,3), True),
            StructField("HWAQTYLBU", DecimalType(17,3), True),
            StructField("HWRQTYBU", DecimalType(17,3), True),
            StructField("HWRQTYLBU", DecimalType(17,3), True),
            StructField("HWZQTYBU", DecimalType(17,3), True),
            StructField("HWZQTYLBU", DecimalType(17,3), True),
            StructField("HSHQTYBU", DecimalType(17,3), True),
            StructField("HSHQTYLBU", DecimalType(17,3), True),
            StructField("HSUQTYBU", DecimalType(17,3), True),
            StructField("HSUQTYLBU", DecimalType(17,3), True),
            StructField("HSHCSTLC", DecimalType(17,2), True),
            StructField("HSHCSTLLC", DecimalType(17,2), True),
            StructField("HSHSELLC", DecimalType(17,2), True),
            StructField("HSHSELLLC", DecimalType(17,2), True),
            StructField("HSUCSTLC", DecimalType(17,2), True),
            StructField("HSUCSTLLC", DecimalType(17,2), True),
            StructField("HSUSELLC", DecimalType(17,2), True),
            StructField("HSUSELLLC", DecimalType(17,2), True),
            StructField("HWACSTLC", DecimalType(17,2), True),
            StructField("HWACSTLLC", DecimalType(17,2), True),
            StructField("HWASELLC", DecimalType(17,2), True),
            StructField("HWASELLLC", DecimalType(17,2), True),
            StructField("HWRCSTLC", DecimalType(17,2), True),
            StructField("HWRCSTLLC", DecimalType(17,2), True),
            StructField("HWRSELLC", DecimalType(17,2), True),
            StructField("HWRSELLLC", DecimalType(17,2), True),
            StructField("HWARCTLC", DecimalType(17,2), True),
            StructField("HWARCTLLC", DecimalType(17,2), True),
            StructField("HWZSELLC", DecimalType(17,2), True),
            StructField("HWZSELLLC", DecimalType(17,2), True),
            StructField("HWZCSTLC", DecimalType(17,2), True),
            StructField("HWZCSTLLC", DecimalType(17,2), True),
            StructField("HRTCOSVLC", DecimalType(17,2), True),
            StructField("HRTCOSLLC", DecimalType(17,2), True),
            StructField("HRTCSVLC", DecimalType(17,2), True),
            StructField("HRTCSVLLC", DecimalType(17,2), True),
            StructField("HSQTYBU", DecimalType(17,3), True),
            StructField("HSQTYLBU", DecimalType(17,3), True),
            StructField("HRTCSNLC", DecimalType(17,3), True),
            StructField("HRTCSNLLC", DecimalType(17,3), True)])

        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 54
        self.schemaDict[1]['validFrom'] = '20200906'
        self.schemaDict[1]['validUntil'] = '20230329'

        self.schemaDict[2] = {'ver': 2}
        self.schemaDict[2]['cols'] = 56
        self.schemaDict[2]['validFrom'] = '20230329'
        self.schemaDict[2]['validUntil'] = '99991231'