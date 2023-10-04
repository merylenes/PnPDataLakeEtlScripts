import re, sys, json
from tokenize import String

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DoubleType, LongType, TimestampType, DecimalType
from pyspark.sql.functions import col, concat, substring, format_string, regexp_replace

from pnp.etl.datasets import dataset

"""
ACZ: s3://pnp-data-lake-dev-acz-retail-euw1/bw/td/hrsam12xx/
STZ: s3://pnp-data-lake-dev-stz-retail-euw1/td/sales/sales_bw_hrsam12/
"""
class pnpdcmarginplanData(dataset):

    def __init__(self, configParams):
        super().__init__(configParams)

        self.hudi_options = {}

        # Join all the hudi_options for this dataset
        self.hudi_options.update(dataset.hudi_options)

        # Specific hudi_options per dataset
        self.local_hudi_options = {
          'hoodie.datasource.write.recordkey.field': 'CALDAY,HPLANT,HAH_CDT6',
          'hoodie.datasource.write.partitionpath.field': 'CALDAY',
          'hoodie.datasource.hive_sync.partition_fields': 'CALDAY',
          'hoodie.cleaner.policy': 'KEEP_LATEST_FILE_VERSIONS',
          'hoodie.cleaner.commits.retained': 1,
          'hoodie.keep.min.commits': 2,
          'hoodie.keep.max.commits': 5,
          'hoodie.datasource.write.operation': 'insert_overwrite',
          'hoodie.combine.before.insert': 'true'
        }

        # Join all the hudi_options for this dataset
        self.hudi_options.update(self.local_hudi_options)
        
        # For absolutely brand new datasets, the datasetSchemaEvo should be set to True always
        # because it's used in testing for a variety of things in the codebase.

        self.datasetSchemaEvo = True
        # There can be only one ... schema, which is increasing in columns
        
        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 10
        self.schemaDict[1]['validFrom'] = '20220921' # This is set to an arbitratry date in the past
        self.schemaDict[1]['validUntil'] = '20240801' # This is ostensibly set to whatever date this version of schema applies until
        
        self.Schema = StructType([
            StructField("DI_REQTSN", StringType(),False),
            StructField("DI_SEQ_NR", IntegerType(),False),
            StructField("CALDAY", LongType(),False),
            StructField("HPLANT", StringType(),False),
            StructField("HAH_CDT6", StringType(),False),
            StructField("HCM_HIEID", StringType(),False),
            StructField("HCM_CDT1", StringType(),True),
            StructField("HCM_CDT2", StringType(),True),
            StructField("LOC_CURRCY", StringType(),True),
            StructField("HDCPRPLC", DecimalType(17,2),True)])