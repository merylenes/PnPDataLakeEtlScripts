import re, sys, json
from tokenize import String

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DoubleType, LongType, TimestampType, DecimalType
from pyspark.sql.functions import col, concat, substring, format_string, regexp_replace

from pnp.etl.datasets import dataset

"""
ACZ: s3://pnp-data-lake-dev-acz-crm-euw1/ems/md/customer_key/
STZ: s3://pnp-data-lake-dev-stz-crm-euw1/md/customer/customer_ems_customer_key_01/
"""
class emarsyscustomerkeyData(dataset):

    def __init__(self, configParams):
        super().__init__(configParams)

        self.hudi_options = {}

        # Join all the hudi_options for this dataset
        self.hudi_options.update(dataset.hudi_options)

        # Specific hudi_options per dataset
        self.local_hudi_options = {
          'hoodie.datasource.write.recordkey.field': 'USER_ID,BP_NUMBER',
          'hoodie.datasource.write.partitionpath.field': 'LOADED_AT',
          'hoodie.datasource.hive_sync.partition_fields': 'LOADED_AT',
          'hoodie.cleaner.policy': 'KEEP_LATEST_FILE_VERSIONS',
          'hoodie.cleaner.commits.retained': 1,
          'hoodie.keep.min.commits': 2,
          'hoodie.keep.max.commits': 5
        }

        # Join all the hudi_options for this dataset
        self.hudi_options.update(self.local_hudi_options)
        
        # For absolutely brand new datasets, the datasetSchemaEvo should be set to True always
        # because it's used in testing for a variety of things in the codebase.

        self.datasetSchemaEvo = True
        # There can be only one ... schema, which is increasing in columns
        
        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 5
        self.schemaDict[1]['validFrom'] = '20220921' # This is set to an arbitratry date in the past
        self.schemaDict[1]['validUntil'] = '20240801' # This is ostensibly set to whatever date this version of schema applies until
        
        self.Schema = StructType([
            StructField("DI_REQTSN", StringType(),False),
            StructField("DI_SEQ_NR", IntegerType(),False),
            StructField("USER_ID", IntegerType(),True),
            StructField("BP_NUMBER", StringType(),True),
            StructField("LOADED_AT", LongType(),True)])