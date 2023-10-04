import re, sys, json
from tokenize import String

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DoubleType, LongType, TimestampType, DecimalType
from pyspark.sql.functions import col, concat, substring, format_string, regexp_replace

from pnp.etl.datasets import dataset

"""
ACZ: s3://pnp-data-lake-dev-acz-pos-euw1/bw/td/hpfmw01/
STZ: s3://pnp-data-lake-dev-stz-pos-euw1/td/pos/pos_bw_hpfmw01/
"""
class posfinancialmovementsw01Data(dataset):

    def __init__(self, configParams):
        super().__init__(configParams)

        self.hudi_options = {}

        # Join all the hudi_options for this dataset
        self.hudi_options.update(dataset.hudi_options)

        # Specific hudi_options per dataset
        self.local_hudi_options = {
          'hoodie.datasource.write.recordkey.field': 'HPATXTIME,HPLANT,HPAWID,HPATNR,HPARQU,HPAPRVIND,HPAFTXIN',
          'hoodie.datasource.write.partitionpath.field': 'CALDAY',
          'hoodie.datasource.hive_sync.partition_fields': 'CALDAY'
        }

        # Join all the hudi_options for this dataset
        self.hudi_options.update(self.local_hudi_options)
        
        # For absolutely brand new datasets, the datasetSchemaEvo should be set to True always
        # because it's used in testing for a variety of things in the codebase.

        self.datasetSchemaEvo = True
        # There can be only one ... schema, which is increasing in columns
        
        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 21
        self.schemaDict[1]['validFrom'] = '20220921' # This is set to an arbitratry date in the past
        self.schemaDict[1]['validUntil'] = '20240801' # This is ostensibly set to whatever date this version of schema applies until
        
        self.Schema = StructType([
            StructField("DI_REQTSN", StringType(),False),
            StructField("DI_SEQ_NR", IntegerType(),False),
            StructField("CALDAY", LongType(),False),
            StructField("HPATXTIME", StringType(),False),
            StructField("HPLANT", StringType(),False),
            StructField("HPAWID", StringType(),False),
            StructField("HPATNR", StringType(),False),
            StructField("HPARQU", StringType(),False),
            StructField("HPAPRVIND", StringType(),False),
            StructField("HPAFTXIN", StringType(),False),
            StructField("HPABTY", StringType(),True),
            StructField("HPATTG", StringType(),True),
            StructField("HPATTC", StringType(),True),
            StructField("HPACSHEMP", StringType(),True),
            StructField("HPACSHRID", StringType(),True),
            StructField("HPAFTXCAT", StringType(),True),
            StructField("HPAFTXTG", StringType(),True),
            StructField("HPAFTXET", StringType(),True),
            StructField("HPAFTPTYP", StringType(),True),
            StructField("LOC_CURRCY", StringType(),True),
            StructField("HPAFTPVLC", DecimalType(17,2),True)])