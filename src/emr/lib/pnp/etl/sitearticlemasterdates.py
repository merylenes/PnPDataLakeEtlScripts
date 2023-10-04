import re, sys, json

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DoubleType, LongType, TimestampType, DecimalType
from pyspark.sql.functions import col, concat, substring, format_string, regexp_replace

from pnp.etl.datasets import dataset

class sitearticlemasterdatesData(dataset):

    def __init__(self, configParams):
        super().__init__(configParams)

        # Specific hudi_options per dataset
        self.hudi_options = {
          'hoodie.datasource.write.recordkey.field': 'HPLANT,HMATERIAL',
          'hoodie.datasource.write.partitionpath.field': 'HPLANT',
          'hoodie.datasource.hive_sync.partition_fields': 'HPLANT'
        }

        # Join all the hudi_options for this dataset
        self.hudi_options.update(dataset.hudi_options)
        
        # For absolutely brand new datasets, the datasetSchemaEvo should be set to True always
        # because it's used in testing for a variety of things in the codebase.

        self.datasetSchemaEvo = True
        # There can be only one ... schema, which is increasing in columns
        
        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 12
        self.schemaDict[1]['validFrom'] = '20220801' # This is set to an arbitratry date in the past
        self.schemaDict[1]['validUntil'] = '20240801' # This is ostensibly set to whatever date this version of schema applies until
        
        self.Schema = StructType([
            StructField("DI_REQTSN", StringType(),False),
            StructField("DI_SEQ_NR", IntegerType(),False),
            StructField("HPLANT", StringType(),False),
            StructField("HMATERIAL", StringType(),False),
            StructField("HDATLMOV", LongType(),True),
            StructField("HDATLORD", LongType(),True),
            StructField("HDATLREC", LongType(),True),
            StructField("HDATLSLD", LongType(),True),
            StructField("HDATKLMOV", DecimalType(17,0),True),
            StructField("HDATKLORD", DecimalType(17,0),True),
            StructField("HDATKLREC", DecimalType(17,0),True),
            StructField("HDATKLSLD", DecimalType(17,0),True)])