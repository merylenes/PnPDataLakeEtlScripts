"""
ACZ:  s3://pnp-data-lake-prod-acz-finance-euw1/bw/td/hfglm01
STZ:  s3://pnp-data-lake-prod-stz-finance-euw1/td/cos/cos_bw_hfglm01
"""

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, LongType, DecimalType
from pyspark.sql import functions as F

from pnp.etl.datasets import dataset

class valuatedcosData(dataset):

    def __init__(self, configParams):
        super().__init__(configParams)

        # Set the specific hudi_options for this class
        self.hudi_options = {}

        # Join these hudi_options to the class variable dictionary
        self.hudi_options.update(dataset.hudi_options)

        self.local_hudi_options = {
          'hoodie.datasource.write.recordkey.field': 'HPLANT,HMATERIAL,CALDAY,SALES_UNIT,HPYEQFLG',
          'hoodie.datasource.write.partitionpath.field': 'CALDAY',
          'hoodie.datasource.hive_sync.partition_fields': 'CALDAY'
        }

        # Join all the hudi_options for this dataset
        self.hudi_options.update(self.local_hudi_options)

        self.datasetSchemaEvo = True

        self.Schema = StructType([
            StructField("DI_REQTSN", StringType(),False),
            StructField("DI_SEQ_NR", IntegerType(),False),
            StructField("HPLANT", StringType(),False),
            StructField("HMATERIAL", StringType(),False),
            StructField("CALDAY", LongType(),False),
            StructField("SALES_UNIT", StringType(),False),
            StructField("HPYEQFLG", IntegerType(), False),
            StructField("HCO_AREA", StringType(),True),
            StructField("HPROFCTR", StringType(),True),
            StructField("LOC_CURRCY", StringType(),True),
            StructField("HFCVMLC", DecimalType(17,2),True),
            StructField("HFCVMLLC", DecimalType(17,2),True),
            StructField("HFCVOLC", DecimalType(17,2),True),
            StructField("HFCVOLLC", DecimalType(17,2),True)])
        
        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 13
        self.schemaDict[1]['validFrom'] = '19000101'
        self.schemaDict[1]['validUntil'] = '20230508'
        
        self.schemaDict[2] = {'ver': 2}
        self.schemaDict[2]['cols'] = 14
        self.schemaDict[2]['validFrom'] = '20230508'
        self.schemaDict[2]['validUntil'] = '99991231'

    