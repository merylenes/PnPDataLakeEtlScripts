"""
ACZ: s3://pnp-data-lake-prod-acz-retail-euw1/erp/td/dcrc_it/
STZ: s3://pnp-data-lake-prod-stz-retail-euw1/td/dcrc/erp_dcrc_it
"""

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, LongType
from pnp.etl.datasets import dataset

class dcrcticketstatusData(dataset):

    def __init__(self, configParams):
        super().__init__(configParams)

        self.hudi_options = {}

        # Join all the hudi_options for this dataset
        self.hudi_options.update(dataset.hudi_options)

        # Specific hudi_options per dataset
        self.local_hudi_options = {
          'hoodie.datasource.write.recordkey.field': 'MANDT,DCRC_ID,COUNTER',
          'hoodie.datasource.write.partitionpath.field': 'DATUM',
          'hoodie.datasource.hive_sync.partition_fields': 'DATUM'
        }

        # Join all the hudi_options for this dataset
        self.hudi_options.update(self.local_hudi_options)

        self.datasetSchemaEvo = True

        self.Schema = StructType([
            StructField("DI_REQTSN", StringType(),False),
            StructField("DI_SEQ_NR", IntegerType(),False),
            StructField("MANDT", StringType(),False),
            StructField("DCRC_ID", StringType(),False),
            StructField("COUNTER", StringType(),False),
            StructField("STATUS", StringType(),True),
            StructField("DATUM", LongType(),True),
            StructField("UZEIT", LongType(),True),
            StructField("ERNAM", StringType(),True),
            StructField("NAVREASTYPE", StringType(),True),
            StructField("NAVREASON", StringType(),True)])

        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 11
        self.schemaDict[1]['validFrom'] = '19000101'
        self.schemaDict[1]['validUntil'] = '29991231'
