"""
ACZ: s3://pnp-data-lake-prod-acz-retail-euw1/erp/td/dcrc_hd
STZ: s3://pnp-data-lake-prod-stz-retail-euw1/td/dcrc/erp_dcrc_hd
"""

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType
from pnp.etl.datasets import dataset

class dcrcheaderData(dataset):

    def __init__(self, configParams):
        super().__init__(configParams)

        self.hudi_options = {}

        # Join all the hudi_options for this dataset
        self.hudi_options.update(dataset.hudi_options)
        
        # Specific hudi_options per dataset
        self.local_hudi_options = {
          'hoodie.datasource.write.recordkey.field': 'MANDT,DCRC_ID',
          'hoodie.datasource.write.partitionpath.field': 'WERKS',
          'hoodie.datasource.hive_sync.partition_fields': 'WERKS'
        }

        # Join all the hudi_options for this dataset
        self.hudi_options.update(self.local_hudi_options)

        self.datasetSchemaEvo = True

        self.Schema = StructType([
            StructField("DI_REQTSN", StringType(),False),
            StructField("DI_SEQ_NR", IntegerType(),False),
            StructField("MANDT", StringType(),False),
            StructField("DCRC_ID", StringType(),False),
            StructField("VZWRK", StringType(),True),
            StructField("WERKS", StringType(),True),
            StructField("DCRC_TYPE", StringType(),True),
            StructField("DCRC_REASON", StringType(),True),
            StructField("DCRC_LEVEL", StringType(),True),
            StructField("TKNUM", StringType(),True),
            StructField("VBELN", StringType(),True),
            StructField("NAV_VERS", StringType(),True),
            StructField("DGRP", StringType(),True),
            StructField("DCRC_PROCESS", StringType(),True),
            StructField("STATUS", StringType(),True),
            StructField("EWMSH", StringType(),True),
            StructField("CRESRC", StringType(),True),
            StructField("RRSITE", StringType(),True),
            StructField("TRIP_NUM", StringType(),True)])

        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 19
        self.schemaDict[1]['validFrom'] = '19000101'
        self.schemaDict[1]['validUntil'] = '29991231'