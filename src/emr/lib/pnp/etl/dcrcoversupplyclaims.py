"""
ACZ: s3://pnp-data-lake-prod-acz-retail-euw1/erp/td/dcrc_ovh/
STZ: s3://pnp-data-lake-prod-stz-retail-euw1/td/dcrc/erp_dcrc_ovh
"""

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, DecimalType
from pnp.etl.datasets import dataset

class dcrcoversupplyclaimsData(dataset):

    def __init__(self, configParams):
        super().__init__(configParams)

        self.hudi_options = {}

        # Join all the hudi_options for this dataset
        self.hudi_options.update(dataset.hudi_options)
        
        # Specific hudi_options per dataset
        self.local_hudi_options = {
          'hoodie.datasource.write.recordkey.field': 'MANDT,DCRC_ID,TKNUM_OR_GROUP,POSNR',
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
            StructField("WERKS", StringType(),False),
            StructField("TKNUM_OR_GROUP", StringType(),False),
            StructField("POSNR", StringType(),False),
            StructField("VENUM", StringType(),True),
            StructField("EXIDV", StringType(),True),
            StructField("DU_EAN11", StringType(),True),
            StructField("MATNR", StringType(),True),
            StructField("DU_QUANTITY", DecimalType(13,3),True),
            StructField("VRKME", StringType(),True)])

        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 13
        self.schemaDict[1]['validFrom'] = '19000101'
        self.schemaDict[1]['validUntil'] = '29991231'