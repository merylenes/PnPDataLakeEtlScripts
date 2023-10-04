"""
ACZ: s3://pnp-data-lake-prod-acz-retail-euw1/erp/md/eord
STZ: s3://pnp-data-lake-prod-stz-retail-euw1/md/sitearticle/sitearticle_erp_eord_01
"""

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, LongType
from pnp.etl.datasets import dataset

class sitearticlesourcelisteordData(dataset):

    def __init__(self, configParams):
        super().__init__(configParams)

        self.hudi_options = {}

        # Join all the hudi_options for this dataset
        self.hudi_options.update(dataset.hudi_options)

        # Specific hudi_options per dataset
        self.local_hudi_options = {
            'hoodie.datasource.write.recordkey.field': 'MATNR,WERKS,ZEORD',
            'hoodie.datasource.write.partitionpath.field': 'WERKS',
            'hoodie.datasource.hive_sync.partition_fields': 'WERKS',
        }

        # Join all the hudi_options for this dataset
        self.hudi_options.update(self.local_hudi_options)

        self.datasetSchemaEvo = True

        self.Schema = StructType([
            StructField("DI_REQTSN", StringType(),False),
            StructField("DI_SEQ_NR", IntegerType(),False),
            StructField("MATNR", StringType(),False),
            StructField("WERKS", StringType(),False),
            StructField("ZEORD", StringType(),False),
            StructField("ERDAT", LongType(),False),
            StructField("ERNAM", StringType(),False),
            StructField("VDATU", LongType(),False),
            StructField("BDATU", LongType(),False),
            StructField("LIFNR", StringType(),False),
            StructField("FLIFN", StringType(),False),
            StructField("EBELN", StringType(),False),
            StructField("EBELP", StringType(),False),
            StructField("FEBEL", StringType(),False),
            StructField("RESWK", StringType(),False),
            StructField("FRESW", StringType(),False),
            StructField("EMATN", StringType(),False),
            StructField("NOTKZ", StringType(),False),
            StructField("EKORG", StringType(),False),
            StructField("VRTYP", StringType(),False),
            StructField("EORTP", StringType(),False),
            StructField("AUTET", StringType(),False),
            StructField("MEINS", StringType(),False),
            StructField("LOGSY", StringType(),False),
            StructField("SOBKZ", StringType(),False),
            StructField("SRM_CONTRACT_ID", StringType(),False),
            StructField("SRM_CONTRACT_ITM", StringType(),False)])

        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 27
        self.schemaDict[1]['validFrom'] = '19000101'
        self.schemaDict[1]['validUntil'] = '29991231'
        