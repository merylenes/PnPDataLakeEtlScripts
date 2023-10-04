"""
ACZ: s3://pnp-data-lake-dev-acz-finance-euw1/erp/td/bkpf
STZ: s3://pnp-data-lake-dev-stz-finance-euw1/td/bkpf/bkpf_erp_bkpf
"""

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, LongType
from pnp.etl.datasets import dataset

class bkpfData(dataset):

    def __init__(self, configParams):
        super().__init__(configParams)

        self.hudi_options = {}

        # Join all the hudi_options for this dataset
        self.hudi_options.update(dataset.hudi_options)

        # Specific hudi_options per dataset
        self.local_hudi_options = {
            'hoodie.datasource.write.recordkey.field': 'BUKRS,BELNR,GJAHR',
            'hoodie.datasource.write.partitionpath.field': 'BUDAT',
            'hoodie.datasource.hive_sync.partition_fields': 'BUDAT',
            'hoodie.upsert.shuffle.parallelism': "50"
        }

        # Join all the hudi_options for this dataset
        self.hudi_options.update(self.local_hudi_options)

        self.datasetSchemaEvo = True

        self.Schema = StructType([
            StructField("DI_REQTSN", StringType(), True),
            StructField("DI_SEQ_NR", IntegerType(), True),
            StructField("BUKRS", StringType(), True),
            StructField("BELNR", StringType(), True),
            StructField("GJAHR", StringType(), True),
            StructField("BUDAT", LongType(), True),
            StructField("BKTXT", StringType(), True)])

        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 7
        self.schemaDict[1]['validFrom'] = '19000101'
        self.schemaDict[1]['validUntil'] = '29991231'
