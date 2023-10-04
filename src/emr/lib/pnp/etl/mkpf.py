"""
ACZ: s3://pnp-data-lake-prod-acz-retail-euw1/erp/td/mkpf/
STZ: s3://pnp-data-lake-prod-stz-retail-euw1/td/mkpf/mkpf_erp_mkpf/
"""

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, LongType
from pnp.etl.datasets import dataset

class mkpfData(dataset):

    def __init__(self, configParams):
        super().__init__(configParams)

        self.hudi_options = {}

        # Join all the hudi_options for this dataset
        self.hudi_options.update(dataset.hudi_options)

        # Specific hudi_options per dataset
        self.local_hudi_options = {
            'hoodie.datasource.write.recordkey.field': 'MBLNR,MJAHR',
            'hoodie.datasource.write.partitionpath.field': 'BUDAT',
            'hoodie.datasource.hive_sync.partition_fields': 'BUDAT',
            'hoodie.memory.merge.fraction': '0.80',
            'hoodie.upsert.shuffle.parallelism': "50"
        }

        # Join all the hudi_options for this dataset
        self.hudi_options.update(self.local_hudi_options)

        self.datasetSchemaEvo = True

        self.Schema = StructType([
            StructField("DI_REQTSN", StringType(), True),
            StructField("DI_SEQ_NR", IntegerType(), True),
            StructField("MBLNR", StringType(), True),
            StructField("MJAHR", StringType(), True),
            StructField("BUDAT", LongType(), True),
            StructField("BKTXT", StringType(), True)])
        
        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 6
        self.schemaDict[1]['validFrom'] = '19000101'
        self.schemaDict[1]['validUntil'] = '29991231'
