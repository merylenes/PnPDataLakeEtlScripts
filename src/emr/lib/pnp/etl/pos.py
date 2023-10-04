"""
ACZ: s3://pnp-data-lake-prod-acz-pos-euw1/bw/td/hprcm08
STZ: s3://pnp-data-lake-prod-stz-pos-euw1/td/pos/pos_bw_hprcm08
"""

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, LongType, DecimalType
from pnp.etl.datasets import dataset

class posData(dataset):

    def __init__(self, configParams):
        super().__init__(configParams)

        self.hudi_options = {}

        # Join all the hudi_options for this dataset
        self.hudi_options.update(dataset.hudi_options)

        # Specific hudi_options per dataset
        self.local_hudi_options = {
          "hoodie.datasource.write.recordkey.field": "HPATXTIME,HPLANT,HPAWID,HPATNR,HMATERIAL,SALES_UNIT,HEANUPC,HSSINLFL,HITMRET",
          "hoodie.datasource.write.partitionpath.field": "CALDAY",
          "hoodie.datasource.hive_sync.partition_fields": "CALDAY",
          'hoodie.cleaner.policy': 'KEEP_LATEST_FILE_VERSIONS',
          'hoodie.cleaner.fileversions.retained': 1,
        }

        # Join all the hudi_options for this dataset
        self.hudi_options.update(self.local_hudi_options)

        self.datasetSchemaEvo = True

        self.Schema = StructType([
            StructField("DI_REQTSN", StringType(), False),
            StructField("DI_SEQ_NR", LongType(), False),
            StructField("CALDAY", LongType(), False),
            StructField("HPATXTIME", StringType(), False),
            StructField("HPLANT", StringType(), False),
            StructField("HPAWID", StringType(), False),
            StructField("HPATNR", StringType(), False),
            StructField("HMATERIAL", StringType(), False),
            StructField("SALES_UNIT", StringType(), False),
            StructField("HEANUPC", StringType(), False),
            StructField("HSSINLFL", StringType(), False),
            StructField("HITMRET", StringType(), False),
            StructField("HPAPSTP", StringType(), True),
            StructField("HCRMSHCRD", StringType(), True),
            StructField("HCRMEMBER", StringType(), True),
            StructField("HCRMSHID", StringType(), True),
            StructField("HPASALHR", LongType(), True),
            StructField("HSSPFL", StringType(), True),
            StructField("HPANCSWYN", StringType(), True),
            StructField("HTXRET", StringType(), True),
            StructField("LOC_CURRCY", StringType(), True),
            StructField("BASE_UOM", StringType(), True),
            StructField("HPASQYBU", DecimalType(17,3), True),
            StructField("HPASQYSU", DecimalType(17,3), True),
            StructField("HPALITCNT", IntegerType(), True),
            StructField("HPASATLC", DecimalType(17,2), True),
            StructField("HPACIDELC", DecimalType(17,2), True),
            StructField("HPATIDILC", DecimalType(17,2), True),
            StructField("HPAITTALC", DecimalType(17,2), True)])

        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 29
        self.schemaDict[1]['validFrom'] = '19000101'
        self.schemaDict[1]['validUntil'] = '29991231'
