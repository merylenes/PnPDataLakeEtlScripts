"""
ACZ: s3://box-data-lake-prod-acz-retail-euw1/box/td/sales/
STZ: s3://box-data-lake-prod-stz-retail-euw1/td/sales/sales_box_sales_01/
"""

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, LongType, DecimalType
from pnp.etl.datasets import dataset

class boxersalesactualData(dataset):

    def __init__(self, configParams):
        super().__init__(configParams)

        self.hudi_options = {}

        # Join all the hudi_options for this dataset
        self.hudi_options.update(dataset.hudi_options)

        # Specific hudi_options per dataset
        self.local_hudi_options = {
            'hoodie.datasource.write.recordkey.field': 'BOXER_ITEM_NO,BRANCH,SALES_DATE',
            'hoodie.datasource.write.partitionpath.field': 'SALES_DATE',
            'hoodie.datasource.hive_sync.partition_fields': 'SALES_DATE'
        }

        # Join all the hudi_options for this dataset
        self.hudi_options.update(self.local_hudi_options)

        self.datasetSchemaEvo = True

        self.Schema = StructType([
            StructField("DI_REQTSN", StringType(), True),
            StructField("DI_SEQ_NR", IntegerType(), True),
            StructField("BOXER_ITEM_NO", StringType(), True),
            StructField("BRANCH", StringType(), True),
            StructField("SALES_DATE", LongType(), True),
            StructField("ON_PROMOTION", StringType(), True),
            StructField("REQTNS", StringType(), True),
            StructField("SALES_QUANTITY", DecimalType(17,2), True),
            StructField("SALES_SKU_QUANTITY", DecimalType(17,2), True),
            StructField("SALES_SELL_EXCL", DecimalType(17,2), True),
            StructField("VAT_VALUE", DecimalType(17,2), True),
            StructField("VAT_PERCENTAGE", DecimalType(17,2), True)])

        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 12
        self.schemaDict[1]['validFrom'] = '19000101'
        self.schemaDict[1]['validUntil'] = '29991231'
