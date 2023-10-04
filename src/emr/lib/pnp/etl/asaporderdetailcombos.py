"""
ACZ: s3://pnp-data-lake-prod-acz-ols-euw1/asap/td/order_detail_combos
STZ: s3://pnp-data-lake-prod-stz-ols-euw1/td/sales/sales_order_detail_combo_01
"""

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DoubleType, LongType, TimestampType, DecimalType
from pyspark.sql.functions import col, concat, substring, format_string, regexp_replace
from pnp.etl.datasets import dataset

class asaporderdetailcombosData(dataset):

    def __init__(self, configParams):
        super().__init__(configParams)

        self.hudi_options = {}

        # Join all the hudi_options for this dataset
        self.hudi_options.update(dataset.hudi_options)
        
        # Specific hudi_options per dataset
        self.local_hudi_options = {
            'hoodie.datasource.write.recordkey.field': 'uid,order_detail_uid',
            'hoodie.datasource.write.partitionpath.field': 'updated_at_date',
            'hoodie.datasource.hive_sync.partition_fields': 'updated_at_date',
            'hoodie.datasource.hive_sync.support_timestamp': 'true'
          }

        # Join all the hudi_options for this dataset
        self.hudi_options.update(self.local_hudi_options)

        self.datasetSchemaEvo = True

        self.Schema = StructType([
            StructField("DI_REQTSN", StringType(),False),
            StructField("DI_SEQ_NR", IntegerType(),False),
            StructField("uid", IntegerType(),False),
            StructField("order_detail_uid", IntegerType(),False),
            StructField("item_product_uid", IntegerType(),True),
            StructField("item_quantity", IntegerType(),True),
            StructField("item_unit_price", DecimalType(10,2), True),
            StructField("updated_at", TimestampType(),True),
            StructField("updated_at_date", LongType(),True)
           ])

        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 9
        self.schemaDict[1]['validFrom'] = '20230822'
        self.schemaDict[1]['validUntil'] = '20240822'