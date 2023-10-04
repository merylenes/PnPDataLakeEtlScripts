"""
ACZ: s3://pnp-data-lake-dev-acz-ols-euw1/opt/td/log_orders_details/
STZ: s3://pnp-data-lake-dev-stz-ols-euw1/td/log_order_detail/log_order_detail_opt_log_order_detail/
"""

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, DecimalType, LongType, TimestampType
from pnp.etl.datasets import dataset

class optimuslogorderdetailData(dataset):

    def __init__(self, configParams):
        super().__init__(configParams)

        self.hudi_options = {}

        # Join all the hudi_options for this dataset
        self.hudi_options.update(dataset.hudi_options)

        # Specific hudi_options per dataset
        self.local_hudi_options = {
        'hoodie.datasource.write.recordkey.field': 'ID',
        'hoodie.datasource.write.partitionpath.field': 'updated_at_date',
        'hoodie.datasource.hive_sync.partition_fields': 'updated_at_date',
        'hoodie.datasource.hive_sync.support_timestamp':'true',
        'hoodie.datasource.write.operation': 'insert_overwrite',
        'hoodie.combine.before.insert': 'true'
        }

        # Join all the hudi_options for this dataset
        self.hudi_options.update(self.local_hudi_options)

        self.datasetSchemaEvo = True

        self.Schema = StructType([
            StructField("DI_REQTSN",StringType(),False),
            StructField("DI_SEQ_NR",IntegerType(),False),
            StructField("ID",IntegerType(),False),
            StructField("original_id",IntegerType(),True),
            StructField("ORDER_ID",IntegerType(),True),
            StructField("deprecated_product_uid",IntegerType(),True),
            StructField("original_qty",IntegerType(),True),
            StructField("current_qty",IntegerType(),True),
            StructField("deprecated_label",StringType(),True),
            StructField("price", DecimalType(10,2),True),
            StructField("oos", StringType(),True),
            StructField("deprecated_article_number", IntegerType(),True),
            StructField("bag_label", StringType(),True),
            StructField("description", StringType(),True),
            StructField("movement", IntegerType(),True),
            StructField("CREATED_AT",TimestampType(),True),
            StructField("UPDATED_AT",TimestampType(),True),
            StructField("product_code", StringType(),True),
            StructField("updated_at_date",LongType(),True)
        ])
        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 19
        self.schemaDict[1]['validFrom'] = '20230411'
        self.schemaDict[1]['validUntil'] = '29991231'
