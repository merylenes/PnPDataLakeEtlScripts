"""
ACZ: s3://pnp-data-lake-dev-acz-ols-euw1/mrl/td/order_items/
STZ: s3://pnp-data-lake-prod-stz-ols-euw1/td/sales/sales_mrl_order_items_01/
"""

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, LongType, TimestampType
from pnp.etl.datasets import dataset

class mrdorderitemsData(dataset):

    def __init__(self, configParams):
        super().__init__(configParams)

        self.hudi_options = {}

        # Join all the hudi_options for this dataset
        self.hudi_options.update(dataset.hudi_options)

        # Specific hudi_options per dataset
        self.local_hudi_options = {
        'hoodie.datasource.write.recordkey.field': 'UID',
        'hoodie.datasource.write.partitionpath.field': 'CREATED_DATE',
        'hoodie.datasource.hive_sync.partition_fields': 'CREATED_DATE',
        'hoodie.datasource.hive_sync.support_timestamp':'true'
        }

        # Join all the hudi_options for this dataset
        self.hudi_options.update(self.local_hudi_options)

        self.datasetSchemaEvo = True

        self.Schema = StructType([
            StructField("DI_REQTSN",StringType(),False,{"comment":"Data Services (DI) Request Transaction Number"}),
            StructField("DI_SEQ_NR",IntegerType(),False,{"comment":"Data Services (DI) Sequence Number"}),
            StructField("UID",IntegerType(),False,{"comment":"Unique ID"}),
            StructField("ORDER_UID",IntegerType(),True,{"comment":"Unique Order ID"}),
            StructField("PRODUCT_UID",IntegerType(),True,{"comment":"Product ID"}),
            StructField("EXTERNAL_RSP",StringType(),True, {"comment":"External Responsible"}),
            StructField("QUANTITY",IntegerType(),True,{"comment":"Quantity"}),
            StructField("SEQ_NO",IntegerType(),True,{"comment":"Sequence Number"}),
            StructField("VARIABLE_VOLUME_SEQ_NO",IntegerType(),True,{"comment":"Sequence Number"}),
            StructField("CREATED_AT",TimestampType(),True, {"comment":"Created at"}),
            StructField("UPDATED_AT",TimestampType(),True, {"comment":"Updated at"}),
            StructField("CREATED_DATE",LongType(),True, {"comment":"Updated at"})
        ])
        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 12
        self.schemaDict[1]['validFrom'] = '19000101'
        self.schemaDict[1]['validUntil'] = '29991231'
