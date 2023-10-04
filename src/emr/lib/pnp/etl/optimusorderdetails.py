"""
ACZ: s3://pnp-data-lake-prod-acz-ols-euw1/opt/td/order_details/
STZ: s3://pnp-data-lake-prod-stz-ols-euw1/td/order_details/orders_opt_order_details/
"""

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, DecimalType, LongType, TimestampType
from pnp.etl.datasets import dataset

class optimusorderdetailsData(dataset):

    def __init__(self, configParams):
        super().__init__(configParams)

        self.hudi_options = {}

        # Join all the hudi_options for this dataset
        self.hudi_options.update(dataset.hudi_options)

        # Specific hudi_options per dataset
        self.local_hudi_options = {
        'hoodie.datasource.write.recordkey.field': 'ID',
        'hoodie.datasource.write.partitionpath.field': 'CREATED_AT_DATE',
        'hoodie.datasource.hive_sync.partition_fields': 'CREATED_AT_DATE',
        'hoodie.datasource.hive_sync.support_timestamp':'true'
        }

        # Join all the hudi_options for this dataset
        self.hudi_options.update(self.local_hudi_options)

        self.datasetSchemaEvo = True

        self.Schema = StructType([
            StructField("DI_REQTSN",StringType(),False,{"comment":"Data Services (DI) Request Transaction Number"}),
            StructField("DI_SEQ_NR",IntegerType(),False,{"comment":"Data Services (DI) Sequence Number"}),
            StructField("ID",IntegerType(),False, {"comment":"Unique ID, Primary Key"}),
            StructField("PRODUCT_CODE",StringType(),True, {"comment":"PnP Product UID + UOM"}),
            StructField("ORDER_ID",IntegerType(),True, {"comment":"Order number"}),
            StructField("DEPRECATED_PRODUCT_UID",IntegerType(),True, {"comment":"PnP Product UID, no longer in use - historic reporting only before Feb 2023"}),
            StructField("ORIGINAL_QTY",IntegerType(),True,{"comment":"Number of units ordered/placed by customer"}),
            StructField("CURRENT_QTY",IntegerType(),True, {"comment":"Available units fulfilled by the store"}),
            StructField("DEPRECATED_LABEL",StringType(),False,{"comment":"PnP Product Description, no longer in use - historic reporting only before Feb 2023"}),
            StructField("PRICE",DecimalType(10,2),True, {"comment":"Item Selling Price"}),
            StructField("OOS",StringType(),True, {"comment":"Out of stock indicator Y/N"}),
            StructField("DEPRECATED_ARTICLE_NUMBER",IntegerType(),True, {"comment":"PnP Product UID, no longer in use - historic reporting only before Feb 2023"}),
            StructField("DEPRECATED_UOM",StringType(),True, {"comment":"Unit of measure, no longer in use - historic reporting only before Feb 2023"}),
            StructField("BAG_LABEL",StringType(),False,{"comment":"Paper Bag for On Demand Delivery"}),
            StructField("CREATED_AT",TimestampType(),True,{"comment":"Order created time"}),
            StructField("UPDATED_AT",TimestampType(),True,{"comment":"Partition key"}),
            StructField("CREATED_AT_DATE",LongType(),True, {"comment":"Order created date for partitioning"})
        ])
        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 17
        self.schemaDict[1]['validFrom'] = '20230411'
        self.schemaDict[1]['validUntil'] = '29991231'
