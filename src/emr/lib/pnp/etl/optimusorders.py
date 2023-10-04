"""
ACZ: s3://pnp-data-lake-prod-acz-ols-euw1/opt/td/orders/
STZ: s3://pnp-data-lake-prod-stz-ols-euw1/td/orders/orders_opt_orders
"""

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, DecimalType, LongType, TimestampType
from pnp.etl.datasets import dataset

class optimusordersData(dataset):

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
            StructField("CHANNEL_ID",StringType(),True, {"comment":"Always defaulted to a 1"}),
            StructField("STATUS",IntegerType(),True, {"comment":"Order delivery status ID"}),
            StructField("ORIGINAL_TOTAL",DecimalType(10,2),True,{"comment":"Total rand value of ordered/placed by customer"}),
            StructField("CURRENT_TOTAL",DecimalType(10,2),True, {"comment":"Total rands based on Available units fulfilled by the store"}),
            StructField("STORE_CODE",StringType(),False,{"comment":"PnP Store code"}),
            StructField("PICKER_NAME",StringType(),True, {"comment":"Staff who picked the stock"}),
            StructField("PICKER_INSTRUCTIONS",StringType(),True, {"comment":"Special notes"}),
            StructField("PICKING_DATETIME",TimestampType(),True, {"comment":"timestamp of picking started"}),
            StructField("CHANNEL_ORDER_NUMBER",StringType(),True, {"comment":"MrD Order number"}),
            StructField("CUSTOMER_ID",IntegerType(),False,{"comment":"customer_id link to customer table"}),
            StructField("CUSTOMER_NAME",StringType(),True, {"comment":"the customer_name"}),
            StructField("CUSTOMER_ADDRESS",StringType(),True,{"comment":"the customer_address"}),
            StructField("CUSTOMER_EMAIL_ADDRESS",StringType(),True, {"comment":"the customer_email_address"}),
            StructField("CUSTOMER_CONTACT",StringType(),True, {"comment":"the customer_contact"}),
            StructField("SMART_SHOPPER_NUMBER",StringType(),True, {"comment":"the smart_shopper_number used in the order"}),
            StructField("SAP_ORDER_NUMBER",IntegerType(),True, {"comment":"the sap_order_number"}),
            StructField("STATUS_REASON",StringType(),True, {"comment":"the delivery status_reason"}),
            StructField("CREATED_AT",TimestampType(),True,{"comment":"Order created time"}),
            StructField("UPDATED_AT",TimestampType(),True,{"comment":"Partition key"}),
            StructField("CREATED_AT_DATE",LongType(),True, {"comment":"Order created date for partitioning"})
        ])
        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 23
        self.schemaDict[1]['validFrom'] = '20230411'
        self.schemaDict[1]['validUntil'] = '29991231'
