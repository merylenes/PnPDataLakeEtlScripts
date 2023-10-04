"""
ACZ: s3://pnp-data-lake-dev-acz-ols-euw1/asap/td/order/
STZ: s3://pnp-data-lake-prod-stz-ols-euw1/td/sales/sales_order_01/
"""

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, DecimalType, LongType, TimestampType
from pnp.etl.datasets import dataset

class asaporderData(dataset):

    def __init__(self, configParams):
        super().__init__(configParams)

        self.hudi_options = {}

        # Join all the hudi_options for this dataset
        self.hudi_options.update(dataset.hudi_options)

        # Specific hudi_options per dataset
        self.local_hudi_options = {
        'hoodie.datasource.write.recordkey.field': 'UID,STORE_UID,PROMO_CODE,CREATED_DATETIME,TRANSACTION_SEQUENCE,USER_UID,STATUS,EDI_ORDER_NO,EDI_INVOICE_STATUS,ORDER_TYPE',
        'hoodie.datasource.write.partitionpath.field': 'UPDATED_AT_DATE',
        'hoodie.datasource.hive_sync.partition_fields': 'UPDATED_AT_DATE',
        'hoodie.datasource.hive_sync.support_timestamp':'true'
        }

        # Join all the hudi_options for this dataset
        self.hudi_options.update(self.local_hudi_options)

        self.datasetSchemaEvo = True

        self.Schema = StructType([
            StructField("DI_REQTSN",StringType(),False),
            StructField("DI_SEQ_NR",IntegerType(),False),
            StructField("UID",IntegerType(),False),
            StructField("DELIVERY_ADDRESS",StringType(),True),
            StructField("COMPLEX_UNIT_NO",StringType(),True),
            StructField("DELIVERY_LATITUDE",StringType(),True),
            StructField("DELIVERY_LONGITUDE",StringType(),True),
            StructField("STORE_UID",IntegerType(),False),
            StructField("APP_VERSION",StringType(),True),
            StructField("DELIVERY_FEE",StringType(),True),
            StructField("AMENDED_DELIVERY_FEE",StringType(),True),
            StructField("BOTTLES_FEE",StringType(),True),
            StructField("AMENDED_BOTTLES_FEE",StringType(),True),
            StructField("PROMO_CODE",StringType(),False),
            StructField("PROMO_DISCOUNT",StringType(),True),
            StructField("PROMO_DISCOUNT_AMOUNT",StringType(),True),
            StructField("PROMO_DISCOUNT_TYPE",StringType(),True),
            StructField("USER_AGENT_STRING",StringType(),True),
            StructField("AMENDED_PROMO_CODE",StringType(),True),
            StructField("AMENDED_PROMO_DISCOUNT",StringType(),True),
            StructField("AMENDED_PROMO_DISCOUNT_AMOUNT",StringType(),True),
            StructField("AMENDED_PROMO_DISCOUNT_TYPE",StringType(),True),
            StructField("DELIVERY_NOTES",StringType(),True),
            StructField("ITEMS_TOTAL",StringType(),True),
            StructField("AMENDED_ITEMS_TOTAL",StringType(),True),
            StructField("ORDER_TOTAL",StringType(),True),
            StructField("THIS_ORDER_TOTAL",StringType(),True),
            StructField("AMENDED_ORDER_TOTAL",StringType(),True),
            StructField("ADJUSTED_ITEMS_TOTAL",StringType(),True),
            StructField("ADJUSTED_DATETIME",TimestampType(),True),
            StructField("ADJUSTED_ORDER_TOTAL",StringType(),True),
            StructField("OUTSTANDING_AMOUNT",StringType(),True),
            StructField("CRC",StringType(),True),
            StructField("CREATED_DATETIME",TimestampType(),False),
            StructField("PLACED_DATETIME",TimestampType(),True),
            StructField("TRANSACTION_SEQUENCE",StringType(),False),
            StructField("USER_UID",IntegerType(),False),
            StructField("STATUS",StringType(),False),
            StructField("ACCEPTED_DATETIME",TimestampType(),True),
            StructField("PICKING_DATETIME",TimestampType(),True),
            StructField("EDI_ORDER_NO",StringType(),False),
            StructField("EDI_INVOICE_STATUS",StringType(),False),
            StructField("EDI_INVOICE_NO",StringType(),True),
            StructField("EDI_MESSAGE_RESPONSE",StringType(),True),
            StructField("LAST_STATUS_CHANGED_DATETIME",TimestampType(),True),
            StructField("EDI_CREDIT_STATUS",StringType(),True),
            StructField("AMENDED_DATETIME",TimestampType(),True),
            StructField("VENDOR_AMENDED_DATETIME",TimestampType(),True),
            StructField("VENDOR_VIEWED_DATETIME",TimestampType(),True),
            StructField("USER_AMENDING_DATETIME",TimestampType(),True),
            StructField("USER_SUBSTITUTED",StringType(),True),
            StructField("RATING",StringType(),True),
            StructField("RATING_COMMENT",StringType(),True),
            StructField("RATING_OPTIONS",StringType(),True),
            StructField("CONFIRMED_DATETIME",TimestampType(),True),
            StructField("DELIVERED_DATETIME",TimestampType(),True),
            StructField("DELIVERY_INTEGRATION_ENABLED",StringType(),True),
            StructField("PAID_IN_FULL",StringType(),True),
            StructField("DRIVER_NAME",StringType(),True),
            StructField("DRIVER_CONTACT_NO",StringType(),True),
            StructField("INVOICE_NO",StringType(),True),
            StructField("PPAY_REQUESTED",StringType(),True),
            StructField("PPAY_PAYMENT_ID",StringType(),True),
            StructField("DRIVER_CANCELLED",StringType(),True),
            StructField("BOTTLES_NOTES",StringType(),True),
            StructField("BOTTLES_AGENT_NAME",StringType(),True),
            StructField("DRIVER_STARTED_DELIVERY",StringType(),True),
            StructField("DRIVER_TRACKING_LINK",StringType(),True),
            StructField("VENDOR_NEED_HELP",StringType(),True),
            StructField("HAS_VENDOR_VIEWED_ORDER",StringType(),True),
            StructField("PICKING_SLIP_PN_SENT",StringType(),True),
            StructField("VENDOR_READY_FOR_DRIVER",StringType(),True),
            StructField("DRIVER_READY_DATETIME",TimestampType(),True),
            StructField("DRIVER_STATUS",StringType(),True),
            StructField("VENDOR_DRIVER_COLLECTED",StringType(),True),
            StructField("DELIVERY_INTEGRATION_SERVICE_NAME",StringType(),True),
            StructField("SHOW_DRIVER_TRACKING",StringType(),True),
            StructField("PAYMENT_METHOD",StringType(),True),
            StructField("POSSIBLE_FRAUD",StringType(),True),
            StructField("INVOICE_STATUS",StringType(),True),
            StructField("DRIVER_TIP",DecimalType(10,2),True),
            StructField("ORDER_TYPE",StringType(),False),
            StructField("PICKER_NAME",StringType(),True),
            StructField("ROLLBACK_STATUS",StringType(),True),
            StructField("ROLLBACK_DATETIME",TimestampType(),True),
            StructField("SMART_SHOPPER_NUMBER",StringType(),True),
            StructField("UPDATED_AT",TimestampType(),True),
            StructField("UPDATED_AT_DATE",LongType(),True)
        ])
        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 88
        self.schemaDict[1]['validFrom'] = '20230831'
        self.schemaDict[1]['validUntil'] = '20240831'
