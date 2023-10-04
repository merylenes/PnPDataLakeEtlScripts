"""
ACZ: s3://pnp-data-lake-dev-acz-ols-euw1/asap/td/daily_orders/
STZ: s3://pnp-data-lake-prod-stz-ols-euw1/td/sales/sales_asap_daily_orders_01/
"""

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, DecimalType, LongType, TimestampType
from pnp.etl.datasets import dataset

class asapdailyordersData(dataset):

    def __init__(self, configParams):
        super().__init__(configParams)

        self.hudi_options = {}

        # Join all the hudi_options for this dataset
        self.hudi_options.update(dataset.hudi_options)

        # Specific hudi_options per dataset
        self.local_hudi_options = {
        'hoodie.datasource.write.recordkey.field': 'PNP_ORDERID,PRODUCT_UID,ORDER_CREATED_AT',
        'hoodie.datasource.write.partitionpath.field': 'ORDER_CREATED_AT_DATE',
        'hoodie.datasource.hive_sync.partition_fields': 'ORDER_CREATED_AT_DATE',
        'hoodie.datasource.hive_sync.support_timestamp':'true'
        }

        # Join all the hudi_options for this dataset
        self.hudi_options.update(self.local_hudi_options)

        self.datasetSchemaEvo = True

        self.Schema = StructType([
            StructField("DI_REQTSN",StringType(),False,{"comment":"Data Services (DI) Request Transaction Number"}),
            StructField("DI_SEQ_NR",IntegerType(),False,{"comment":"Data Services (DI) Sequence Number"}),
            StructField("SALES_CHANNEL",StringType(),True, {"comment":"THE ON-DEMAND SALES CHANNEL IE: MRD, ASAP, ONLINE ETC"}),
            StructField("DATE_KEY",LongType(),True, {"comment":"DATE KEY BASED ON ORDER_CREATED_AT DATE"}),
            StructField("STORE_UID",IntegerType(),True,{"comment":"STORE_UID"}),
            StructField("MRD_ORDERID",StringType(),True, {"comment":"MRD INTERNAL ORDER NUMBER"}),
            StructField("PNP_ORDERID",IntegerType(),False,{"comment":"PNP INTERNAL ORDER NUMBER"}),
            StructField("MRD_CUSTOMERID",StringType(),True, {"comment":"MRD_CUSTOMERID"}),
            StructField("SMART_SHOPPER_NUMBER",StringType(),True, {"comment":"SMART_SHOPPER_NUMBER"}),
            StructField("STATUS_ID",StringType(),True, {"comment":"STATUS_ID"}),
            StructField("ORDER_STATUS",StringType(),True, {"comment":"ORDER_STATUS"}),
            StructField("PRODUCT_UID",IntegerType(),False,{"comment":"PRODUCT_UID"}),
            StructField("ORIGINAL_QTY",StringType(),True, {"comment":"QUANTITY ORDERED BY CUSTOMER BEFORE OUT OF STOCK"}),
            StructField("CURRENT_QTY",IntegerType(),True,{"comment":"QUANTITY FULFILLED BY STORE AFTER OUT OF STOCK"}),
            StructField("OOS_ITEMS_QTY",LongType(),True, {"comment":"OUT OF STOCK QUANTITY"}),
            StructField("PRICE",StringType(),True, {"comment":"PRICE"}),
            StructField("OOS",StringType(),True, {"comment":"OUT OF STOCK INDICATOR Y/N"}),
            StructField("ARTICLE_NUMBER",StringType(),True, {"comment":"ASAP APP ARTICLE NUMBER"}),
            StructField("UOM",StringType(),True, {"comment":"UNIT OF MEASURE"}),
            StructField("ORIGINAL_TOTAL",DecimalType(23,2),True,{"comment":"RANDS ORDERED BY CUSTOMER BEFORE OUT OF STOCK"}),
            StructField("CURRENT_TOTAL",DecimalType(23,2),True,{"comment":"RANDS FULFILLED BY STORE AFTER OUT OF STOCK"}),
            StructField("SAP_ORDER_NUMBER",StringType(),True, {"comment":"SAP ORDER NUMBER"}),
            StructField("STORE_ACCEPTED_ORDER_TIME",LongType(),True, {"comment":"AN OPERATIONS KPI - TIMESTAMP DIFFERENCE IN SECONDS"}),
            StructField("STORE_PICKING_TIME",LongType(),True, {"comment":"AN OPERATIONS KPI - TIMESTAMP DIFFERENCE IN SECONDS"}),
            StructField("STORE_ORDER_PREP_TIME",LongType(),True, {"comment":"AN OPERATIONS KPI - TIMESTAMP DIFFERENCE IN SECONDS"}),
            StructField("DRIVER_IN_STORE_TIME",LongType(),True, {"comment":"AN OPERATIONS KPI - TIMESTAMP DIFFERENCE IN SECONDS"}),
            StructField("DRIVER_TO_CLIENT_TIME",LongType(),True, {"comment":"AN OPERATIONS KPI - TIMESTAMP DIFFERENCE IN SECONDS"}),
            StructField("TOTAL_DELIVERY_TIME",LongType(),True, {"comment":"AN OPERATIONS KPI - TIMESTAMP DIFFERENCE IN SECONDS"}),
            StructField("ROLLEDOVERCHECK",StringType(),True, {"comment":"AN OPERATIONS KPI - TIMESTAMP DIFFERENCE IN SECONDS"}),
            StructField("STORE_INVOICED_ORDER_TIME",LongType(),True, {"comment":"AN OPERATIONS KPI - TIMESTAMP DIFFERENCE IN SECONDS"}),
            StructField("DRIVER_ORDER_ACCEPT_TIME",LongType(),True, {"comment":"AN OPERATIONS KPI - TIMESTAMP DIFFERENCE IN SECONDS"}),
            StructField("DRIVER_TO_STORE_TIME",LongType(),True, {"comment":"AN OPERATIONS KPI - TIMESTAMP DIFFERENCE IN SECONDS"}),
            StructField("DRIVER_AT_CLIENT_TIME",LongType(),True, {"comment":"AN OPERATIONS KPI - TIMESTAMP DIFFERENCE IN SECONDS"}),
            StructField("ORDER_CREATED_AT",TimestampType(),False, {"comment":"YYYY-MM-DD HH:MM:SS"}),
            StructField("COLLECTED_AT",TimestampType(),True, {"comment":"YYYY-MM-DD HH:MM:SS"}),
            StructField("DELIVERED_AT",TimestampType(),True, {"comment":"YYYY-MM-DD HH:MM:SS"}),
            StructField("ACCEPTED_AT",TimestampType(),True, {"comment":"YYYY-MM-DD HH:MM:SS"}),
            StructField("PROMISED_PACKING_COMPLETED_AT",TimestampType(),True, {"comment":"YYYY-MM-DD HH:MM:SS"}),
            StructField("DRIVER_ARRIVED_AT",TimestampType(),True, {"comment":"YYYY-MM-DD HH:MM:SS"}),
            StructField("SALES",DecimalType(19,2),True,{"comment":"SALES"}),
            StructField("BASKETITEMS",StringType(),True, {"comment":"UNITS / QUANTITY INVOICED"}),
            StructField("ORIGINALBASKETITEMS",StringType(),True, {"comment":"QUANTITY ORDERED BY CUSTOMER BEFORE OUT OF STOCK"}),
            StructField("PICKERID",LongType(),True, {"comment":"PICKER ID"}),
            StructField("PICKINGTIME",LongType(),True, {"comment":"PICKING TIME"}),
            StructField("PICKINGMETHOD",StringType(),True, {"comment":"INDICATD IF A PICKING DEVICE WAS USED"}),
            StructField("ADJUSTEDORIGINALBASKETITEMS",StringType(),True, {"comment":"QUANTITY FULFILLED BY STORE AFTER OUT OF STOCK"}),
            StructField("ADJUSTEDBASKETITEMS",StringType(),True, {"comment":"QUANTITY FULFILLED BY STORE AFTER OUT OF STOCK"}),
            StructField("TURNOVER",DecimalType(19,2),True,{"comment":"SALES RAND VALUE"}),
            StructField("PICKING_STARTED_AT",TimestampType(),True, {"comment":"MM-DD-YYYY HH:MM:SS"}),
            StructField("UPDATED_AT",TimestampType(),True, {"comment":"YYYY-MM-DD HH:MM:SS"}), 
            StructField("ORDER_CREATED_AT_DATE",LongType(),False, {"comment":"Created date derived from order_created_at_date field."}) 
        ])
        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 51
        self.schemaDict[1]['validFrom'] = '20230224'
        self.schemaDict[1]['validUntil'] = '29991231'
