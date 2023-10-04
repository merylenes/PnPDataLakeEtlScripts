"""
ACZ: s3://pnp-data-lake-prod-acz-ols-euw1/opt/td/log_order_status/
STZ: s3://pnp-data-lake-prod-stz-ols-euw1/td/log_order_status/orders_opt_log_order_status/
"""

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, DecimalType, LongType, TimestampType
from pnp.etl.datasets import dataset

class optimuslogorderstatusData(dataset):

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
        'hoodie.datasource.hive_sync.support_timestamp':'true',
        'hoodie.datasource.write.operation': 'insert_overwrite',
        'hoodie.combine.before.insert': 'true'
        }

        # Join all the hudi_options for this dataset
        self.hudi_options.update(self.local_hudi_options)

        self.datasetSchemaEvo = True

        self.Schema = StructType([
            StructField("DI_REQTSN",StringType(),False,{"comment":"Data Services (DI) Request Transaction Number"}),
            StructField("DI_SEQ_NR",IntegerType(),False,{"comment":"Data Services (DI) Sequence Number"}),
            StructField("ID",IntegerType(),False, {"comment":"Unique ID, Primary Key"}),
            StructField("ORDER_ID",IntegerType(),True, {"comment":"Order number"}),
            StructField("OLD_STATUS",IntegerType(),True, {"comment":"previous status"}),
            StructField("NEW_STATUS",IntegerType(),True, {"comment":"latest staus"}),
            StructField("DESCRIPTION",StringType(),False,{"comment":"status text / status description"}),
            StructField("CREATED_AT",TimestampType(),True,{"comment":"Order created time"}),
            StructField("UPDATED_AT",TimestampType(),True,{"comment":"last modified timestamp - table/row modification"}),
            StructField("CREATED_AT_DATE",LongType(),True, {"comment":"Order created date for partitioning"})
        ])
        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 10
        self.schemaDict[1]['validFrom'] = '20230411'
        self.schemaDict[1]['validUntil'] = '29991231'
