"""
ACZ: s3://pnp-data-lake-dev-acz-ols-euw1/opt/td/picking_sessions/
STZ: s3://pnp-data-lake-dev-stz-ols-euw1/td/picking_sessions/picking_sessions_opt_picking_sessions/
"""

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, DecimalType, LongType, TimestampType
from pnp.etl.datasets import dataset

class optimuspickingsessionsData(dataset):

    def __init__(self, configParams):
        super().__init__(configParams)

        self.hudi_options = {}

        # Join all the hudi_options for this dataset
        self.hudi_options.update(dataset.hudi_options)

        # Specific hudi_options per dataset
        self.local_hudi_options = {
        'hoodie.datasource.write.recordkey.field': 'id,order_id,picker_user_uid',
        'hoodie.datasource.write.partitionpath.field': 'start_time_date',
        'hoodie.datasource.hive_sync.partition_fields': 'start_time_date',
        'hoodie.datasource.hive_sync.support_timestamp':'true',
        'hoodie.datasource.write.operation': 'insert_overwrite',
        'hoodie.combine.before.insert': 'true'
        }

        # Join all the hudi_options for this dataset
        self.hudi_options.update(self.local_hudi_options)

        self.datasetSchemaEvo = True

        self.Schema = StructType([
            StructField("DI_REQTSN", StringType(),False),
            StructField("DI_SEQ_NR", IntegerType(),False),
            StructField("ID", DecimalType(18,0),False),
            StructField("order_id", IntegerType(),False),
            StructField("picker_user_uid", IntegerType(),False),
            StructField("start_time", TimestampType(),True),
            StructField("end_time", TimestampType(),True),
            StructField("picking_type", StringType(),True),
            StructField("channel_id", IntegerType(),True),
            StructField("start_time_date", LongType(),True)
        ])
        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 10
        self.schemaDict[1]['validFrom'] = '20230411'
        self.schemaDict[1]['validUntil'] = '29991231'
