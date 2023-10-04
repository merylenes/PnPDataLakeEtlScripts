"""
ACZ: s3://pnp-data-lake-dev-acz-ols-euw1/asap/td/sales_order_delivery_details_01/
STZ: s3://pnp-data-lake-dev-stz-ols-euw1/td/sales/sales_order_delivery_details_01/
"""

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DoubleType, LongType, TimestampType, DecimalType
from pyspark.sql.functions import col, concat, substring, format_string, regexp_replace

from pnp.etl.datasets import dataset

# Please make sure the DATASET_NAME is lowecase - the whole thing. So NO PosCore. Rather poscore.


class asaporderdeliverydetailsData(dataset):

    def __init__(self, configParams):
        super().__init__(configParams)

        self.hudi_options = {}

        # Join all the hudi_options for this dataset
        self.hudi_options.update(dataset.hudi_options)

        # Specific hudi_options per dataset
        # NOTE: Take a look at SalesActualDataMart ETL class. There is an option to put the hudi specific
        # options in the dictionary. There are special cases of when to do that, and in the case of
        # salesactualdatamart, it was becuase we wanted to add a key to the dataset. This could be an option
        # but it does need to add a key to the schema as follows:
        # self.schemaDict[N]['hudi_options'] = { ... }
        # and then you ALSO require a method to handle those as well as a datasetSpecific method which will
        # be called from allDatasets.py.

        self.local_hudi_options = {
            'hoodie.datasource.write.recordkey.field': 'uid,order_uid,delivery_id',
            'hoodie.datasource.write.partitionpath.field': 'updated_at_date',
            'hoodie.datasource.hive_sync.partition_fields': 'updated_at_date',
            'hoodie.datasource.hive_sync.support_timestamp': 'true'
        }

        # Join all the hudi_options for this dataset
        self.hudi_options.update(self.local_hudi_options)

        # For absolutely brand new datasets, the datasetSchemaEvo should be set to True always
        # because it's used in testing for a variety of things in the codebase.

        self.datasetSchemaEvo = True

        # There can be only one ... schema, which is increasing in columns
        self.Schema = StructType([
            StructField("DI_REQTSN", StringType(), False),
            StructField("DI_SEQ_NR", IntegerType(), False),
            StructField("uid", IntegerType(), False),
            StructField("order_uid", IntegerType(), False),
            StructField("delivery_id", StringType(), False),
            StructField("delivery_integration_service", StringType(), True),
            StructField("driver_name", StringType(), True),
            StructField("driver_contact_no", StringType(), True),
            StructField("driver_cancelled", StringType(), True),
            StructField("driver_status", StringType(), True),
            StructField("driver_started_delivery", StringType(), True),
            StructField("driver_tracking_link", StringType(), True),
            StructField("datetime_created", TimestampType(), True),
            StructField("status", StringType(), True),
            StructField("distance_kms", DecimalType(22, 0), True),
            StructField("time_minutes", DecimalType(22, 0), True),
            StructField("vechicle_type", StringType(), True),
            StructField("updated_at", TimestampType(), True),
            StructField("updated_at_date", LongType(), True)
            ])

        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 19

        self.schemaDict[1]['validFrom'] = '20230816'
        self.schemaDict[1]['validUntil'] = '20240816'

        # Setting up this second schema version in case it's going to be needed
        # however for a new dataset (i.e. one never seen before) we do not need version 2
        # but it's here so that when some new version does come along, it can be uncommented.

        # In theory, because we count the columns in the input dataset, we use this to determine which
        # version of a schema we're using. So for example if there's version 3 or 4 in the future
        # provided those new schemas have additional columns added to the existing schema, we can just
        # add another entry to the schemaDict (e.g. schemaDict[3] or schemaDict[4] etc.)

        # self.schemaDict[2] = {'ver': 2}
        # self.schemaDict[2]['cols'] = XXXX_SET_TO_NEW_NUMBER_OF_COLUMNS_XXXX
        # self.schemaDict[2]['validFrom'] = '20220301' # This date should start from the first ingestion date of the new schema
        # self.schemaDict[2]['validUntil'] = '20300228' # This is ostensibly set to sometime in the distant future when this could become invalid
        # self.schemaDict[2]['schema'] = StructType([
        #    XXXX_DATASET_STRUCTURE_XXXX])