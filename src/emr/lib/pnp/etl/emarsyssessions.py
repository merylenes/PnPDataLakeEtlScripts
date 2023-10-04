"""
ACZ: s3://pnp-data-lake-dev-acz-crm-euw1/ems/td/sessions/
STZ: s3://pnp-data-lake-dev-stz-crm-euw1/td/sessions/sessions_ems_sessions/
"""

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DoubleType, LongType, TimestampType, DecimalType
from pyspark.sql.functions import col, concat, substring, format_string, regexp_replace

from pnp.etl.datasets import dataset

# Please make sure the DATASET_NAME is lowecase - the whole thing. So NO PosCore. Rather poscore.


class emarsyssessionsData(dataset):

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
            'hoodie.datasource.write.recordkey.field': 'start_time,end_time,user_id,customer_id',
            'hoodie.datasource.write.partitionpath.field': 'loaded_at_date',
            'hoodie.datasource.hive_sync.partition_fields': 'loaded_at_date',
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
            StructField("start_time", TimestampType(), False),
            StructField("end_time", TimestampType(), False),
            StructField("purchases__order_id", StringType(), True),
            StructField("purchases__event_time", TimestampType(), True),
            StructField("purchases__items__item_id", StringType(), True),
            StructField("purchases__items__price", DecimalType(38, 24), True),
            StructField("purchases__items__quantity",
                        DecimalType(38, 24), True),
            StructField("views__event_time", TimestampType(), True),
            StructField("views__item_id", StringType(), True),
            StructField("tags__event_time", TimestampType(), True),
            StructField("tags__tag", StringType(), True),
            StructField("tags__attributes__name", StringType(), True),
            StructField("tags__attributes__string_value", StringType(), True),
            StructField("tags__attributes__number_value",
                        DecimalType(38, 24), True),
            StructField("tags__attributes__boolean_value", StringType(), True),
            StructField("categories__event_time", TimestampType(), True),
            StructField("categories__category", StringType(), True),
            StructField("last_cart__event_time", TimestampType(), True),
            StructField("last_cart__items__item_id", StringType(), True),
            StructField("last_cart__items__price", DecimalType(38, 24), True),
            StructField("last_cart__items__quantity",
                        DecimalType(38, 24), True),
            StructField("user_id", StringType(), False),
            StructField("user_id_type", StringType(), True),
            StructField("user_id_field_id", IntegerType(), True),
            StructField("contact_id", IntegerType(), True),
            StructField("currency", StringType(), True),
            StructField("customer_id", IntegerType(), False),
            StructField("partitiontime", TimestampType(), True),
            StructField("loaded_at", TimestampType(), True),
            StructField("loaded_at_date", LongType(), True)])

        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 32
        self.schemaDict[1]['validFrom'] = '20230829'
        self.schemaDict[1]['validUntil'] = '20240829'

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
