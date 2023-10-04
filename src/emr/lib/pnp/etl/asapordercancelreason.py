"""
ACZ: s3://pnp-data-lake-prod-acz-ols-euw1/asap/td/order_cancel_reason /
STZ: s3://pnp-data-lake-prod-stz-ols-euw1/td/sales/sales_order_cancel_reason_01/
"""

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DoubleType, LongType, TimestampType, DecimalType
from pyspark.sql.functions import col, concat, substring, format_string, regexp_replace

from pnp.etl.datasets import dataset

# Please make sure the DATASET_NAME is lowecase - the whole thing. So NO PosCore. Rather poscore.


class asapordercancelreasonData(dataset):

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
            'hoodie.datasource.write.recordkey.field': 'UID,USER_UID,ORDER_UID',
            'hoodie.datasource.write.partitionpath.field': 'UPDATED_AT_DATE',
            'hoodie.datasource.hive_sync.partition_fields': 'UPDATED_AT_DATE',
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
            StructField("UID", IntegerType(), False),
            StructField("USER_UID", IntegerType(), False),
            StructField("ORDER_UID", IntegerType(), False),
            StructField("REASON_OPTION", StringType(), True),
            StructField("MESSAGE", StringType(), True),
            StructField("UPDATED_AT", TimestampType(), True),
            StructField("UPDATED_AT_DATE", LongType(), True)

            ])

        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 9

        self.schemaDict[1]['validFrom'] = '20230918'
        self.schemaDict[1]['validUntil'] = '2099018'

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