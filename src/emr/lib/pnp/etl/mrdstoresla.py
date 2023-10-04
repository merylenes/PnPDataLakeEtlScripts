
"""
ACZ: s3://pnp-data-lake-prod-acz-ols-euw1/mrd/td/store_sla/
STZ: "s3://pnp-data-lake-prod-stz-ols-euw1/td/sales/sales_mrd_store_sla_01/
"""

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DecimalType
from pyspark.sql.functions import col, concat, substring, format_string, regexp_replace

from pnp.etl.datasets import dataset

# Please make sure the DATASET_NAME is lowecase - the whole thing. So NO PosCore. Rather poscore.
class mrdstoreslaData(dataset):

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
          'hoodie.datasource.write.recordkey.field': 'STORE_ID',
          'hoodie.datasource.write.partitionpath.field': 'ORDERS_CREATED_WEEK',
          'hoodie.datasource.hive_sync.partition_fields': 'ORDERS_CREATED_WEEK'
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
            StructField("ORDERS_CREATED_WEEK", LongType(), True),
            StructField("STORE_ID", StringType(), False),
            StructField("STORE_NAME", StringType(), True),
            StructField("ORDERS_SENT_TO_STORE", DecimalType(19,0), True),
            StructField("ORDERS_SUCCESSFULLY_PICKED", DecimalType(19,0), True),
            StructField("ORDERS_SUCCESSFULLY_DELIVERED", DecimalType(19,0), True),
            StructField("AVG_DRIVER_RATING", DecimalType(38,9), True),
            StructField("PERC_OF_ORDERS_WHERE_DRIVER_ARRIVED_WITHIN_15MINS_OF_PICKING_COMPLETE", DecimalType(19,4), True),
            StructField("PERC_OF_ORDERS_WHERE_DRIVER_ARRIVED_WITHIN_25MINS_OF_PICKING_COMPLETE", DecimalType(19,4), True),
            StructField("FAILED_DELIVERY_PERC", DecimalType(19,4), True),
            StructField("PERC_OF_ORDERS_DELIVERED_WITHIN_20MINUTES_FROM_COLLECTION", DecimalType(19,4), True),
            StructField("PERC_OF_ORDERS_DELIVERED_WITHIN_25MINUTES_FROM_COLLECTION", DecimalType(19,4), True),
            StructField("ON_TIME_IN_FULL_DELIVERY", DecimalType(19,4), True),
            StructField("STORE_ORDER_COMPLETED_PERC", DecimalType(19,4), True),
            StructField("PERC_OF_ORDERS_READY_FOR_COLLECTION_BEFORE_PROMISED_TIME", DecimalType(19,4), True),
            StructField("PERC_OF_ORDERS_READY_FOR_COLLECTION_WITHIN_15MINUTES_FROM_PROMISED_TIME", DecimalType(19,4), True),
            StructField("PERC_OF_ORDERS_READY_FOR_COLLECTION_WITHIN_45MINUTES_OF_SENT_TO_STORE", DecimalType(19,4), True)])

        # NOTE:
        # The validFrom and validUntil items are currently not used in any way, but I thought
        # it could potentially be used in the future in case the version/column number could then be
        # switched out with a date instead of the number of columns.
        # It is also just a sanity check because in time, there will be corporate memory loss of when
        # a schema changed - and this should be captured somewhere.

        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 19
        self.schemaDict[1]['validFrom'] = '20221011' # This is set to an arbitratry date in the past
        self.schemaDict[1]['validUntil'] = '20231011' # This is ostensibly set to whatever date this version of schema applies until

        # Setting up this second schema version in case it's going to be needed
        # however for a new dataset (i.e. one never seen before) we do not need version 2
        # but it's here so that when some new version does come along, it can be uncommented.

        # In theory, because we count the columns in the input dataset, we use this to determine which
        # version of a schema we're using. So for example if there's version 3 or 4 in the future
        # provided those new schemas have additional columns added to the existing schema, we can just 
        # add another entry to the schemaDict (e.g. schemaDict[3] or schemaDict[4] etc.) 
        
        #self.schemaDict[2] = {'ver': 2}
        #self.schemaDict[2]['cols'] = XXXX_SET_TO_NEW_NUMBER_OF_COLUMNS_XXXX
        #self.schemaDict[2]['validFrom'] = '20220301' # This date should start from the first ingestion date of the new schema
        #self.schemaDict[2]['validUntil'] = '20300228' # This is ostensibly set to sometime in the distant future when this could become invalid
        #self.schemaDict[2]['schema'] = StructType([
        #    XXXX_DATASET_STRUCTURE_XXXX])           