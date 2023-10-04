
"""
ACZ: s3://pnp-data-lake-prod-acz-ols-euw1/asap/td/payment_details/
STZ: s3://pnp-data-lake-prod-stz-ols-euw1/td/sales/sales_payment_details_01/
"""

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DecimalType, TimestampType
from pyspark.sql.functions import col, concat, substring, format_string, regexp_replace

from pnp.etl.datasets import dataset

# Please make sure the DATASET_NAME is lowecase - the whole thing. So NO PosCore. Rather poscore.
class asappaymentdetailData(dataset):

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
          'hoodie.datasource.write.recordkey.field': 'UID,ORDER_UID,TRANSACTION_REFERENCE,TRANSACTION_TYPE',
          'hoodie.datasource.write.partitionpath.field': 'DATETIME_CREATED_DATE',
          'hoodie.datasource.hive_sync.partition_fields': 'DATETIME_CREATED_DATE',
          'hoodie.datasource.hive_sync.support_timestamp':'true'
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
            StructField("UID", LongType(), True),
            StructField("ORDER_UID", IntegerType(), False),
            StructField("ORDER_STATUS", StringType(), True),
            StructField("CREDIT_UID", IntegerType(), True),
            StructField("TRANSACTION_REFERENCE", StringType(), True),
            StructField("ORIGINAL_REFERENCE", StringType(), True),
            StructField("TRANSACTION_TYPE", StringType(), True),
            StructField("SYSTEM_TRANSACTION_REFERENCE", StringType(), True),
            StructField("ORIGINAL_AMOUNT", DecimalType(10,2), True),
            StructField("BALANCE", DecimalType(10,2), True),
            StructField("DATETIME_CREATED", TimestampType(), True),
            StructField("DATETIME_UPDATED", TimestampType(), True),
            StructField("STATUS", StringType(), True),
            StructField("PAYMENT_TYPE", StringType(), True),
            StructField("UPDATED_AT", TimestampType(), True),
            StructField("DATETIME_CREATED_DATE", LongType(), True)])

        # NOTE:
        # The validFrom and validUntil items are currently not used in any way, but I thought
        # it could potentially be used in the future in case the version/column number could then be
        # switched out with a date instead of the number of columns.
        # It is also just a sanity check because in time, there will be corporate memory loss of when
        # a schema changed - and this should be captured somewhere.

        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 18
        self.schemaDict[1]['validFrom'] = '20230821' # This is set to an arbitratry date in the past
        self.schemaDict[1]['validUntil'] = '20990821' # This is ostensibly set to whatever date this version of schema applies until

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