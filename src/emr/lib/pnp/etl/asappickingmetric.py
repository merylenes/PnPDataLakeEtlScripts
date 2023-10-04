"""
ACZ: s3://pnp-data-lake-dev-acz-ols-euw1/asap/td/picking_metric/
STZ: s3://pnp-data-lake-dev-stz-ols-euw1/td/sales/sales_picking_metric_01/
"""

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DoubleType, LongType, TimestampType, DecimalType
from pyspark.sql.functions import col, concat, substring, format_string, regexp_replace

from pnp.etl.datasets import dataset

# Please make sure the DATASET_NAME is lowecase - the whole thing. So NO PosCore. Rather poscore.



class asappickingmetricData(dataset):

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
            'hoodie.datasource.write.recordkey.field': 'uid,order_uid',
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
        StructField("store_uid", IntegerType(), True),
        StructField("start_datetime", TimestampType(), True),
        StructField("twenty_perc_time", TimestampType(), True),
        StructField("fifty_perc_time", TimestampType(), True),
        StructField("eighty_perc_time", TimestampType(), True),
        StructField("end_datetime", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
        StructField("updated_at_date", LongType(), True)])


        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 12
        self.schemaDict[1]['validFrom'] = '20230912'
        self.schemaDict[1]['validUntil'] = '20240912'