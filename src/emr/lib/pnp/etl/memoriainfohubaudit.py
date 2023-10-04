"""
ACZ: s3://pnp-data-lake-dev-acz-sud-euw1/memoria/td/event_memoria_infohub_audit/
STZ: s3://pnp-data-lake-dev-stz-sud-euw1/td/audit/event_memoria_infohub_audit
"""

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

from pnp.etl.datasets import dataset

# Please make sure the DATASET_NAME is lowecase - the whole thing. So NO PosCore. Rather poscore.
class memoriainfohubauditData(dataset):

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
            'hoodie.datasource.write.recordkey.field': 'memoria_infohub_audit_id,memoria_id',
            'hoodie.datasource.write.partitionpath.field': 'calday',
            'hoodie.datasource.hive_sync.partition_fields': 'calday'
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
            StructField("memoria_infohub_audit_id", StringType(), True),
            StructField("memoria_id", StringType(), True),
            StructField("infohub_id", StringType(), True),
            StructField("calday", LongType(), True),
            StructField("created_by", StringType(), True),
            StructField("created_date", StringType(), True),
            StructField("permission_scopes", StringType(), True),
            StructField("modified_by", StringType(), True),
            StructField("modified_date", StringType(), True),
            StructField("extracts_category_permissions", StringType(), True),
            StructField("insights_category_permissions", StringType(), True),
            StructField("real_estate_site_permissions", StringType(), True),
            StructField("direct_query_permissions", StringType(), True),
            StructField("user_activation_date", StringType(), True),
            StructField("status", StringType(), True),
            StructField("flags", StringType(), True),
            StructField("vendor_ids", StringType(), True),
            StructField("alias", StringType(), True),
            StructField("company", StringType(), True),
            StructField("event", StringType(), True),
            StructField("change_log", StringType(), True),
            StructField("territory_permissions", StringType(), True)])

        # NOTE:
        # The validFrom and validUntil items are currently not used in any way, but I thought
        # it could potentially be used in the future in case the version/column number could then be
        # switched out with a date instead of the number of columns.
        # It is also just a sanity check because in time, there will be corporate memory loss of when
        # a schema changed - and this should be captured somewhere.

        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 23
        self.schemaDict[1]['validFrom'] = '20221011'
        self.schemaDict[1]['validUntil'] = '20230322'

        self.schemaDict[2] = {'ver': 2}
        self.schemaDict[2]['cols'] = 24
        self.schemaDict[2]['validFrom'] = '20230322'
        self.schemaDict[2]['validUntil'] = '99991231'
