import re, sys, json

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DoubleType, LongType, TimestampType, DecimalType
from pyspark.sql.functions import col, concat, substring, format_string, regexp_replace

from pnp.etl.datasets import dataset

# Please make sure the DATASET_NAME is lowecase - the whole thing. So NO PosCore. Rather poscore.
class pnpmemberactivitiesData(dataset):

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
          'hoodie.datasource.write.recordkey.field': 'HCLMAOID,HCLMASAG',
          'hoodie.datasource.write.partitionpath.field': 'CALDAY',
          'hoodie.datasource.hive_sync.partition_fields': 'CALDAY'
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
            StructField("HCLMAOID", StringType(), False),
            StructField("HCLMASAG", StringType(), False),
            StructField("HCLMAGUID", StringType(), True),
            StructField("HCLMAPTYP", StringType(), True),
            StructField("HCLMACID", StringType(), True),
            StructField("HCLMAUDID", StringType(), True),
            StructField("HCLMRTNO", StringType(), True),
            StructField("HCLMARSN", StringType(), True),
            StructField("HCLMABIID", StringType(), True),
            StructField("HCLMASVAL", StringType(), True),
            StructField("HCLMAPVP", StringType(), True),
            StructField("HCLMRRGTP", StringType(), True),
            StructField("HCLMAPRTP", StringType(), True),
            StructField("HCLMABVAL", StringType(), True),
            StructField("HCLMAPST", StringType(), True),
            StructField("HCLMASUOM",StringType(), True),
            StructField("HCLMACHL", StringType(), True),
            StructField("HCRLYCHNL", StringType(), True),
            StructField("HCLMP1SDT", LongType(), True),
            StructField("HCLM1SCHN", StringType(), True),
            StructField("HCRLPID", StringType(), True),
            StructField("HCRMSHID", StringType(), True),
            StructField("H1STSWDAT", LongType(), True),
            StructField("HCRMSHCRD", StringType(), True),
            StructField("HCRMEMBER", StringType(), True),
            StructField("HCBQFPIND", StringType(), True),
            StructField("HCP1SWIND", StringType(), True),
            StructField("HCBAPTIND", StringType(), True),
            StructField("HCBOPTIND", StringType(), True),
            StructField("HCRMARTTP", StringType(), True),
            StructField("HCRMTAXIN", StringType(), True),
            StructField("HCRMDISIN", StringType(), True),
            StructField("HCRMLOYEX", StringType(), True),
            StructField("HCLMBSTID", StringType(), True),
            StructField("HPLANT", StringType(), True),
            StructField("HPAWID", StringType(), True),
            StructField("HPATNR", StringType(), True),
            StructField("HMATERIAL", StringType(), True),
            StructField("HEANUPC", StringType(), True),
            StructField("HCRMKCAMP", StringType(), True),
            StructField("HCMKCOUPT", StringType(), True),
            StructField("HCMKCPVC", StringType(), True),
            StructField("HCMCMFLR", StringType(), True),
            StructField("CALDAY", LongType(), True),
            StructField("HCRLPCRDT", LongType(), True),
            StructField("BASE_UOM", StringType(), True),
            StructField("SALES_UNIT", StringType(), True),
            StructField("DOC_CURRCY", StringType(), True),
            StructField("LOC_CURRCY", StringType(), True),
            StructField("GRP_CURRCY", StringType(), True),
            StructField("HCLMSQYBU", DecimalType(17,3), True),
            StructField("HCLMSQYSU", DecimalType(17,3), True),
            StructField("HCLM3PRDC", DecimalType(17,2), True),
            StructField("HCLM3PRGC", DecimalType(17,2), True),
            StructField("HCLM3PRLC", DecimalType(17,2), True),
            StructField("HCLMLAIDC", DecimalType(17,2), True),
            StructField("HCLMLAILC", DecimalType(17,2), True),
            StructField("HCLMLAIGC", DecimalType(17,2), True),
            StructField("HCLMLDIDC", DecimalType(17,2), True),
            StructField("HCLMLDILC", DecimalType(17,2), True),
            StructField("HCLMLDIGC", DecimalType(17,2), True),
            StructField("HCLMTAIDC", DecimalType(17,2), True),
            StructField("HCLMTAILC", DecimalType(17,2), True),
            StructField("HCLMTAIGC", DecimalType(17,2), True),
            StructField("HCLMDAIDC", DecimalType(17,2), True),
            StructField("HCLMDAILC", DecimalType(17,2), True),
            StructField("HCLMDAIGC", DecimalType(17,2), True),
            StructField("HCLMARWDR", DecimalType(17,3), True),
            StructField("HCLMBAPTS", DecimalType(17,3), True),
            StructField("HCLMBOPTS", DecimalType(17,3), True),
            StructField("HCLMTOPTS", DecimalType(17,3), True),            
            StructField("HCLMDBIND", StringType(), True),
            StructField("HCLMBAILC", DecimalType(17,2), True),
            StructField("HCLMBAIGC", DecimalType(17,2), True),
            StructField("HCLMBAIDC", DecimalType(17,2), True),
            StructField("HCLMBCDLC", DecimalType(17,2), True),
            StructField("HCLMBCDGC", DecimalType(17,2), True),
            StructField("HCLMBCDDC", DecimalType(17,2), True),
            StructField("HCLMTEILC", DecimalType(17,2), True),
            StructField("HCLMTEIGC", DecimalType(17,2), True),
            StructField("HCLMTEIDC", DecimalType(17,2), True),
            StructField("HCLMLIDLC", DecimalType(17,2), True),
            StructField("HCLMLIDGC", DecimalType(17,2), True),
            StructField("HCLMLIDDC", DecimalType(17,2), True),
            StructField("HCLMBLDGC", DecimalType(17,2), True),
            StructField("HCLMBLDLC", DecimalType(17,2), True),
            StructField("HCLMBLDDC", DecimalType(17,2), True)])

        # NOTE:
        # The validFrom and validUntil items are currently not used in any way, but I thought
        # it could potentially be used in the future in case the version/column number could then be
        # switched out with a date instead of the number of columns.
        # It is also just a sanity check because in time, there will be corporate memory loss of when
        # a schema changed - and this should be captured somewhere.

        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 73
        self.schemaDict[1]['validFrom'] = '20220926' # This is set to an arbitratry date in the past
        self.schemaDict[1]['validUntil'] = '20220926' # This is ostensibly set to whatever date this version of schema applies until

        self.schemaDict[2] = {'ver':2}
        self.schemaDict[2]['cols'] = 89
        self.schemaDict[2]['validFrom'] = '20230721'
        self.schemaDict[2]['validUntil'] = '20300721'