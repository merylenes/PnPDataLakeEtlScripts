import re
import sys
import json
from tokenize import String

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DoubleType, LongType, TimestampType, DecimalType
from pyspark.sql.functions import col, concat, substring, format_string, regexp_replace

from pnp.etl.datasets import dataset

"""
ACZ: s3://pnp-data-lake-dev-acz-crm-euw1/crm/md/pnp_loy_bskt_in/
STZ: s3://pnp-data-lake-dev-stz-crm-euw1/md/loyalty/loyalty_crm_pnp_loy_bskt_in/
"""


class smartshopperpedpromptregistrationengagementData(dataset):

    def __init__(self, configParams):
        super().__init__(configParams)

        # Specific hudi_options per dataset
        self.hudi_options = {
            'hoodie.datasource.write.recordkey.field': 'GUID',
            'hoodie.datasource.write.partitionpath.field': 'CRDAT',
            'hoodie.datasource.hive_sync.partition_fields': 'CRDAT'
        }

        # Join all the hudi_options for this dataset
        self.hudi_options.update(dataset.hudi_options)

        # For absolutely brand new datasets, the datasetSchemaEvo should be set to True always
        # because it's used in testing for a variety of things in the codebase.

        self.datasetSchemaEvo = True
        # There can be only one ... schema, which is increasing in columns

        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 16
        # This is set to an arbitratry date in the past
        self.schemaDict[1]['validFrom'] = '20220921'
        # This is ostensibly set to whatever date this version of schema applies until
        self.schemaDict[1]['validUntil'] = '20230418'


        self.schemaDict[2] = {'ver': 2}
        self.schemaDict[2]['cols'] = 28
        self.schemaDict[2]['validFrom'] = '20230418'
        self.schemaDict[2]['validUntil'] = '20240418'


        self.Schema = StructType([
            StructField("DI_REQTSN", StringType(), False),
            StructField("DI_SEQ_NR", IntegerType(), False),
            StructField("GUID", StringType(), False),
            StructField("REF_GUID", StringType(), True),
            StructField("EXTERNAL_CARD_NU", StringType(), True),
            StructField("IDENTIFIER", StringType(), True),
            StructField("VALUE", StringType(), True),
            StructField("STATUS", StringType(), True),
            StructField("POSTING_DATE", DecimalType(15, 0), True),
            StructField("POSTING_DATE_TZ", StringType(), True),
            StructField("CRUSR", StringType(), True),
            StructField("CRDAT", LongType(), True),
            StructField("CRTIM", StringType(), True),
            StructField("CHUSR", StringType(), True),
            StructField("CHDAT", LongType(), True),
            StructField("CHTIM", StringType(), True),
            StructField("ACTION_CODE", StringType(), True),
            StructField("ACTION", StringType(), True),
            StructField("COMMUNICATION_STATUS", StringType(), True),
            StructField("REASON_CODE", StringType(), True),
            StructField("MSGID", StringType(), True),
            StructField("MSGNO", StringType(), True),
            StructField("MSGTEXT", StringType(), True),
            StructField("CELL_NUM_PARTNER_GUID", StringType(), True),
            StructField("CELL_NUM_PARTNER", StringType(), True),
            StructField("CELL_NUM_MSH_GUID", StringType(), True),
            StructField("CELL_NUM_CARD_TO_REPL", StringType(), True),
            StructField("LAST_CHANGED", DecimalType(15, 0), True)
        ])
