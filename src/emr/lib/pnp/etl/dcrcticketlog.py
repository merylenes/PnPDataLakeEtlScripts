"""
ACZ: s3://pnp-data-lake-prod-acz-retail-euw1/erp/td/dcrc_lg/
STZ: s3://pnp-data-lake-prod-stz-retail-euw1/td/dcrc/erp_dcrc_lg
"""

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, LongType
from pnp.etl.datasets import dataset

class dcrcticketlogData(dataset):

    def __init__(self, configParams):
        super().__init__(configParams)

        self.hudi_options = {}

        # Join all the hudi_options for this dataset
        self.hudi_options.update(dataset.hudi_options)

        # Specific hudi_options per dataset
        self.local_hudi_options = {
          'hoodie.datasource.write.recordkey.field': 'MANDT,DCRC_ID,COUNTER,LOG_TYPE',
          'hoodie.datasource.write.partitionpath.field': 'DATUM',
          'hoodie.datasource.hive_sync.partition_fields': 'DATUM'
        }

        # Join all the hudi_options for this dataset
        self.hudi_options.update(self.local_hudi_options)

        self.datasetSchemaEvo = True

        self.Schema = StructType([
            StructField("DI_REQTSN", StringType(),False),
            StructField("DI_SEQ_NR", IntegerType(),False),
            StructField("MANDT", StringType(),False),
            StructField("DCRC_ID", StringType(),False),
            StructField("COUNTER", StringType(),False),
            StructField("LOG_TYPE", StringType(),False),
            StructField("DATUM", LongType(),True),
            StructField("UZEIT", LongType(),True),
            StructField("ERNAM", StringType(),True),
            StructField("REF_VBTYP", StringType(),True),
            StructField("REF_VGBEL", StringType(),True),
            StructField("REF_GJAHR", StringType(),True),
            StructField("SRC_VBTYP", StringType(),True),
            StructField("SRC_VGBEL", StringType(),True),
            StructField("SRC_GJAHR", StringType(),True),
            StructField("NEW_VBTYP", StringType(),True),
            StructField("NEW_VGBEL", StringType(),True),
            StructField("NEW_GJAHR", StringType(),True),
            StructField("DCRC_DOCSPEC", StringType(),True),
            StructField("MSGID", StringType(),True),
            StructField("MSGTY", StringType(),True),
            StructField("MSGNO", StringType(),True),
            StructField("MSGV1", StringType(),True),
            StructField("MSGV2", StringType(),True),
            StructField("MSGV3", StringType(),True),
            StructField("MSGV4", StringType(),True),
            StructField("NEW_BUKRS", StringType(),True),
            StructField("REF_BUKRS", StringType(),True),
            StructField("SRC_BUKRS", StringType(),True),
            StructField("DCRC_PROCESS", StringType(),True),
            StructField("DCRC_SEQ", StringType(),True),
            StructField("DCRC_DOCVAR", StringType(),True)])

        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 32
        self.schemaDict[1]['validFrom'] = '19000101'
        self.schemaDict[1]['validUntil'] = '29991231'
