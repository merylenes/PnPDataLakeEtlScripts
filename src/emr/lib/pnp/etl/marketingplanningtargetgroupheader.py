"""
ACZ: s3://pnp-data-lake-prod-acz-crm-euw1/bw/td/hcmpw06/
STZ: s3://pnp-data-lake-prod-stz-crm-euw1/td/target_grp/target_grp_bw_hcmpw06/
"""

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, LongType, DecimalType
from pnp.etl.datasets import dataset

class marketingplanningtargetgroupheaderData(dataset):

    def __init__(self, configParams):
        super().__init__(configParams)

        self.hudi_options = {}

        # Join all the hudi_options for this dataset
        self.hudi_options.update(dataset.hudi_options)

        self.local_hudi_options = {
          'hoodie.datasource.write.recordkey.field': 'HVALIDTO,HCRMKTGGP,FISCPER3',
          'hoodie.datasource.write.partitionpath.field': 'HCMKTGTYP',
          'hoodie.datasource.hive_sync.partition_fields': 'HCMKTGTYP'
        }

        # Join all the hudi_options for this dataset
        self.hudi_options.update(self.local_hudi_options)

        self.datasetSchemaEvo = True

        self.Schema = StructType([
            StructField("DI_REQTSN", StringType(), False),
            StructField("DI_SEQ_NR", IntegerType(), False),
            StructField("HCAPTUSER", StringType(), True),
            StructField("HCREATDAT", LongType(), True),
            StructField("HCREATTIM", StringType(), True),
            StructField("HCHANUSER", StringType(), True),
            StructField("HCHANGDAT", LongType(), True),
            StructField("HCHANGTIM", StringType(), True),
            StructField("HVALIDFRM", LongType(), True),
            StructField("HVALIDTO",  LongType(), False),
            StructField("HDELIND", StringType(), True),
            StructField("HCMKTGID", StringType(), True),
            StructField("HCRMKTGGP", StringType(), False),
            StructField("HCMKTGTYP", StringType(), True),
            StructField("HCMKCMETG", DecimalType(17,3), True),
            StructField("HCMKTGCNT", IntegerType(), True),
            StructField("FISCPER3", StringType(),True),
            StructField("HCMKTGID_TXT", StringType(),True)
            ])

        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 16
        self.schemaDict[1]['validFrom'] = '20221108'
        self.schemaDict[1]['validUntil'] = '20230523'

        self.schemaDict[2] = {'ver': 2}
        self.schemaDict[2]['cols'] = 17
        self.schemaDict[2]['validFrom'] = '20230523'
        self.schemaDict[2]['validUntil'] = '99991231'
        
        self.schemaDict[3] = {'ver': 3}
        self.schemaDict[3]['cols'] = 18
        self.schemaDict[3]['validFrom'] = '20230723'
        self.schemaDict[3]['validUntil'] = '99991231'