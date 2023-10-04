"""
ACZ: s3://pnp-data-lake-dev-acz-crm-euw1/bw/td/hcmpw07/
STZ: s3://pnp-data-lake-dev-stz-crm-euw1/td/target_grp/target_grp_bw_hcmpw07/
"""

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, LongType, DecimalType
from pnp.etl.datasets import dataset

class marketingplanningtargetgroupData(dataset):

    def __init__(self, configParams):
        super().__init__(configParams)

        self.hudi_options = {}

        # Join all the hudi_options for this dataset
        self.hudi_options.update(dataset.hudi_options)

        # Specific hudi_options per dataset
        self.local_hudi_options = {
          'hoodie.datasource.write.recordkey.field': 'HVALIDTO,HCRMPTGGD,FISCPER3',
          'hoodie.datasource.write.partitionpath.field': 'HVALIDFRM',
          'hoodie.datasource.hive_sync.partition_fields': 'HVALIDFRM'
        }

        # Join all the hudi_options for this dataset
        self.hudi_options.update(self.local_hudi_options)

        self.datasetSchemaEvo = True
        
        self.Schema = StructType([
            StructField("DI_REQTSN", StringType(),False),
            StructField("DI_SEQ_NR", IntegerType(),False),
            StructField("HVALIDFRM", LongType(),False),
            StructField("HVALIDTO", LongType(),False),
            StructField("HDELIND", StringType(),False),
            StructField("HPROCESED", StringType(),False),
            StructField("HCMKTGID", StringType(),False),
            StructField("HCRMKTGGP", StringType(),False),
            StructField("HBPARTNER", StringType(),False),
            StructField("HBP_GUID", StringType(),False),
            StructField("HCRMPTGGD", StringType(),False),
            StructField("HCMKCMETG", DecimalType(17,3),True),
            StructField("_hoodie_is_deleted", StringType(),True),
            StructField("FISCPER3", StringType(),True)
            ])

        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 13
        self.schemaDict[1]['validFrom'] = '20220921'
        self.schemaDict[1]['validUntil'] = '20230523'

        self.schemaDict[2] = {'ver': 2}
        self.schemaDict[2]['cols'] = 14
        self.schemaDict[2]['validFrom'] = '20230523'
        self.schemaDict[2]['validUntil'] = '99991231'
