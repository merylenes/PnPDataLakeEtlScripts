"""
ACZ: s3://pnp-data-lake-prod-acz-crm-euw1/bw/td/hclpw01
STZ: s3://pnp-data-lake-prod-stz-crm-euw1/td/points_account/points_account_bw_hclpw01
"""

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, LongType, DecimalType
from pnp.etl.datasets import dataset

class pnppointsaccountData(dataset):

    def __init__(self, configParams):
        super().__init__(configParams)

        self.hudi_options = {}

        # Join all the hudi_options for this dataset
        self.hudi_options.update(dataset.hudi_options)
        
        # Specific hudi_options per dataset
        self.local_hudi_options = {
          'hoodie.datasource.write.recordkey.field': 'HCRLPGUID',
          'hoodie.datasource.write.partitionpath.field': 'CALDAY',
          'hoodie.datasource.hive_sync.partition_fields': 'CALDAY',
          'hoodie.memory.merge.fraction': '0.80',
          'hoodie.upsert.shuffle.parallelism':"50"

        }

        # Join all the hudi_options for this dataset
        self.hudi_options.update(self.local_hudi_options)

        self.datasetSchemaEvo = True

        self.Schema = StructType([
            StructField("DI_REQTSN", StringType(),False),
            StructField("DI_SEQ_NR", IntegerType(),False),
            StructField("CALDAY", LongType(),False),
            StructField("HCRLPGUID", StringType(),False),
            StructField("HPLANT", StringType(),True),
            StructField("HBPARTNER", StringType(),True),
            StructField("HPOSTDAT", LongType(),True),
            StructField("HCRLPCRDT", LongType(),True),
            StructField("HCRLPEXDT", LongType(),True),
            StructField("HCLMACHL", StringType(),True),
            StructField("HCRMSHTP", StringType(),True),
            StructField("HCRMSHID", StringType(),True),
            StructField("HCRMEMBER", StringType(),True),
            StructField("HCRMSHCRD", StringType(),True),
            StructField("HCRMKGUID", StringType(),True),
            StructField("HCRLYCHNL", StringType(),True),
            StructField("HCRLPRRID", StringType(),True),
            StructField("HCRLPID", StringType(),True),
            StructField("HCRLTTLVL", StringType(),True),
            StructField("HCRBTLVL", StringType(),True),
            StructField("HCRMKCAMP", StringType(),True),
            StructField("HCLMAUDID", StringType(),True),
            StructField("HCLMAOID", StringType(),True),
            StructField("HCLMARSN", StringType(),True),
            StructField("HCRLPTREF", StringType(),True),
            StructField("HCRLPQTYP", StringType(),True),
            StructField("HCRLPTID", StringType(),True),
            StructField("HCRLPTTYP", StringType(),True),
            StructField("HCRLPTRSN", StringType(),True),
            StructField("HCRLPNTYP", StringType(),True),
            StructField("HCACICVN", StringType(),True),
            StructField("HCACICPG", StringType(),True),
            StructField("HCRATDSC2", StringType(),True),
            StructField("HCRATDSC1", StringType(),True),
            StructField("HCRACICAG", StringType(),True),
            StructField("HCRATPACG", StringType(),True),
            StructField("HCRACICAP", StringType(),True),
            StructField("HCRMPAPNT", DecimalType(17,3),True)])

        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 38
        self.schemaDict[1]['validFrom'] = '19000101'
        self.schemaDict[1]['validUntil'] = '29991231'