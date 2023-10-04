"""
ACZ: s3://pnp-data-lake-prod-acz-retail-euw1/bw/td/hrsam03
STZ: s3://pnp-data-lake-prod-stz-retail-euw1/td/sales/sales_bw_hrsam03
"""

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, LongType, DecimalType
from pnp.etl.datasets import dataset

class salesplancategorydatamartData(dataset):

    def __init__(self, configParams):
        super().__init__(configParams)

        self.hudi_options = {}

        # Join all the hudi_options for this dataset
        self.hudi_options.update(dataset.hudi_options)

        # Specific hudi_options per dataset
        self.local_hudi_options = {
          'hoodie.datasource.write.recordkey.field': 'HPLANT,HAH_CDT6,HSALPLVER',
          'hoodie.datasource.write.partitionpath.field': 'CALDAY',
          'hoodie.datasource.hive_sync.partition_fields': 'CALDAY',
          'hoodie.cleaner.policy': 'KEEP_LATEST_FILE_VERSIONS',
          'hoodie.cleaner.commits.retained': 1,
          'hoodie.keep.min.commits': 2,
          'hoodie.keep.max.commits': 5,
          'hoodie.datasource.write.operation': 'insert_overwrite'
        }

        # Join all the hudi_options for this dataset
        self.hudi_options.update(self.local_hudi_options)

        self.datasetSchemaEvo = True

        self.Schema = StructType([
            StructField("DI_REQTSN", StringType(),False),
            StructField("DI_SEQ_NR", IntegerType(),False),
            StructField("HPLANT", StringType(),False),
            StructField("HAH_CDT6", StringType(),False),
            StructField("CALDAY", LongType(),False),
            StructField("HSALPLVER", StringType(),False),
            StructField("LOC_CURRCY", StringType(),True),
            StructField("HSPVNEVLC", DecimalType(17,2),True),
            StructField("HSPVNELLC", DecimalType(17,2),True),
            StructField("HSPRE10LC", DecimalType(17,2),True),
            StructField("HSPR10LLC", DecimalType(17,2),True),
            StructField("HSPRE20LC", DecimalType(17,2),True),
            StructField("HSPR20LLC", DecimalType(17,2),True),
            StructField("HSPRE30LC", DecimalType(17,2),True),
            StructField("HSPR30LLC", DecimalType(17,2),True),
            StructField("HSPSALLLC", DecimalType(17,2),True),
            StructField("HSPSLLLLC", DecimalType(17,2),True),
            StructField("HSPTALLLC", DecimalType(17,2),True),
            StructField("HSPTLLLLC", DecimalType(17,2),True),
            StructField("HSPSWLC", DecimalType(17,2),True),
            StructField("HSPSWLLC", DecimalType(17,2),True),
            StructField("HSPWASTLC", DecimalType(17,2),True),
            StructField("HSPWASLLC", DecimalType(17,2),True),
            StructField("HSPSHRKLC", DecimalType(17,2),True),
            StructField("HSPSHRLLC", DecimalType(17,2),True),
            StructField("HSPRTCLC", DecimalType(17,2),True),
            StructField("HSPRTCLLC", DecimalType(17,2),True),
            StructField("HSPCOSLC", DecimalType(17,2),True),
            StructField("HSPCOSLLC", DecimalType(17,2),True),
            StructField("HPLDCPLC", DecimalType(17,2),True),
            StructField("HPLGLALC", DecimalType(17,2),True),
            StructField("HPLSMALC", DecimalType(17,2),True)])

        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 32
        self.schemaDict[1]['validFrom'] = '19000101'
        self.schemaDict[1]['validUntil'] = '29991231'
