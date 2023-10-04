"""
ACZ: s3://pnp-data-lake-prod-acz-retail-euw1/bw/td/hrsaw03
STZ: s3://pnp-data-lake-prod-stz-retail-euw1/td/sales/sales_bw_hrsaw03
"""

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, LongType, DecimalType
from pnp.etl.datasets import dataset

class salesplancategorycoreData(dataset):

    def __init__(self, configParams):
        super().__init__(configParams)

        self.hudi_options = {}

        # Join all the hudi_options for this dataset
        self.hudi_options.update(dataset.hudi_options)

        # Specific hudi_options per dataset
        self.local_hudi_options = {
            'hoodie.datasource.write.recordkey.field': 'HPLANT,HAH_CDT6,CALDAY,HSALPLVER',
            'hoodie.datasource.write.partitionpath.field': 'CALDAY',
            'hoodie.datasource.hive_sync.partition_fields': 'CALDAY',
            'hoodie.cleaner.policy': 'KEEP_LATEST_FILE_VERSIONS',
            'hoodie.cleaner.commits.retained': 1,
            'hoodie.keep.min.commits': 2,
            'hoodie.keep.max.commits': 5,
            'hoodie.datasource.write.operation': 'insert_overwrite',
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
            StructField("HSPRECTYP", StringType(),False),
            StructField("LOC_CURRCY", StringType(),False),
            StructField("GRP_CURRCY", StringType(),False),
            StructField("HSPVNEVLC", DecimalType(17,2),False),
            StructField("HSPVNEVGC", DecimalType(17,2),False),
            StructField("HSPRE10LC", DecimalType(17,2),False),
            StructField("HSPRE10GC", DecimalType(17,2),False),
            StructField("HSPRE20LC", DecimalType(17,2),False),
            StructField("HSPRE20GC", DecimalType(17,2),False),
            StructField("HSPRE30LC", DecimalType(17,2),False),
            StructField("HSPRE30GC", DecimalType(17,2),False),
            StructField("HSPSALLLC", DecimalType(17,2),False),
            StructField("HSPSALLGC", DecimalType(17,2),False),
            StructField("HSPTALLLC", DecimalType(17,2),False),
            StructField("HSPTALLGC", DecimalType(17,2),False),
            StructField("HSPSWLC", DecimalType(17,2),False),
            StructField("HSPSWLGC", DecimalType(17,2),False),
            StructField("HSPWASTLC", DecimalType(17,2),False),
            StructField("HSPWASTGC", DecimalType(17,2),False),
            StructField("HSPSHRKLC", DecimalType(17,2),False),
            StructField("HSPSHRKGC", DecimalType(17,2),False),
            StructField("HSPRTCLC", DecimalType(17,2),False),
            StructField("HSPRTCGC", DecimalType(17,2),False),
            StructField("HSPCOSLC", DecimalType(17,2),False),
            StructField("HSPCOSGC", DecimalType(17,2),False),
            StructField("HPLDCPLC", DecimalType(17,2),False),
            StructField("HPLGLALC", DecimalType(17,2),False),
            StructField("HPLSMALC", DecimalType(17,2),False)])

        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 34
        self.schemaDict[1]['validFrom'] = '19000101'
        self.schemaDict[1]['validUntil'] = '29991231'


