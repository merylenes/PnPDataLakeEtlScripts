"""
ACZ: s3://pnp-data-lake-prod-acz-scp-euw1/bw/td/hssew01/
STZ: s3://pnp-data-lake-prod-stz-scp-euw1/td/stock_exceptions/stock_exceptions_bw_hssew01/
"""

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, LongType, DecimalType
from pnp.etl.datasets import dataset

class pnpstockexceptionsData(dataset):

    def __init__(self, configParams):
        super().__init__(configParams)

        self.hudi_options = {}

        # Join all the hudi_options for this dataset
        self.hudi_options.update(dataset.hudi_options)

        # Specific hudi_options per dataset
        self.local_hudi_options = {
            'hoodie.datasource.write.recordkey.field': 'HMATERIAL,HPLANT,CALDAY',
            'hoodie.datasource.write.partitionpath.field': 'CALDAY',
            'hoodie.datasource.hive_sync.partition_fields': 'CALDAY',
        }

        # Join all the hudi_options for this dataset
        self.hudi_options.update(self.local_hudi_options)

        self.datasetSchemaEvo = True

        self.Schema = StructType([
            StructField("DI_REQTSN", StringType(), False),
            StructField("DI_SEQ_NR", IntegerType(), False),
            StructField("HMATERIAL", StringType(), False),
            StructField("HPLANT", StringType(), False),
            StructField("CALDAY", LongType(), False),
            StructField("HSUPPLANT", StringType(), True),
            StructField("HVENDOR", StringType(), True),
            StructField("HFRREPTYP", StringType(), True),
            StructField("HFRROCTH", StringType(), True),
            StructField("HFRSTEXTY", StringType(), True),
            StructField("HFRREPLTD", StringType(), True),
            StructField("HFRSELCLA", StringType(), True),
            StructField("HFRREPPLA", StringType(), True),
            StructField("LOC_CURRCY", StringType(), True),
            StructField("GRP_CURRCY", StringType(), True),
            StructField("BASE_UOM", StringType(), True),
            StructField("HFRLCONBU", DecimalType(17,3), True),
            StructField("HFRLCONLC", DecimalType(17,2), True),
            StructField("HFRLCONGC", DecimalType(17,2), True),
            StructField("HFRDCONLC", DecimalType(17,2), True),
            StructField("HFRDCONGC", DecimalType(17,2), True),
            StructField("HFRDCONBU", DecimalType(17,3), True)])

        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 22
        self.schemaDict[1]['validFrom'] = '19000101'
        self.schemaDict[1]['validUntil'] = '29991231'
