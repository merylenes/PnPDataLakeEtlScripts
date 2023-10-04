"""
ACZ: s3://pnp-data-lake-prod-acz-retail-euw1/bw/td/hsfaw01/
STZ: s3://pnp-data-lake-prod-stz-retail-euw1/td/forecasting/forecasting_bw_hsfaw01/
"""

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, LongType, DecimalType
from pnp.etl.datasets import dataset

class forecastingData(dataset):

    def __init__(self, configParams):
        super().__init__(configParams)

        # Specific hudi_options per dataset
        self.hudi_options = {
          'hoodie.datasource.write.recordkey.field': 'HPLANT,HMATERIAL',
          'hoodie.datasource.write.partitionpath.field': 'CALDAY',
          'hoodie.datasource.hive_sync.partition_fields': 'CALDAY'
        }

        # Join all the hudi_options for this dataset
        self.hudi_options.update(dataset.hudi_options)

        self.datasetSchemaEvo = True

        self.Schema = StructType([
            StructField("DI_REQTSN", StringType(),False),
            StructField("DI_SEQ_NR", IntegerType(),False),
            StructField("HPLANT", StringType(),False),
            StructField("HMATERIAL", StringType(),False),
            StructField("CALDAY", LongType(),False),
            StructField("HSUPPLANT", StringType(),True),
            StructField("HVENDOR", StringType(),True),
            StructField("HFRNOPER1", StringType(),True),
            StructField("HFRNOPER2", StringType(),True),
            StructField("HFRREPTYP", StringType(),True),
            StructField("HFRPRSURE", StringType(),True),
            StructField("HFRLISTAT", StringType(),True),
            StructField("HFRAFOTYP", StringType(),True),
            StructField("HFRREPPLA", StringType(),True),
            StructField("HFRSELCLA", StringType(),True),
            StructField("LOC_CURRCY", StringType(),True),
            StructField("GRP_CURRCY", StringType(),True),
            StructField("BASE_UOM", StringType(),True),
            StructField("HFRWCONBU", DecimalType(17,3),True),
            StructField("HFRWCONGC", DecimalType(17,2),True),
            StructField("HFRWCONLC", DecimalType(17,2),True),
            StructField("HFRFCONBU", DecimalType(17,3),True),
            StructField("HRWAFQTBU", DecimalType(17,3),True),
            StructField("HFRWDIFBU", DecimalType(17,3),True)])

        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 24
        self.schemaDict[1]['validFrom'] = '19000101'
        self.schemaDict[1]['validUntil'] = '29991231'