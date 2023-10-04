"""
ACZ: s3://pnp-data-lake-prod-acz-finance-euw1/bw/td/hfglm03
STZ: s3://pnp-data-lake-prod-stz-finance-euw1/td/margin_man_adj/margin_man_adj_hfglm03
"""

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, LongType, DecimalType
from pnp.etl.datasets import dataset

class marginmanualadjustmentsData(dataset):

    def __init__(self, configParams):
        super().__init__(configParams)

        self.hudi_options = {
          'hoodie.datasource.write.recordkey.field': 'HPLANT,HMATERIAL,SALES_UNIT,HFIADJREC,HVENDOR,HGLACCNT,HCHRTACC,HPYEQFLG',
          'hoodie.datasource.write.partitionpath.field': 'CALDAY',
          'hoodie.datasource.hive_sync.partition_fields': 'CALDAY',
        }

        self.hudi_options.update(dataset.hudi_options)

        self.datasetSchemaEvo = True

        self.Schema = StructType([
            StructField("DI_REQTSN", StringType(),False),
            StructField("DI_SEQ_NR", IntegerType(),False),
            StructField("HPLANT", StringType(),False),
            StructField("HMATERIAL", StringType(),False),
            StructField("CALDAY", LongType(),False),
            StructField("SALES_UNIT", StringType(),False),
            StructField("HFIADJREC", StringType(),False),
            StructField("HVENDOR", StringType(),False),
            StructField("HGLACCNT", StringType(),False),
            StructField("HCHRTACC", StringType(),False),
            StructField("HPYEQFLG", StringType(),False),
            StructField("FISCPER3", StringType(),True),
            StructField("HFIMADD", StringType(),True),
            StructField("HFIMACOM", StringType(),True),
            StructField("LOC_CURRCY", StringType(),True),
            StructField("HFIPMSLC", DecimalType(17,2),True),
            StructField("HFIPMSLLC", DecimalType(17,2),True),
            StructField("HFIDMSLC", DecimalType(17,2),True),
            StructField("HFIDMSLLC", DecimalType(17,2),True),
            StructField("HFIMMLC", DecimalType(17,2),True),
            StructField("HFIMMLLC", DecimalType(17,2),True),
            StructField("HFICORLC", DecimalType(17,2),True),
            StructField("HFICORLLC", DecimalType(17,2),True),
            StructField("HFIGSDLC", DecimalType(17,2),True),
            StructField("HFIGSDLLC", DecimalType(17,2),True),
            StructField("HFICRRLC", DecimalType(17,2),True),
            StructField("HFICRRLLC", DecimalType(17,2),True)])

        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 27
        self.schemaDict[1]['validFrom'] = '19000101'
        self.schemaDict[1]['validUntil'] = '29991231'

