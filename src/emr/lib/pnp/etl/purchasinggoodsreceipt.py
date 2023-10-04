"""
ACZ: s3://pnp-data-lake-prod-acz-retail-euw1/bw/td/hrpuw03
STZ: s3://pnp-data-lake-prod-stz-retail-euw1/td/purchasing/purchasing_bw_hrpuw03
"""

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, LongType, DecimalType
from pnp.etl.datasets import dataset

class purchasinggoodsreceiptData(dataset):

    def __init__(self, configParams):
        super().__init__(configParams)

        # Specific hudi_options per dataset
        self.hudi_options = {
          'hoodie.datasource.write.recordkey.field': 'HPODOC,HPODOCIT,HPODOCSC',
          'hoodie.datasource.write.partitionpath.field': 'CALDAY',
          'hoodie.datasource.hive_sync.partition_fields': 'CALDAY'
        }

        # Join all the hudi_options for this dataset
        self.hudi_options.update(dataset.hudi_options)

        self.datasetSchemaEvo = True

        self.Schema = StructType([
            StructField("DI_REQTSN", StringType(),False),
            StructField("DI_SEQ_NR", IntegerType(),False),
            StructField("HPODOC", StringType(),False),
            StructField("HPODOCIT", StringType(),False),
            StructField("HPODOCSC", StringType(),False),
            StructField("CALDAY", LongType(),False),
            StructField("HPO_TYP", StringType(),True),
            StructField("HPO_CAT", StringType(),True),
            StructField("HPO_ITMCT", StringType(),True),
            StructField("HPOSTATUS", StringType(),True),
            StructField("HPROCEDUR", StringType(),True),
            StructField("HSOUSTYPE", StringType(),True),
            StructField("HPLANT", StringType(),True),
            StructField("HSTOR_LOC", StringType(),True),
            StructField("HPO_GRP", StringType(),True),
            StructField("HSUPPLANT", StringType(),True),
            StructField("HMATERIAL", StringType(),True),
            StructField("HVENDOR", StringType(),True),
            StructField("HINV_PTY", StringType(),True),
            StructField("HDOCDAT", LongType(),True),
            StructField("HENTDAT", LongType(),True),
            StructField("HGRDAT", LongType(),True),
            StructField("HSCHEDAT", LongType(),True),
            StructField("HPDELDAT", LongType(),True),
            StructField("HSDELDAT", LongType(),True),
            StructField("HPOITMRET", StringType(),True),
            StructField("HPODELCOM", StringType(),True),
            StructField("HPOINVIND", StringType(),True),
            StructField("HPOGRBINV", StringType(),True),
            StructField("HPOERSIND", StringType(),True),
            StructField("HPOSRCIND", StringType(),True),
            StructField("BASE_UOM", StringType(),True),
            StructField("PO_UNIT", StringType(),True),
            StructField("DOC_CURRCY", StringType(),True),
            StructField("LOC_CURRCY", StringType(),True),
            StructField("GRP_CURRCY", StringType(),True),
            StructField("HPOGRQTYO", DecimalType(17,3),True),
            StructField("HPOGRQTYB", DecimalType(17,3),True),
            StructField("HPOGRCVDC", DecimalType(17,2),True),
            StructField("HPOGRCVLC", DecimalType(17,2),True),
            StructField("HPOGRCVGC", DecimalType(17,2),True),
            StructField("HPOGRRILC", DecimalType(17,2),True),
            StructField("HPOGRRIGC", DecimalType(17,2),True),
            StructField("HPOGRRELC", DecimalType(17,2),True),
            StructField("HPOGRREGC", DecimalType(17,2),True),
            StructField("HPOGRNOLI", IntegerType(),True),
            StructField("HPOREQTYO", DecimalType(17,3),True),
            StructField("HPOREQTYB", DecimalType(17,3),True),
            StructField("HPORECVDC", DecimalType(17,2),True),
            StructField("HPORECVLC", DecimalType(17,2),True),
            StructField("HPORECVGC", DecimalType(17,2),True),
            StructField("HPORERILC", DecimalType(17,2),True),
            StructField("HPORERIGC", DecimalType(17,2),True),
            StructField("HPORERELC", DecimalType(17,2),True),
            StructField("HPOREREGC", DecimalType(17,2),True),
            StructField("HPORENOLI", IntegerType(),True),
            StructField("HPOCST1DC", DecimalType(17,2),True),
            StructField("HPOCST1LC", DecimalType(17,2),True),
            StructField("HPOCST1GC", DecimalType(17,2),True),
            StructField("HPOSRQTYO", DecimalType(17,3),True),
            StructField("HPOSRQTYB", DecimalType(17,3),True),
            StructField("HPOSRRELC", DecimalType(17,2),True),
            StructField("HPOSRREGC", DecimalType(17,2),True)])

        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 63
        self.schemaDict[1]['validFrom'] = '19000101'
        self.schemaDict[1]['validUntil'] = '29991231'