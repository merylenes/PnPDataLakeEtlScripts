"""
ACZ: s3://pnp-data-lake-prod-acz-retail-euw1/bw/td/hrpuw01
STZ: s3://pnp-data-lake-prod-stz-retail-euw1/td/purchasing/purchasing_bw_hrpuw01
"""

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, LongType, DecimalType
from pnp.etl.datasets import dataset

# 
# STZ: 
class purchasingdocumentdateData(dataset):

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
            StructField("CALDAY", LongType(),True),
            StructField("HDOCDAT", LongType(),True),
            StructField("HENTDAT", LongType(),True),
            StructField("HPOSTDAT", LongType(),True),
            StructField("HGRDAT", LongType(),True),
            StructField("HINVDAT", LongType(),True),
            StructField("HSCHEDAT", LongType(),True),
            StructField("HPDELDAT", LongType(),True),
            StructField("HSDELDAT", LongType(),True),
            StructField("HPOITMDEL", StringType(),True),
            StructField("HPOITMRET", StringType(),True),
            StructField("HPODELCOM", StringType(),True),
            StructField("HPOINVIND", StringType(),True),
            StructField("HPOGRBINV", StringType(),True),
            StructField("HPOERSIND", StringType(),True),
            StructField("HPOFUNOI", StringType(),True),
            StructField("HPOFUYESI", StringType(),True),
            StructField("HPOTINOI", StringType(),True),
            StructField("HPOTIYESI", StringType(),True),
            StructField("HPOSRNOI", StringType(),True),
            StructField("HPOSRYESI", StringType(),True),
            StructField("HPOSRCIND", StringType(),True),
            StructField("BASE_UOM", StringType(),True),
            StructField("PO_UNIT", StringType(),True),
            StructField("DOC_CURRCY", StringType(),True),
            StructField("GRP_CURRCY", StringType(),True),
            StructField("LOC_CURRCY", StringType(),True),
            StructField("HPOORQTYO", DecimalType(17,3),True),
            StructField("HPOORQTYB", DecimalType(17,3),True),
            StructField("HPOORCVDC", DecimalType(17,2),True),
            StructField("HPOORCVLC", DecimalType(17,2),True),
            StructField("HPOORCVGC", DecimalType(17,2),True),
            StructField("HPOORRILC", DecimalType(17,2),True),
            StructField("HPOORRIGC", DecimalType(17,2),True),
            StructField("HPOORRELC", DecimalType(17,2),True),
            StructField("HPOORREGC", DecimalType(17,2),True),
            StructField("HPOORNOLI", IntegerType(),True),
            StructField("HPOPRQTYO", DecimalType(17,3),True),
            StructField("HPOPRQTYB", DecimalType(17,3),True),
            StructField("HPOPRCVDC", DecimalType(17,2),True),
            StructField("HPOPRCVLC", DecimalType(17,2),True),
            StructField("HPOPRCVGC", DecimalType(17,2),True),
            StructField("HPOPRRILC", DecimalType(17,2),True),
            StructField("HPOPRRIGC", DecimalType(17,2),True),
            StructField("HPOPRRELC", DecimalType(17,2),True),
            StructField("HPOPRREGC", DecimalType(17,2),True),
            StructField("HPOPRNOLI", IntegerType(),True),
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
            StructField("HPOIVQTYO", DecimalType(17,3),True),
            StructField("HPOIVQTYB", DecimalType(17,3),True),
            StructField("HPOIVCVDC", DecimalType(17,2),True),
            StructField("HPOIVCVLC", DecimalType(17,2),True),
            StructField("HPOIVCVGC", DecimalType(17,2),True),
            StructField("HPOIVNOLI", IntegerType(),True),
            StructField("HPOCMQTYO", DecimalType(17,3),True),
            StructField("HPOCMQTYB", DecimalType(17,3),True),
            StructField("HPOCMCVDC", DecimalType(17,2),True),
            StructField("HPOCMCVLC", DecimalType(17,2),True),
            StructField("HPOCMCVGC", DecimalType(17,2),True),
            StructField("HPOCMNOLI", IntegerType(),True),
            StructField("HPOFUNOK", IntegerType(),True),
            StructField("HPOFUYESK", IntegerType(),True),
            StructField("HPOTINOK", IntegerType(),True),
            StructField("HPOTIYESK", IntegerType(),True),
            StructField("HPOSRNOK", IntegerType(),True),
            StructField("HPOSRYESK", IntegerType(),True),
            StructField("HPOCST1DC", DecimalType(17,2),True),
            StructField("HPOCST1LC", DecimalType(17,2),True),
            StructField("HPOCST1GC", DecimalType(17,2),True),
            StructField("HPOSRQTYO", DecimalType(17,3),True),
            StructField("HPOSRQTYB", DecimalType(17,3),True),
            StructField("HPOSRRELC", DecimalType(17,2),True),
            StructField("HPOSRREGC", DecimalType(17,2),True)])

        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 110
        self.schemaDict[1]['validFrom'] = '19000101'
        self.schemaDict[1]['validUntil'] = '29991231'