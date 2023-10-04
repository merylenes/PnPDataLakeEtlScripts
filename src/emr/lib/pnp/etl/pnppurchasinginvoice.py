"""
ACZ: s3://pnp-data-lake-prod-acz-retail-euw1/bw/td/hrpuw04
STZ: s3://pnp-data-lake-prod-stz-retail-euw1/td/purchasing/purchasing_bw_hrpuw04
"""

from pyspark.sql.types import StructType, StructField, DecimalType
from pyspark.sql.types import StringType, IntegerType, LongType
from pnp.etl.datasets import dataset

class pnppurchasinginvoiceData(dataset):

    def __init__(self, configParams):
        super().__init__(configParams)

        self.hudi_options = {
            'hoodie.datasource.write.recordkey.field': 'HPODOC,HPODOCIT,HPODOCSC,CALDAY',
            'hoodie.datasource.write.partitionpath.field': 'CALDAY',
            'hoodie.datasource.hive_sync.partition_fields': 'CALDAY'
        }

        self.hudi_options.update(dataset.hudi_options)

        self.datasetSchemaEvo = True

        self.Schema = StructType([
            StructField("DI_REQTSN", StringType(),True),
            StructField("DI_SEQ_NR", IntegerType(),True),
            StructField("HPODOC", StringType(),True),
            StructField("HPODOCIT", StringType(),True),
            StructField("HPODOCSC", StringType(),True),
            StructField("CALDAY", LongType(),True),
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
            StructField("HINVDAT", LongType(),True),
            StructField("HSCHEDAT", LongType(),True),
            StructField("HPDELDAT", LongType(),True),
            StructField("HSDELDAT", LongType(),True),
            StructField("HPOITMRET", StringType(),True),
            StructField("HPODELCOM", StringType(),True),
            StructField("HPOINVIND", StringType(),True),
            StructField("HPOGRBINV", StringType(),True),
            StructField("HPOERSIND", StringType(),True),
            StructField("BASE_UOM", StringType(),True),
            StructField("PO_UNIT", StringType(),True),
            StructField("DOC_CURRCY", StringType(),True),
            StructField("GRP_CURRCY", StringType(),True),
            StructField("LOC_CURRCY", StringType(),True),
            StructField("HPOIVQTYO", DecimalType(17,2),True),
            StructField("HPOIVQTYB", DecimalType(17,2),True),
            StructField("HPOIVCVDC", DecimalType(17,2),True),
            StructField("HPOIVCVLC", DecimalType(17,2),True),
            StructField("HPOIVCVGC", DecimalType(17,2),True),
            StructField("HPOIVNOLI", IntegerType(),True),
            StructField("HPOCMQTYO", DecimalType(17,2),True),
            StructField("HPOCMQTYB", DecimalType(17,2),True),
            StructField("HPOCMCVDC", DecimalType(17,2),True),
            StructField("HPOCMCVLC", DecimalType(17,2),True),
            StructField("HPOCMCVGC", DecimalType(17,2),True),
            StructField("HPOCMNOLI", IntegerType(),True),
            StructField("HINVDATK", DecimalType(17,3),True),
            StructField("HDOCDATK", DecimalType(17,3),True),
            StructField("HPDELDATK", DecimalType(17,3),True),
            StructField("HSDELDATK", DecimalType(17,3),True),
            StructField("HPOCST1DC", DecimalType(17,2),True),
            StructField("HPOCST1LC", DecimalType(17,2),True),
            StructField("HPOCST1GC", DecimalType(17,2),True),
            StructField("FISCYEAR", StringType(),True),
            StructField("FISCVARNT", StringType(),True),
            StructField("FISCPER3", StringType(),True) 
            ])

        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 54
        self.schemaDict[1]['validFrom'] = '19000101'
        self.schemaDict[1]['validUntil'] = '20230515'

        self.schemaDict[2] = {'ver': 2}
        self.schemaDict[2]['cols'] = 57
        self.schemaDict[2]['validFrom'] = '20230515'
        self.schemaDict[2]['validUntil'] = '99991231'
