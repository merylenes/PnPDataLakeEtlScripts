"""
ACZ: s3://pnp-data-lake-prod-acz-finance-euw1/bw/td/hfglw02
STZ: s3://pnp-data-lake-prod-stz-finance-euw1/td/gl_line_items/gl_line_items_bw_hfglw02
"""

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, LongType, DecimalType
from pnp.etl.datasets import dataset

class gllineitemsData(dataset):

    def __init__(self, configParams):
        super().__init__(configParams)

        self.hudi_options = {}

        # Join all the hudi_options for this dataset
        self.hudi_options.update(dataset.hudi_options)

        # Specific hudi_options per dataset
        self.local_hudi_options = {
          'hoodie.datasource.write.recordkey.field': 'HCOMPCODE,HFILEDGER,HFIRECTYP,HVERSION,HFIDOCNO,HFIDOCITM',
          'hoodie.datasource.write.partitionpath.field': 'CALDAY',
          'hoodie.datasource.hive_sync.partition_fields': 'CALDAY'
        }

        # Join all the hudi_options for this dataset
        self.hudi_options.update(self.local_hudi_options)

        self.datasetSchemaEvo = True

        self.Schema = StructType([
            StructField("DI_REQTSN", StringType(),False),
            StructField("DI_SEQ_NR", IntegerType(),False),
            StructField("HCOMPCODE", StringType(),False),
            StructField("HFILEDGER", StringType(),False),
            StructField("HFIRECTYP", StringType(),False),
            StructField("HVERSION", StringType(),True),
            StructField("HFIDOCNO", StringType(),True),
            StructField("HFIDOCITM", StringType(),True),
            StructField("HFIADOCNO", StringType(),True),
            StructField("HFIADOCIT", StringType(),True),
            StructField("HFICLDOCN", StringType(),True),
            StructField("HFIREFDOC", StringType(),True),
            StructField("HPODOC", StringType(),True),
            StructField("HPODOCIT", StringType(),True),
            StructField("HBILDOC", StringType(),True),
            StructField("HSALDOC", StringType(),True),
            StructField("HREFKEY", StringType(),True),
            StructField("HFIDOCTYP", StringType(),True),
            StructField("HFIITSTAT", StringType(),True),
            StructField("HPOST_KEY", StringType(),True),
            StructField("HPOSTXT", StringType(),True),
            StructField("HREFKEY1", StringType(),True),
            StructField("HREFKEY2", StringType(),True),
            StructField("HREFKEY3", StringType(),True),
            StructField("HTAXCDE", StringType(),True),
            StructField("HFIASSNR", StringType(),True),
            StructField("HPLANT", StringType(),True),
            StructField("HCO_AREA", StringType(),True),
            StructField("HCOSTCTR", StringType(),True),
            StructField("HPROFCTR", StringType(),True),
            StructField("HSEGMENT", StringType(),True),
            StructField("HMATERIAL", StringType(),True),
            StructField("HVENDOR", StringType(),True),
            StructField("CALDAY", LongType(),True),
            StructField("HPOSTDAT", LongType(),True),
            StructField("HDOCDAT", LongType(),True),
            StructField("HCREATDAT", LongType(),True),
            StructField("HCLEARDAT", LongType(),True),
            StructField("HDCINDIC", StringType(),True),
            StructField("HCHRTACC", StringType(),True),
            StructField("HGLACCNT", StringType(),True),
            StructField("HGLACCTYP", StringType(),True),
            StructField("HFIASSETN", StringType(),True),
            StructField("HFIORDNO", StringType(),True),
            StructField("HCURTYPE2", StringType(),True),
            StructField("HFIBUSTRA", StringType(),True),
            StructField("HFIREFPRO", StringType(),True),
            StructField("HFITRTYPE", StringType(),True),
            StructField("BASE_UOM", StringType(),True),
            StructField("GRP_CURRCY", StringType(),True),
            StructField("DOC_CURRCY", StringType(),True),
            StructField("LOC_CURRCY", StringType(),True),
            StructField("HFICRE_DC", DecimalType(17,2),True),
            StructField("HFICRE_LC", DecimalType(17,2),True),
            StructField("HFICRE_GC", DecimalType(17,2),True),
            StructField("HFIDEB_DC", DecimalType(17,2),True),
            StructField("HFIDEB_LC", DecimalType(17,2),True),
            StructField("HFIDEB_GC", DecimalType(17,2),True),
            StructField("HFIAMT_DC", DecimalType(17,2),True),
            StructField("HFIAMT_LC", DecimalType(17,2),True),
            StructField("HFIAMT_GC", DecimalType(17,2),True),
            StructField("HFIQTYBU", DecimalType(17,3),True)])

        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 62
        self.schemaDict[1]['validFrom'] = '19000101'
        self.schemaDict[1]['validUntil'] = '29991231'
