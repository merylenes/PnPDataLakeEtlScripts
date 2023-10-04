"""
ACZ: s3://pnp-data-lake-prod-acz-retail-euw1/bw/td/hrmvw01
STZ: s3://pnp-data-lake-prod-stz-retail-euw1/td/article_movements/article_move_bw_hrmvw01
"""

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, LongType, DecimalType
from pnp.etl.datasets import dataset

class articlemovementsData(dataset):

    def __init__(self, configParams):
        super().__init__(configParams)

        self.hudi_options = {}

        # Join all the hudi_options for this dataset
        self.hudi_options.update(dataset.hudi_options)

        self.datasetSchemaEvo = True

        # Specific hudi_options per dataset
        self.local_hudi_options = {
          'hoodie.datasource.write.recordkey.field': 'HMATDOCYR,HMATDOC,HMATERIAL,HMATDOCIT,HMATCOUNT',
          'hoodie.datasource.write.partitionpath.field': 'CALDAY',
          'hoodie.datasource.hive_sync.partition_fields': 'CALDAY'
        }

        # Join all the hudi_options for this dataset
        self.hudi_options.update(self.local_hudi_options)

        self.datasetSchemaEvo = True

        self.Schema = StructType([
            StructField("DI_REQTSN", StringType(),False),
            StructField("DI_SEQ_NR", IntegerType(),False),
            StructField("HMATDOCYR", StringType(),False),
            StructField("HMATDOC", StringType(),False),
            StructField("HMATERIAL", StringType(),False),
            StructField("HMATDOCIT", StringType(),True),
            StructField("HMATCOUNT", StringType(),True),
            StructField("HPODOC", StringType(),True),
            StructField("HPODOCIT", StringType(),True),
            StructField("HPLANT", StringType(),True),	
            StructField("HSTOR_LOC", StringType(),True),
            StructField("HMOVPLANT", StringType(),True),
            StructField("HWHSE_NUM", StringType(),True),
            StructField("HCO_AREA", StringType(),True),
            StructField("HPROFCTR", StringType(),True),
            StructField("HPARTPCTR", StringType(),True),
            StructField("HCOSTCTR", StringType(),True),
            StructField("HCUSTOMER", StringType(),True),
            StructField("HVENDOR", StringType(),True),	
            StructField("CALDAY", LongType(),True),
            StructField("HPOSTDAT", LongType(),True),
            StructField("HDOCDAT", LongType(),True),
            StructField("HDCINDIC", StringType(),True),
            StructField("HSTOCKTYP", StringType(),True),
            StructField("HSTORBIN", StringType(),True),
            StructField("HSTORTYPE", StringType(),True),
            StructField("HSTOCKCAT", StringType(),True),
            StructField("HREVSTKRE", StringType(),True),
            StructField("HSTOCKVAL", StringType(),True),
            StructField("HSTOCKINS", StringType(),True),
            StructField("HMOVETYPE", StringType(),True),
            StructField("HMATMREA", StringType(),True),
            StructField("HCHRTACC", StringType(),True),
            StructField("HGLACCNT", StringType(),True),
            StructField("HBWAPPLNM", StringType(),True),
            StructField("HPROCKEY", StringType(),True),
            StructField("HSTORNO", StringType(),True),
            StructField("LOC_CURRCY", StringType(),True),
            StructField("GRP_CURRCY", StringType(),True),
            StructField("BASE_UOM", StringType(),True),
            StructField("PO_UNIT", StringType(),True),
            StructField("HENT_UNIT", StringType(),True),
            StructField("HMVQTYBU", DecimalType(17,3),True),
            StructField("HMVQTYOU", DecimalType(17,3),True),
            StructField("HMVQTYEU", DecimalType(17,3),True),
            StructField("HMVCOSTGC", DecimalType(17,2),True),
            StructField("HMVCOSTLC", DecimalType(17,2),True),
            StructField("HMVSVEVGC", DecimalType(17,2),True),
            StructField("HMVSVEVLC", DecimalType(17,2),True),
            StructField("HMVSVIVGC", DecimalType(17,2),True),
            StructField("HMVSVIVLC", DecimalType(17,2),True),
            StructField("HMVNOACT", IntegerType(),True)])

        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 52
        self.schemaDict[1]['validFrom'] = '19000101'
        self.schemaDict[1]['validUntil'] = '29991231'
        