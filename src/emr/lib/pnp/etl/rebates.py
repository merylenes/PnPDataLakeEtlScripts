"""
ACZ: s3://pnp-data-lake-prod-acz-retail-euw1/bw/td/hrrbw01
STZ: s3://pnp-data-lake-prod-stz-retail-euw1/td/rebates/rebates_bw_hrrbw01
"""

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, LongType, DecimalType
from pnp.etl.datasets import dataset

class rebatesData(dataset):

    def __init__(self, configParams):
        super().__init__(configParams)

        # Specific hudi_options per dataset
        self.hudi_options = {
          "hoodie.datasource.write.recordkey.field": "HREFDOC,HREFDOCIT,HREFDOCYR,HREBDOCTY,HREBTYPE,HCO_AREA,HCOMPCODE,HPROFCTR,HVENDOR,HKSCHL",
          "hoodie.datasource.write.partitionpath.field": "CALDAY",
          "hoodie.datasource.hive_sync.partition_fields": "CALDAY"
        }

        # Join all the hudi_options for this dataset
        self.hudi_options.update(dataset.hudi_options)

        self.datasetSchemaEvo = True

        self.Schema = StructType([
            StructField("DI_REQTSN", StringType(), False),
            StructField("DI_SEQ_NR", LongType(), False),
            StructField("HREFDOC", StringType(), False),
            StructField("HREFDOCIT", StringType(), False),
            StructField("HREFDOCYR", IntegerType(), False),
            StructField("HREBDOCTY", StringType(), False),
            StructField("HREBTYPE", StringType(), False),
            StructField("HCO_AREA", StringType(), False),
            StructField("HCOMPCODE", StringType(), False),
            StructField("HPROFCTR", StringType(), False),
            StructField("HVENDOR", StringType(), False),
            StructField("HKSCHL", StringType(), False),
            StructField("HMATDOC", StringType(), True),
            StructField("HMATDOCIT", StringType(), True),
            StructField("HMATDOCYR", IntegerType(), True),
            StructField("HBILDOC", LongType(), True),
            StructField("HBILDOCIT", StringType(), True),
            StructField("HFRADOC", IntegerType(), True),
            StructField("HFRADOCRE", StringType(), True),
            StructField("HREBREF", StringType(), True),
            StructField("HREBERR", StringType(), True),
            StructField("HREBTAXCD", StringType(), True),
            StructField("HREBPSTFI", StringType(), True),
            StructField("HPLANT", StringType(), True),
            StructField("HMATERIAL", StringType(), True),
            StructField("HINV_PTY", StringType(), True),
            StructField("CALDAY", LongType(), True),
            StructField("HPOSTDAT", LongType(), True),
            StructField("HENTDAT", LongType(), True),
            StructField("HVALIDTO", LongType(), True),
            StructField("BASE_UOM", StringType(), True),
            StructField("SALES_UNIT", StringType(), True),
            StructField("GRP_CURRCY", StringType(), True),
            StructField("LOC_CURRCY", StringType(), True),
            StructField("HREBQTYBU", DecimalType(17,3), True),
            StructField("HREBQTYSU", DecimalType(17,3), True),
            StructField("HREBVALGC", DecimalType(17,2), True),
            StructField("HREBVALLC", DecimalType(17,2), True),
            StructField("HREBFVAGC", DecimalType(17,2), True),
            StructField("HREBFVALC", DecimalType(17,2), True),
            StructField("HREBVATGC", DecimalType(17,2), True),
            StructField("HREBVATLC", DecimalType(17,2), True),
            StructField("HREBEXVGC", DecimalType(17,2), True),
            StructField("HREBEXVLC", DecimalType(17,2), True)])

        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 44
        self.schemaDict[1]['validFrom'] = '19000101'
        self.schemaDict[1]['validUntil'] = '29991231'