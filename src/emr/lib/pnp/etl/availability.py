import re, sys, json

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DoubleType, LongType, TimestampType, DecimalType
from pyspark.sql.functions import col, concat, substring, format_string, regexp_replace

from pnp.etl.datasets import dataset

# ACZ: s3://pnp-data-lake-prod-acz-retail-euw1/bw/td/hrmvw01
# STZ: s3://pnp-data-lake-prod-stz-retail-euw1/td/availability/availability_bw_hravm01
class availabilityData(dataset):

    def __init__(self, configParams):
        super().__init__(configParams)

        self.hudi_options = {}

        # Join all the hudi_options for this dataset
        self.hudi_options.update(dataset.hudi_options)
        
        # Specific hudi_options per dataset
        self.local_hudi_options = {
          'hoodie.datasource.write.recordkey.field': 'HPLANT,HMATERIAL',
          'hoodie.datasource.write.partitionpath.field': 'CALDAY',
          'hoodie.datasource.hive_sync.partition_fields': 'CALDAY'
        }

        # Join all the hudi_options for this dataset
        self.hudi_options.update(self.local_hudi_options)

        self.datasetSchemaEvo = True

        self.Schema = StructType([
            StructField("DI_REQTSN", StringType(),False),
            StructField("DI_SEQ_NR", IntegerType(),False),
            StructField("HPLANT", StringType(),False),
            StructField("HMATERIAL", StringType(),False),
            StructField("CALDAY", LongType(),False),
            StructField("HAVMIND", StringType(),True),
            StructField("HAVYIND", StringType(),True),
            StructField("HINSIND", StringType(),True),
            StructField("HSLSIND", StringType(),True),
            StructField("HSMPIND", StringType(),True),
            StructField("HPROM_YN", StringType(),True),
            StructField("BASE_UOM", StringType(),True),
            StructField("HSQTYBU", DecimalType(17,3),True),
            StructField("HSQDROSBU", DecimalType(17,3),True),
            StructField("HSQCROSBU", DecimalType(17,3),True),
            StructField("HSRPROSBU", DecimalType(17,3),True),
            StructField("HSQMPBU", DecimalType(17,3),True),
            StructField("HSVSQTYBU", DecimalType(17,3),True),
            StructField("HSVTQTYBU", DecimalType(17,3),True),
            StructField("HSVGEVLC", DecimalType(17,2),True),
            StructField("HSVGEVGC", DecimalType(17,2),True),
            StructField("HSVDROSLC", DecimalType(17,2),True),
            StructField("HSVDROSGC", DecimalType(17,2),True),
            StructField("HSVTSVELC", DecimalType(17,2),True),
            StructField("HSVTSVEGC", DecimalType(17,2),True),
            StructField("HPOGRQTYB", DecimalType(17,3),True),
            StructField("HPOREQTYB", DecimalType(17,3),True),
            StructField("LOC_CURRCY", StringType(),True),
            StructField("GRP_CURRCY", StringType(),True),
            StructField("HSVNEVLC", DecimalType(17,3),True),
            StructField("HSRDRSNLC", DecimalType(17,3),True),
            StructField("HSVDRSNLC", DecimalType(17,3),True),
            StructField("HSVDRSNGC", DecimalType(17,3),True)
            ])

        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 29
        self.schemaDict[1]['validFrom'] = '20200906'
        self.schemaDict[1]['validUntil'] = '20230310'

        self.schemaDict[2] = {'ver': 2}
        self.schemaDict[2]['cols'] = 31
        self.schemaDict[2]['validFrom'] = '20230310'
        self.schemaDict[2]['validUntil'] = '20230829'

        self.schemaDict[3] = {'ver': 3}
        self.schemaDict[3]['cols'] = 33
        self.schemaDict[3]['validFrom'] = '20230829'
        self.schemaDict[3]['validUntil'] = '99991231'