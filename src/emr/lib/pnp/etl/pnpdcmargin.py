import re, sys, json
from tokenize import String

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DoubleType, LongType, TimestampType, DecimalType
from pyspark.sql.functions import col, concat, substring, format_string, regexp_replace

from pnp.etl.datasets import dataset

"""
ACZ: s3://pnp-data-lake-dev-acz-retail-euw1/bw/td/hrsam10xx/
STZ: s3://pnp-data-lake-dev-stz-retail-euw1/td/sales/sales_bw_hrsam10/
"""
class pnpdcmarginData(dataset):

    def __init__(self, configParams):
        super().__init__(configParams)

        self.hudi_options = {}

        # Join all the hudi_options for this dataset
        self.hudi_options.update(dataset.hudi_options)

        # Specific hudi_options per dataset
        self.local_hudi_options = {
            'hoodie.datasource.write.recordkey.field': 'HSAL_CHA,HBILL_TYP,HPLANT,HMATERIAL,SALES_UNIT,CALDAY',
            'hoodie.datasource.write.partitionpath.field': 'CALDAY',
            'hoodie.datasource.hive_sync.partition_fields': 'CALDAY',
            'hoodie.cleaner.policy': 'KEEP_LATEST_FILE_VERSIONS',
            'hoodie.cleaner.commits.retained': 1,
            'hoodie.keep.min.commits': 2,
            'hoodie.keep.max.commits': 5,
            'hoodie.datasource.write.operation': 'insert_overwrite'
        }

        # Join all the hudi_options for this dataset
        self.hudi_options.update(self.local_hudi_options)
        
        # For absolutely brand new datasets, the datasetSchemaEvo should be set to True always
        # because it's used in testing for a variety of things in the codebase.
        self.datasetSchemaEvo = True

        # There can be only one ... schema, which is increasing in columns
        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 25
        self.schemaDict[1]['validFrom'] = '20220921' # This is set to an arbitratry date in the past
        self.schemaDict[1]['validUntil'] = '20240801' # This is ostensibly set to whatever date this version of schema applies until
        
        self.Schema = StructType([
            StructField("DI_REQTSN", StringType(),False),
            StructField("DI_SEQ_NR", IntegerType(),False),
            StructField("HSAL_CHA", StringType(),False),
            StructField("HBILL_TYP", StringType(),False),
            StructField("HPLANT", StringType(),False),
            StructField("HMATERIAL", StringType(),False),
            StructField("SALES_UNIT", StringType(),False),
            StructField("CALDAY", LongType(),False),
            StructField("HCO_AREA", StringType(),True),
            StructField("HPROFCTR", StringType(),True),
            StructField("HEANUPC", StringType(),True),
            StructField("HEQDTPRY", LongType(),True),
            StructField("BASE_UOM", StringType(),True),
            StructField("PO_UNIT", StringType(),True),
            StructField("LOC_CURRCY", StringType(),True),
            StructField("HSQTYBU", DecimalType(17,3),True),
            StructField("HSQTYSU", DecimalType(17,3),True),
            StructField("HSQTYOU", DecimalType(17,3),True),
            StructField("HSQTYLBU", DecimalType(17,3),True),
            StructField("HSQTYLSU", DecimalType(17,3),True),
            StructField("HSQTYLOU", DecimalType(17,3),True),
            StructField("HSVNEVLC", DecimalType(17,2),True),
            StructField("HSVNEVLLC", DecimalType(17,2),True),
            StructField("HCOSLC", DecimalType(17,2),True),
            StructField("HCOSLLC", DecimalType(17,2),True)])