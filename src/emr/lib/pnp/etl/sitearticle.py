"""
ACZ: s3://pnp-data-lake-prod-acz-retail-euw1/bw/md/hrsmw01
STZ: s3://pnp-data-lake-prod-stz-retail-euw1/md/sitearticle/sitearticle_bw_sitearticle_01
"""

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, LongType, DecimalType, BooleanType
from pnp.etl.datasets import dataset

class sitearticleData(dataset):

    def __init__(self, configParams, partition_by=""):
        super().__init__(configParams)

        self.partition_by = partition_by

        # Specific hudi_options per dataset
        self.hudi_options = {
          "hoodie.datasource.write.recordkey.field": "HMATERIAL",
          "hoodie.datasource.write.partitionpath.field": f"{partition_by}",
          "hoodie.datasource.hive_sync.partition_fields": f"{partition_by}"
        }

        # Join all the hudi_options for this dataset
        self.hudi_options.update(dataset.hudi_options)

        self.datasetSchemaEvo = True

        self.Schema = StructType([
            StructField("DI_REQTSN", StringType(),False),
            StructField("DI_SEQ_NR", IntegerType(),False),
            StructField("HPLANT", StringType(),False),
            StructField("HMATERIAL", StringType(),False),
            StructField("HPLANTREF", StringType(),True),
            StructField("HCNTRYORI", StringType(),True),
            StructField("HREGORIG", StringType(),True),
            StructField("HSTOR_LEP", StringType(),True),
            StructField("HCO_AREA", StringType(),True),
            StructField("HPROFCTR", StringType(),True),
            StructField("HMATEREF", StringType(),True),
            StructField("HVALFRAST", LongType(),True),
            StructField("HVALTACFC", LongType(),True),
            StructField("HDATLLUG", LongType(),True),
            StructField("HBMRIND", StringType(),True),
            StructField("HPISIND", StringType(),True),
            StructField("HLISTSTAT", StringType(),True),
            StructField("HDELIND", StringType(),True),
            StructField("HAUTPOIND", StringType(),True),
            StructField("HNSTKAIND", StringType(),True),
            StructField("HRSFRMAU", StringType(),True),
            StructField("HMARDHEXI", StringType(),True),
            StructField("HMLUGIND", StringType(),True),
            StructField("HSITESTAT", StringType(),True),
            StructField("HASLSTS", StringType(),True),
            StructField("HMRPTYPE", StringType(),True),
            StructField("HMRPCONTR", StringType(),True),
            StructField("HPLANCYC", StringType(),True),
            StructField("HDELCYC", StringType(),True),
            StructField("HFOPERIND", StringType(),True),
            StructField("HLOTSIZE", StringType(),True),
            StructField("HSAFETIME", StringType(),True),
            StructField("HSFETMIND", StringType(),True),
            StructField("HCAPCLASS", StringType(),True),
            StructField("HABCIND", StringType(),True),
            StructField("HPO_GRP", StringType(),True),
            StructField("HMAINSTAT", StringType(),True),
            StructField("HPROCTYPE", StringType(),True),
            StructField("HREPCLASS", StringType(),True),
            StructField("HLOADGRP", StringType(),True),
            StructField("HSPLITIND", StringType(),True),
            StructField("HAVCHECK", StringType(),True),
            StructField("HCOMIMPCD", StringType(),True),
            StructField("HRNDPROF", StringType(),True),
            StructField("HSPROCIND", StringType(),True),
            StructField("HDISTPROF", StringType(),True),
            StructField("HORPAKSIZ", StringType(),True),
            StructField("HAVABC", StringType(),True),
            StructField("HSOUSUPP", StringType(),True),
            StructField("HSOUDC", StringType(),True),
            StructField("HSOUVENDR", StringType(),True),
            StructField("HSVNDSCEN", StringType(),True),
            StructField("HPMARC01", StringType(),True),
            StructField("HPMARC02", StringType(),True),
            StructField("HPMARC03", StringType(),True),
            StructField("HPMARC04", StringType(),True),
            StructField("HPMARC05", StringType(),True),
            StructField("HPMARC06", StringType(),True),
            StructField("HPMARC07", StringType(),True),
            StructField("HPMARC08", StringType(),True),
            StructField("HPMARC09", StringType(),True),
            StructField("HPMARC10", StringType(),True),
            StructField("HPMARC11", StringType(),True),
            StructField("HPMARC12", StringType(),True),
            StructField("HPMARC13", StringType(),True),
            StructField("HPEXITRSN", StringType(),True),
            StructField("HASSORTMT", StringType(),True),
            StructField("BASE_UOM", StringType(),True),
            StructField("PO_UNIT", StringType(),True),
            StructField("HISS_UNIT", StringType(),True),
            StructField("HTIM_UNIT", StringType(),True),
            StructField("HDATKLLUG", DecimalType(17,0),True),
            StructField("HPLDELTIM", DecimalType(17,3),True),
            StructField("HGRPRTIMD", DecimalType(17,3),True),
            StructField("HSAFSTKBU", DecimalType(17,3),True),
            StructField("HSAFSTMBU", DecimalType(17,3),True),
            StructField("HCSTLSIBU", DecimalType(17,3),True),
            StructField("HOPAKSIZ", DecimalType(17,3),True),
            StructField("HMAXSTRPE", DecimalType(17,3),True),
            StructField("HMRFARCON", DecimalType(17,3),True)])

        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 80
        self.schemaDict[1]['validFrom'] = '19000101'
        self.schemaDict[1]['validUntil'] = '20300228'
