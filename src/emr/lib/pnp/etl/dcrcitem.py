"""
ACZ: s3://pnp-data-lake-prod-acz-retail-euw1/erp/td/dcrc_it
STZ: s3://pnp-data-lake-prod-stz-retail-euw1/td/dcrc/erp_dcrc_it
"""

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, DecimalType
from pyspark.sql import functions as F
from pnp.etl.datasets import dataset

class dcrcitemData(dataset):

    def __init__(self, configParams,  partition_by=""):
        super().__init__(configParams)

        self.hudi_options = {}

        # Join all the hudi_options for this dataset
        self.hudi_options.update(dataset.hudi_options)

        # Specific hudi_options per dataset
        self.local_hudi_options = {
          'hoodie.datasource.write.recordkey.field': 'MANDT,DCRC_ID,DCRC_POSNR',
          'hoodie.datasource.write.partitionpath.field': f"{partition_by}",
          'hoodie.datasource.hive_sync.partition_fields': f"{partition_by}",
          'hoodie.upsert.shuffle.parallelism':'200',
        }

        # Join all the hudi_options for this dataset
        self.hudi_options.update(self.local_hudi_options)

        self.datasetSchemaEvo = True

        self.Schema = StructType([
            StructField("DI_REQTSN", StringType(),False),
            StructField("DI_SEQ_NR", IntegerType(),False),
            StructField("DI_EXTR_DATE", IntegerType(),False),
            StructField("MANDT", StringType(),False),
            StructField("DCRC_ID", StringType(),False),
            StructField("DCRC_POSNR", StringType(),False),
            StructField("VBTYP", StringType(),True),
            StructField("VGBEL", StringType(),True),
            StructField("VGPOS", StringType(),True),
            StructField("MATNR", StringType(),True),
            StructField("LFIMG", DecimalType(13,3),True),
            StructField("VRKME", StringType(),True),
            StructField("DCRC_CAUSE", StringType(),True),
            StructField("REFVBTYP", StringType(),True),
            StructField("REFVGBEL", StringType(),True),
            StructField("REFVGPOS", StringType(),True),
            StructField("IT_VALUE", DecimalType(11,2),True),
            StructField("IT_WAERS", StringType(),True),
            StructField("CHARG", StringType(),True)])

        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 19
        self.schemaDict[1]['validFrom'] = '19000101'
        self.schemaDict[1]['validUntil'] = '29991231'

    def partition_by_year(self, data_frame):

      """
        Add a new column on which we plan to partition based on the partitionCol
        passed to the method. This was added for DCRCItem where there was no
        parition initially. Then a new column was added (DI_EXTR_DATE) and the
        requirement is to parition on the year on that column
        DI_EXTR_DATE in this case was 20211210 and the requirement is to
        partition on 2021
      """
      
      return data_frame \
              .withColumn('DI_EXTR_YYYY', F.regexp_extract(data_frame.DI_EXTR_DATE, '([0-9]{4})[0-9]{4}', 1))