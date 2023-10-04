from stat import ST_UID
import re, sys, json

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DoubleType, LongType, TimestampType, DecimalType, BooleanType
from pyspark.sql.functions import col, concat, substring, format_string, regexp_replace, lit, when, isnull

from pnp.etl.datasets import dataset

# ACZ: s3://pnp-data-lake-prod-acz-retail-euw1/bw/td/hrsam01
# STZ: s3://pnp-data-lake-prod-stz-retail-euw1/td/sales/sales_bw_hrsam01
class salesactualdatamartData(dataset):

    def __init__(self, configParams):
        super().__init__(configParams)

        self.hudi_options = {}

        # Merge all the hudi_options from the superclass datasets.py into this dataset
        self.hudi_options.update(dataset.hudi_options)

        self.datasetSchemaEvo = True

        self.Schema = StructType([
            StructField("DI_REQTSN", StringType(),False),
            StructField("DI_SEQ_NR", IntegerType(),False),
            StructField("HSAL_CHA", StringType(),False),
            StructField("HBILL_TYP", StringType(),False),
            StructField("HPLANT", StringType(),False),
            StructField("HMATERIAL", StringType(),False),
            StructField("SALES_UNIT", StringType(),False),
            StructField("CALDAY", LongType(),False),
            StructField("HPROM_YN", StringType(),False),
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
            StructField("HSVGIVLC", DecimalType(17,2),True),
            StructField("HSVGIVLLC", DecimalType(17,2),True),
            StructField("HSVNEVLC", DecimalType(17,2),True),
            StructField("HSVNIVLC", DecimalType(17,2),True),
            StructField("HSVNEVLLC", DecimalType(17,2),True),
            StructField("HSVNIVLLC", DecimalType(17,2),True),
            StructField("HSVSGIVLC", DecimalType(17,2),True),
            StructField("HSVPGIVLC", DecimalType(17,2),True),
            StructField("HSVSGILLC", DecimalType(17,2),True),
            StructField("HSVPGILLC", DecimalType(17,2),True),
            StructField("HVATLC", DecimalType(17,2),True),
            StructField("HVATLLC", DecimalType(17,2),True),
            StructField("HCOSLC", DecimalType(17,2),True),
            StructField("HCOSLLC", DecimalType(17,2),True),
            StructField("HDISC01LC", DecimalType(17,2),True),
            StructField("HDISC02LC", DecimalType(17,2),True),
            StructField("HDISC03LC", DecimalType(17,2),True),
            StructField("HDISC04LC", DecimalType(17,2),True),
            StructField("HDISC05LC", DecimalType(17,2),True),
            StructField("HDISC06LC", DecimalType(17,2),True),
            StructField("HDISC07LC", DecimalType(17,2),True),
            StructField("HDISC08LC", DecimalType(17,2),True),
            StructField("HDISC09LC", DecimalType(17,2),True),
            StructField("HDISC10LC", DecimalType(17,2),True),
            StructField("HDISC11LC", DecimalType(17,2),True),
            StructField("HDISC13LC", DecimalType(17,2),True),
            StructField("HDISC14LC", DecimalType(17,2),True),
            StructField("HDISC15LC", DecimalType(17,2),True),
            StructField("HDISC16LC", DecimalType(17,2),True),
            StructField("HDISC17LC", DecimalType(17,2),True),
            StructField("HDISCXRLC", DecimalType(17,2),True),
            StructField("HDISCRNLC", DecimalType(17,2),True),
            StructField("HDIS01LLC", DecimalType(17,2),True),
            StructField("HDIS02LLC", DecimalType(17,2),True),
            StructField("HDIS03LLC", DecimalType(17,2),True),
            StructField("HDIS04LLC", DecimalType(17,2),True),
            StructField("HDIS05LLC", DecimalType(17,2),True),
            StructField("HDIS06LLC", DecimalType(17,2),True),
            StructField("HDIS07LLC", DecimalType(17,2),True),
            StructField("HDIS08LLC", DecimalType(17,2),True),
            StructField("HDIS09LLC", DecimalType(17,2),True),
            StructField("HDIS10LLC", DecimalType(17,2),True),
            StructField("HDIS11LLC", DecimalType(17,2),True),
            StructField("HDIS13LLC", DecimalType(17,2),True),
            StructField("HDIS14LLC", DecimalType(17,2),True),
            StructField("HDIS15LLC", DecimalType(17,2),True),
            StructField("HDIS16LLC", DecimalType(17,2),True),
            StructField("HDIS17LLC", DecimalType(17,2),True),
            StructField("HDISXRLLC", DecimalType(17,2),True),
            StructField("HDISRNLLC", DecimalType(17,2),True),
            StructField("HPYEQFLG", IntegerType(),False),
            StructField("HDISC18LC", DecimalType(17,2),True),
            StructField("HDISC19LC", DecimalType(17,2),True),
            StructField("HDISC20LC", DecimalType(17,2),True),
            StructField("HDISC21LC", DecimalType(17,2),True),
            StructField("HDISC22LC", DecimalType(17,2),True),
            StructField("HDISC23LC", DecimalType(17,2),True),
            StructField("HDISC24LC", DecimalType(17,2),True),
            StructField("HDISC25LC", DecimalType(17,2),True),
            StructField("HDIS18LLC", DecimalType(17,2),True),
            StructField("HDIS19LLC", DecimalType(17,2),True),
            StructField("HDIS20LLC", DecimalType(17,2),True),
            StructField("HDIS21LLC", DecimalType(17,2),True),
            StructField("HDIS22LLC", DecimalType(17,2),True),
            StructField("HDIS23LLC", DecimalType(17,2),True),
            StructField("HDIS24LLC", DecimalType(17,2),True),
            StructField("HDIS25LLC", DecimalType(17,2),True)])

        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 72
        self.schemaDict[1]['validFrom'] = '20160301'
        self.schemaDict[1]['validUntil'] = '20320228'
        self.schemaDict[1]['hudi_options'] = {
          'hoodie.datasource.write.recordkey.field': 'HSAL_CHA,HBILL_TYP,HPLANT,HMATERIAL,SALES_UNIT,CALDAY,HPROM_YN,HPYEQFLG',
          'hoodie.datasource.write.partitionpath.field': 'CALDAY',
          'hoodie.datasource.hive_sync.partition_fields': 'CALDAY',
          'hoodie.cleaner.policy': 'KEEP_LATEST_FILE_VERSIONS',
          'hoodie.cleaner.fileversions.retained': 1
        }

        # Add back when the schema is evolved to include the deleted column.
        self.schemaDict[2] = {'ver': 2}
        self.schemaDict[2]['cols'] = 89
        self.schemaDict[2]['validFrom'] = '20221121'
        self.schemaDict[2]['validUntil'] = '20320228'
        self.schemaDict[2]['hudi_options'] = {
           'hoodie.datasource.write.recordkey.field': 'HSAL_CHA,HBILL_TYP,HPLANT,HMATERIAL,SALES_UNIT,CALDAY,HPROM_YN,HPYEQFLG',
           'hoodie.datasource.write.partitionpath.field': 'CALDAY',
           'hoodie.datasource.hive_sync.partition_fields': 'CALDAY',
           'hoodie.cleaner.policy': 'KEEP_LATEST_FILE_VERSIONS',
           'hoodie.cleaner.fileversions.retained': 1
        }
       
    def updateHudiOptions(self):
      """
      Small method to update the Hudi options for the new composite key
      """
      try:
        # First try to see whether there are hudi_options for this schema version
        self.hudi_options.update(self.schemaDict[self.schemaVer]['hudi_options'])
      except KeyError as e:
        # No key for the schema version?
        # This precludes us from having to have hudi_options in every schema if we don't need them
        # Specific hudi_options per dataset
        dataset_hudi_options = {
          'hoodie.datasource.write.recordkey.field': 'HSAL_CHA,HBILL_TYP,HPLANT,HMATERIAL,SALES_UNIT,CALDAY,HPROM_YN,HPYEQFLG',
          'hoodie.datasource.write.partitionpath.field': 'CALDAY',
          'hoodie.datasource.hive_sync.partition_fields': 'CALDAY',
          'hoodie.cleaner.policy': 'KEEP_LATEST_FILE_VERSIONS',
          'hoodie.cleaner.fileversions.retained': 1
        }
        # Join all the hudi_options for this dataset
        self.hudi_options.update(dataset_hudi_options)

      
    def datasetSpecific(self, df):
      """
      Method to do something specific to this dataset

      The thinking is that if there's something specific for a dataset, one can
      create a method called datasetSpecific sending in the dataframe, and then manipulate 
      the dataset specific things like this method does, returning a dataframe
      
      Specifically, this method to alters the key in the hudi_options by adding the key to
      the schema, but also inserting a 0 into the key field"""

      # Add the column for the new key. This is subclass specific logic. Do not copy to all
      # datasets

      self.updateHudiOptions()
      #print(f"<-- Hudi options after datasetSpecific hudi options set\n{self.hudi_options} -->")

      #print(f"<-- {df.columns} -->")
      recordKeys = self.getKeys('hoodie.datasource.write.recordkey.field')

      if 'HPYEQFLG' not in df.columns:
        # If the column is not present in the data
        #print(f"<-- HPYEQFLG is not in the columns -->")
        df = df.withColumn('HPYEQFLG', lit(0))

      # If the column is present but it's empty or else the value in the key
      df = df.withColumn('HPYEQFLG', when(df.HPYEQFLG.isNull(), lit(0)).otherwise(col('HPYEQFLG')))
      df.limit(2).show()

      allColumns = df.columns

      # Remove the record keys from all the columns, because we want
      # to put them back in the order the new CSV will be coming into the dataset
      # Check whether DI_RQTSN and DI_SEQ_NR are required to be removed and put back

      # Remove all the keys and the 2 seq numbers 
      for ele in recordKeys + ['DI_REQTSN', 'DI_SEQ_NR']:
        try:
          allColumns.remove(ele)
        except ValueError as e:
          print(f"<-- {ele} not in allColumns. Skipping {ele} -->")

      newColOrder = ['DI_REQTSN', 'DI_SEQ_NR'] + recordKeys + allColumns
      print(f"<-- Order of columns before select\n{newColOrder} -->")

      newOrderDf = df.select(newColOrder)

      return newOrderDf