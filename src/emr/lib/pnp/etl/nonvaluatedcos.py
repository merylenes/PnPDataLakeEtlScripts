"""
ACZ: s3://pnp-data-lake-prod-acz-finance-euw1/bw/td/hfglm02
STZ: s3://pnp-data-lake-prod-stz-finance-euw1/td/cos/cos_bw_hfglm02
"""

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, LongType, DecimalType
from pyspark.sql import functions as F
from pnp.etl.datasets import dataset

class nonvaluatedcosData(dataset):

    def __init__(self, configParams):
        super().__init__(configParams)

        self.hudi_options = {}

        # Join all the hudi_options for this dataset
        self.hudi_options.update(dataset.hudi_options)

        self.datasetSchemaEvo = True

        self.Schema = StructType([
            StructField("DI_REQTSN", StringType(),False),
            StructField("DI_SEQ_NR", IntegerType(),False),
            StructField("HPLANT", StringType(),False),
            StructField("HMATERIAL", StringType(),False),
            StructField("CALDAY", LongType(),False),
            StructField("SALES_UNIT", StringType(),False),
            StructField("HOPENPER", StringType(),True),
            StructField("HSTORNO", StringType(),True),
            StructField("HCO_AREA", StringType(),True),
            StructField("HPROFCTR", StringType(),True),
            StructField("LOC_CURRCY", StringType(),True),
            StructField("HFCNVLC", DecimalType(17,2),True),
            StructField("HFCNVLLC", DecimalType(17,2),True),
            StructField("HFCNVOLC", DecimalType(17,2),True),
            StructField("HFCNVORLC", DecimalType(17,2),True)])

        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 15
        self.schemaDict[1]['validFrom'] = '20160301'
        self.schemaDict[1]['validUntil'] = '20230229'
        self.schemaDict[1]['hudi_options'] = {
          'hoodie.datasource.write.recordkey.field': 'HPLANT,HMATERIAL,CALDAY,SALES_UNIT,HOPENPER,HSTORNO,HPYEQFLG',
          'hoodie.datasource.write.partitionpath.field': 'CALDAY',
          'hoodie.datasource.hive_sync.partition_fields': 'CALDAY',
          'hoodie.cleaner.policy': 'KEEP_LATEST_FILE_VERSIONS',
          'hoodie.cleaner.commits.retained': 1,
          'hoodie.keep.min.commits': 2,
          'hoodie.keep.max.commits': 5,
          'hoodie.datasource.write.operation': 'insert_overwrite'
        }


        self.schemaDict[2] = {'ver': 2}
        self.schemaDict[2]['cols'] = 15
        self.schemaDict[2]['validFrom'] = '20230229'
        self.schemaDict[2]['validUntil'] = '99991231'
        self.schemaDict[2]['hudi_options'] = {
          'hoodie.datasource.write.recordkey.field': 'HPLANT,HMATERIAL,CALDAY,SALES_UNIT,HOPENPER,HSTORNO,HPYEQFLG',
          'hoodie.datasource.write.partitionpath.field': 'CALDAY',
          'hoodie.datasource.hive_sync.partition_fields': 'CALDAY',
          'hoodie.cleaner.policy': 'KEEP_LATEST_FILE_VERSIONS',
          'hoodie.cleaner.commits.retained': 1,
          'hoodie.keep.min.commits': 2,
          'hoodie.keep.max.commits': 5,
          'hoodie.datasource.write.operation': 'insert_overwrite'
        }

    def updateHudiOptions(self):
      """
      Small method to update the Hudi options for the new composite key
      """
      try:
        # First try to see whether there are hudi_options for this schema version
        self.hudi_options.update(self.schemaDict[self.schemaVer]['hudi_options'])
      except KeyError as exception:
        
        print(f"<-- {exception} :No key for the schema version.-->")
        dataset_hudi_options = {
          'hoodie.datasource.write.recordkey.field': 'HPLANT,HMATERIAL,CALDAY,SALES_UNIT,HOPENPER,HSTORNO,HPYEQFLG',
          'hoodie.datasource.write.partitionpath.field': 'CALDAY',
          'hoodie.datasource.hive_sync.partition_fields': 'CALDAY',
          'hoodie.cleaner.policy': 'KEEP_LATEST_FILE_VERSIONS',
          'hoodie.cleaner.commits.retained': 1,
          'hoodie.keep.min.commits': 2,
          'hoodie.keep.max.commits': 5,
          'hoodie.datasource.write.operation': 'insert_overwrite'
        }
        # Join all the hudi_options for this dataset
        self.hudi_options.update(dataset_hudi_options)

    def datasetSpecific(self, data_frame):
      """
      Method to do something specific to this dataset

      The thinking is that if there's something specific for a dataset, one can
      create a method called datasetSpecific sending in the dataframe, and then manipulate
      the dataset specific things like this method does, returning a dataframe
      Specifically, this method to alters the key in the hudi_options by adding the key to
      the schema, but also inserting a 0 into the key field
      """

      self.updateHudiOptions()

      record_keys = self.getKeys('hoodie.datasource.write.recordkey.field')

      #debugging purposes: this will return 2 rows
      data_frame.limit(2).show()

      if 'HPYEQFLG' not in data_frame.columns:
        # If the column is not present in the data
        #print(f"<-- HPYEQFLG is not in the columns -->")
        data_frame = data_frame.withColumn('HPYEQFLG', F.lit(0))
      
      else:
        # If the column is present but it's empty or else the value in the key
        data_frame = data_frame.withColumn('HPYEQFLG', F.when(data_frame.HPYEQFLG.isNull(), F.lit(0)).otherwise(F.col('HPYEQFLG')))
        data_frame.limit(2).show()

      all_columns = data_frame.columns

      # Remove the record keys from all the columns, because we want
      # to put them back in the order the new CSV will be coming into the dataset
      # Check whether DI_RQTSN and DI_SEQ_NR are required to be removed and put back

      # Remove all the keys and the 2 seq numbers
      for ele in record_keys + ['DI_REQTSN', 'DI_SEQ_NR']:
        try:
          all_columns.remove(ele)
        except ValueError as exception:
          print(f"<-- {exception,ele} not in allColumns. Skipping {ele} -->")

      new_col_order = ['DI_REQTSN', 'DI_SEQ_NR'] + record_keys + all_columns
      print(f"<-- Order of columns before select\n{new_col_order} -->")

      new_col_order = data_frame.select(new_col_order)

      return new_col_order
