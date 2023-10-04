#import boto3
import re, sys, json, socket

#from .logging import Log4j
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DoubleType, LongType, TimestampType, DecimalType
from pyspark.sql.functions import col, concat, substring, format_string, date_format


class dataset(object):

    # Std set of hudi options for almost all datasets
    hudi_options = {
        'hoodie.datasource.write.keygenerator.class': 'org.apache.hudi.keygen.ComplexKeyGenerator',
        'hoodie.datasource.write.operation': 'upsert',
        'hoodie.datasource.write.precombine.field': 'precombine_key',
        'hoodie.datasource.hive_sync.enable': 'true',
        'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.MultiPartKeysValueExtractor',
        'hoodie.parquet.compression.codec': 'snappy',
        'hoodie.upsert.shuffle.parallelism':'200',
        'hoodie.datasource.hive_sync.mode': 'hms',
        'hoodie.memory.merge.fraction': '0.80',
        'hoodie.payload.ordering.field': 'precombine_key',
        'hoodie.datasource.write.payload.class': 'org.apache.hudi.common.model.DefaultHoodieRecordPayload'
    }

    def __init__(self,configParams):

        self.schemaDict = {}
        self.datasetSchemaEvo = False
        # The schemaDict contains the following information
        #   self.schemaDict[1]['cols']   -- the number of columns in the schema. This is the primary thing we use to determine what version the schema is
        #   self.schemaDict[1]['ver']    -- the version if we need it (though this is in the index too)
        #   self.schemaDict[1]['validFrom'] -- we may be required in the future to determine the schema based on the financial calenday date
        #   self.schemaDict[1]['validTo'] -- we may be required in the future to determine the schema based on the financial calenday date
        #   self.schemaDict[1]['schema'] -- the actual schema for this version

        self.databaseName = configParams['database']
        self.tableName = configParams['tableName']

        # This is the basePath as required by Hudi
        self.basePath = configParams['destSTZ']

        self.destSTZ = configParams['destSTZ']

        self.sourceURI = configParams['sourceURI']

        self.schemaVer = int(configParams['schemaVer'])

        self.partition_by = ""

        # Update the hudi_options for each dataset
        self.hudi_options = dataset.hudi_options.update({
          'hoodie.table.name': f"{self.tableName}",
          'hoodie.datasource.write.table.name': f"{self.tableName}",
          'hoodie.datasource.hive_sync.database': f"{self.databaseName}",
          'hoodie.datasource.hive_sync.table': f"{self.tableName}",
          'hoodie.index.type': 'SIMPLE'
        })

        self.datasetStats = {"filesRead": {"count": None, "desc": "Total number of input files read"},
                            "total":{"rows": None, "desc": "Total rows for the current dataset read in"},
                            "dupPrimayKeysTotal":{"rows": None, "desc": "Total rows that contain \"duplicate\" primary keys. These rows are the ones Hudi will have to choose between"},
                            "nonNullPK":{"rows": None, "desc": "Total rows that are have all non-null Primary Keys"},
                            "nullPK": {"rows": None, "desc": "Number of rows that have null Primary Keys"},
                            "nullPartK": {"rows": None, "desc": "Number of rows that have null Partition Keys"},
                            "rowsRemaining": {"rows": None, "desc": "Total number of rows remaining after all null keys and null part keys have been removed"}}

        #sparkLogger = Log4j(super().spark)

    def setBasePath(self, path):
        self.destSTZ = path
        self.basePath = path

    def setUpsertParallelism(self, parallelism):
        """
        Change the hudi options for upsert parallelism
        This number is based on the size of the input data
        Take as input the dataset instance & the parallelism
        and modify the upsert parallelism
        """
        self.hudi_options['hoodie.upsert.shuffle.parallelism'] = parallelism

    def getKeys(self, hudi_field):

        keyList = []

        try:
            k = self.schemaDict[self.schemaVer]['hudi_options'][hudi_field]
        except KeyError as e:
            k = self.hudi_options[hudi_field]

        noSpaceKeys = k.replace(" ","")
        for key in noSpaceKeys.split(","):
            keyList.append(str(key))

        return keyList

    def reorderColumns(self, dataFrame):
        """
        Reorder the columns in a dataframe, putting the meta_data columns at the front
        """
        
        columns = dataFrame.columns

        # Remove our old metadata columns because we want to add them back at the beginning of the dataframe.
        for ele in ['precombine_key', 'fileName', 'date_ingested']:
            columns.remove(ele)

        # Prepend the precombine_key, fileName and date_ingested to the front of the columns
        newColumnOrder = ['precombine_key', 'fileName', 'date_ingested'] + columns
        print(f"<-- Columns are: {newColumnOrder} -->")

        return dataFrame.select(newColumnOrder)

    def add_precombine_key(self, dataFrame):
        # There is a small but significant hitch. If you add any "custom" columns to the end of the 
        # dataframe, you end up never being able to mutate the schema.

        # Hudi has pretty strict rules on how schemas can mutate. Since we're using 0.9.0 here they are:
        # https://hudi.apache.org/docs/schema_evolution#schema-evolution-pre-0110

        # As a result, and since I absolutely require precombine_key, I need to
        # add this to the beginning of the dataframe. Turns out the client wants to keep the other meta-data
        # fields.

        # So I do some fancy footwork to make that happen.

        # This is the last thing we do before we finally write the data to S3, so in this method:
        # - we reorder the metadata columns brining them up to right after the hudi columns
        # - drop rows where DI_SEQ_NR, DI_RQTSN (and hence) precombined key are all nulls - we can't have nulls
        #   in a hudi precombine key

        # Keep the columns of the original dataframe
        columns = dataFrame.columns

        if self.datasetSchemaEvo:

            # Remove our old metadata columns because we want to add them back at the beginning of the dataframe.
            for ele in ['fileName', 'date_ingested']:
                columns.remove(ele)

            # Prepend the precombine_key, fileName and date_ingested to the front of the columns
            newColumnOrder = ['precombine_key', 'fileName', 'date_ingested'] + columns
            print(f"<-- Columns are: {newColumnOrder} -->")

            # Add the columns with the precombine_key 
            df = dataFrame \
                    .withColumn('precombine_key', concat(substring(col("DI_REQTSN"),0,14), format_string('%010d', col("DI_SEQ_NR")))) \
                    .dropna(subset=(['DI_SEQ_NR','DI_REQTSN',"precombine_key"]))

            # Return the dataframe with the order of the columns maintained and the precombine_key at the head of the columns
            returnDf = df.select(newColumnOrder)
        else:
            # Add the columns with the precombine_key 
            df = dataFrame \
                    .withColumn('precombine_key', concat(substring(col("DI_REQTSN"),0,14), format_string('%010d', col("DI_SEQ_NR")))) \
                    .dropna(subset=(['DI_SEQ_NR','DI_REQTSN',"precombine_key"]))
            returnDf = df
            
        return returnDf

    def convert(self, string):
        st = string.replace(" ", "")
        return list(st.split(","))

    def removeNullKeys(self, dataFrame, datasetKeys):
        return dataFrame \
                .dropna(how='all', subset = datasetKeys)

    def dataStats(self, dataFrame, datasetKeys):
        """
            takes a datasetKeys string of primary keys
            and returns 3 things:
            (1) the total number of records unmolested (just what was read out of the file)
            (2) the number of keys dropped because there were blank primary key
            (3) the dataframe - now molested by having the null primary keys removed
        """

        totalRowCount = dataFrame \
                            .count()
                    
        nonNulltotalRowCount = dataFrame \
                                .dropna(how='all', subset = datasetKeys) \
                                .count()

        difference = totalRowCount - nonNulltotalRowCount

        return (totalRowCount, nonNulltotalRowCount, difference)


    def getSchema(self, ver):
        """Get the schema for the version of the dataset from the dataset class"""
        return self.schemaDict[ver]['schema']

    def getColumns(self, ver):
        """Get the number of columns in this particular version of the schema from the dataset class"""
        return self.schemaDict[ver]['cols']

    def getSchemaVer(self, readSchemaCols, dataFrame):
        """This function will try to return the schema version if the columns for the schema match"""

        schemaVer = 0
        numCols = len(dataFrame.columns)
        # The schema version will never start at 0, so bump up the range from 1 to length of the schemaDict + 1

        for sv in range(1, (len(self.schemaDict) + 1)):
            if self.schemaDict[sv]['cols'] == readSchemaCols:
                # We're found a schema which has the name number of columns as we're looking for
                schemaVer = sv
                break
        
        return schemaVer

