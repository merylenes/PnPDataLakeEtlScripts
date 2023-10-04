import re, sys, json

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DoubleType, LongType, TimestampType, DecimalType
from pyspark.sql.functions import col, concat, substring, format_string, regexp_replace

from pnp.etl.datasets import dataset

# Please make sure the DATASET_NAME is lowecase - the whole thing. So NO PosCore. Rather poscore.
class warehousetasksconfirmedinlandData(dataset):

    def __init__(self, configParams):
        super().__init__(configParams)

        self.hudi_options = {}

        # Join all the hudi_options for this dataset
        self.hudi_options.update(dataset.hudi_options)
        
        # Specific hudi_options per dataset
        # NOTE: Take a look at SalesActualDataMart ETL class. There is an option to put the hudi specific
        # options in the dictionary. There are special cases of when to do that, and in the case of 
        # salesactualdatamart, it was becuase we wanted to add a key to the dataset. This could be an option
        # but it does need to add a key to the schema as follows:
        # self.schemaDict[N]['hudi_options'] = { ... }
        # and then you ALSO require a method to handle those as well as a datasetSpecific method which will
        # be called from allDatasets.py.

        self.local_hudi_options = {
          'hoodie.datasource.write.recordkey.field': 'MANDT,LGNUM,TANUM,TAPOS',
          'hoodie.datasource.write.partitionpath.field': 'CREATED_DATE',
          'hoodie.datasource.hive_sync.partition_fields': 'CREATED_DATE',
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
        self.Schema = StructType([
            StructField("DI_REQTSN", StringType(), False),
            StructField("DI_SEQ_NR", IntegerType(), False),
            StructField("MANDT", StringType(), False),
            StructField("LGNUM", StringType(), False),
            StructField("TANUM", StringType(), False),
            StructField("TAPOS", StringType(), False),
            StructField("FLGHUTO", StringType(), True),
            StructField("PROCTY", StringType(), True),
            StructField("TRART", StringType(), True),
            StructField("PRCES", StringType(), True),
            StructField("PROCS", StringType(), True),
            StructField("TOSTAT", StringType(), True),
            StructField("CREATED_BY", StringType(), True),            
            StructField("CREATED_AT", DecimalType(15,0), True),
            StructField("CREATED_DATE", StringType(), True),
            StructField("WTCODE", StringType(), True),
            StructField("PRSRC", StringType(), True),
            StructField("REASON", StringType(), True),
            StructField("PRIORITY", IntegerType(), True),
            StructField("GUID_STOCK", StringType(), True),
            StructField("IDX_STOCK", StringType(), True),
            StructField("MATID", StringType(), True),
            StructField("BATCHID", StringType(), True),
            StructField("CAT", StringType(), True),
            StructField("STOCK_DOCCAT", StringType(), True),
            StructField("STOCK_DOCNO", StringType(), True),
            StructField("STOCK_ITMNO", StringType(), True),
            StructField("DOCCAT", StringType(), True),
            StructField("STOCK_USAGE", StringType(), True),
            StructField("OWNER", StringType(), True),
            StructField("OWNER_ROLE", StringType(), True),
            StructField("ENTITLED", StringType(), True),
            StructField("ENTITLED_ROLE", StringType(), True),
            StructField("STOCK_CNT", StringType(), True),
            StructField("CHARG", StringType(), True),
            StructField("MEINS", StringType(), True),
            StructField("ALTME", StringType(), True),
            StructField("VSOLM", DecimalType(31,14), True),
            StructField("VSOLA", DecimalType(31,14), True),
            StructField("KQUAN", DecimalType(31,14), True),
            StructField("LETYP", StringType(), True),
            StructField("WEIGHT", DecimalType(15,3), True),
            StructField("UNIT_W", StringType(), True),
            StructField("VOLUM", DecimalType(15,3), True),
            StructField("UNIT_V", StringType(), True),
            StructField("CAPA", DecimalType(15,3), True),
            StructField("PLACE_INV_P", StringType(), True),
            StructField("LOWCHK_INV_P", StringType(), True),
            StructField("VFDAT", LongType(), True),
            StructField("WDATU", DecimalType(15,0), True),
            StructField("COO", StringType(), True),
            StructField("HAZMAT", StringType(), True),
            StructField("INSPTYP", StringType(), True),
            StructField("INSPID", StringType(), True),
            StructField("IDPLATE", StringType(), True),
            StructField("DSTGRP", StringType(), True),
            StructField("VLTYP", StringType(), True),
            StructField("VLBER", StringType(), True),
            StructField("VLPLA", StringType(), True),
            StructField("SLOGPOS", StringType(), True),
            StructField("SRSRC", StringType(), True),
            StructField("STU_NUM", StringType(), True),
            StructField("SLOC_TYPE", StringType(), True),
            StructField("SGUID_HU", StringType(), True),
            StructField("VLENR", StringType(), True),
            StructField("NLTYP", StringType(), True),
            StructField("NLBER", StringType(), True),
            StructField("NLPLA", StringType(), True),
            StructField("DLOGPOS", StringType(), True),
            StructField("DRSRC", StringType(), True),
            StructField("DTU_NUM", StringType(), True),
            StructField("DLOC_TYPE", StringType(), True),
            StructField("DGUID_HU", StringType(), True),
            StructField("NLENR", StringType(), True),
            StructField("DESTD", StringType(), True),
            StructField("KZSUB", StringType(), True),
            StructField("SOLPO", DecimalType(13,3), True),
            StructField("ZEIEI", StringType(), True),
            StructField("VAS", StringType(), True),
            StructField("BATCH_NOCHG", StringType(), True),
            StructField("RDOCCAT", StringType(), True),
            StructField("RDOCID", StringType(), True),
            StructField("RITMID", StringType(), True),
            StructField("WAVE", StringType(), True),
            StructField("WAVE_ITM", StringType(), True),
            StructField("L2SKA", StringType(), True),
            StructField("L2SKR", StringType(), True),
            StructField("QDOCCAT", StringType(), True),
            StructField("QDOCID", StringType(), True),
            StructField("QITMID", StringType(), True),
            StructField("QIDPLATE", StringType(), True),
            StructField("KIT_CREA", StringType(), True),
            StructField("GUID_TO", StringType(), True),
            StructField("DBIND", StringType(), True),
            StructField("MFS_CS", StringType(), True),
            StructField("PSA", StringType(), True),
            StructField("PROD_ORDER", StringType(), True),
            StructField("KANBAN", StringType(), True),
            StructField("SUOM", StringType(), True),
            StructField("AAREA", StringType(), True), 
            StructField("QUEUE", StringType(), True),
            StructField("WHO", StringType(), True),
            StructField("REPID", StringType(), True),
            StructField("TCODE", StringType(), True),
            StructField("ORDIM_DUMMY", StringType(), True),
            StructField("ZEUGN", StringType(), True),
            StructField("CONFIRMED_BY", StringType(), True),
            StructField("CONFIRMED_AT", DecimalType(15,0), True),
            StructField("PROCESSOR", StringType(), True),
            StructField("EXCCODE", StringType(), True),
            StructField("BUSCON", StringType(), True),
            StructField("EXEC_STEP", StringType(), True),
            StructField("STARTED_AT",DecimalType(15,0), True),
            StructField("NISTM", DecimalType(31,14), True),
            StructField("NISTA", DecimalType(31,14), True),
            StructField("PLACE_INV", StringType(), True),
            StructField("LOWCHK_INV", StringType(), True),
            StructField("HUENT", StringType(), True),
            StructField("ORIG_TO", StringType(), True),
            StructField("SLGNUM_VIEW", StringType(), True),
            StructField("DLGNUM_VIEW", StringType(), True),
            StructField("DMENG", DecimalType(31,14), True),
            StructField("DMENA", DecimalType(31,14), True),
            StructField("REFPOS", StringType(), True),
            StructField("DOCCAT_EXT", StringType(), True),
            StructField("DOCNO_EXT", StringType(), True),
            StructField("ITMNO_EXT", StringType(), True),
            StructField("LOGSYS_EXT", StringType(), True),
            StructField("CWQUAN", DecimalType(31,14), True),
            StructField("CWUNIT", StringType(), True),
            StructField("CWEXACT", StringType(), True),
            StructField("CWQUAN_DIFF", DecimalType(31,14), True),
            StructField("CWQEXACT_DIFF", StringType(), True),
            StructField("COMBINATION_GRP", StringType(), True)])
        # NOTE:
        # The validFrom and validUntil items are currently not used in any way, but I thought
        # it could potentially be used in the future in case the version/column number could then be
        # switched out with a date instead of the number of columns.
        # It is also just a sanity check because in time, there will be corporate memory loss of when
        # a schema changed - and this should be captured somewhere.

        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 134
        self.schemaDict[1]['validFrom'] = '20220801' # This is set to an arbitratry date in the past
        self.schemaDict[1]['validUntil'] = '202308801' # This is ostensibly set to whatever date this version of schema applies until

        # Setting up this second schema version in case it's going to be needed
        # however for a new dataset (i.e. one never seen before) we do not need version 2
        # but it's here so that when some new version does come along, it can be uncommented.

        # In theory, because we count the columns in the input dataset, we use this to determine which
        # version of a schema we're using. So for example if there's version 3 or 4 in the future
        # provided those new schemas have additional columns added to the existing schema, we can just 
        # add another entry to the schemaDict (e.g. schemaDict[3] or schemaDict[4] etc.) 
        
        #self.schemaDict[2] = {'ver': 2}
        #self.schemaDict[2]['cols'] = XXXX_SET_TO_NEW_NUMBER_OF_COLUMNS_XXXX
        #self.schemaDict[2]['validFrom'] = '20220301' # This date should start from the first ingestion date of the new schema
        #self.schemaDict[2]['validUntil'] = '20300228' # This is ostensibly set to sometime in the distant future when this could become invalid
        #self.schemaDict[2]['schema'] = StructType([
        #    XXXX_DATASET_STRUCTURE_XXXX])           