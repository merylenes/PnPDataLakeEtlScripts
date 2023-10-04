import re, sys, json

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DoubleType, LongType, TimestampType, DecimalType
from pyspark.sql.functions import col, concat, substring, format_string, regexp_replace

from pnp.etl.datasets import dataset

# Please make sure the DATASET_NAME is lowecase - the whole thing. So NO PosCore. Rather poscore.
class ekkopurchasingdocumentheaderData(dataset):

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
          'hoodie.datasource.write.recordkey.field': 'EBELN',
          'hoodie.datasource.write.partitionpath.field': 'BEDAT',
          'hoodie.datasource.hive_sync.partition_fields': 'BEDAT'
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
            StructField("EBELN", StringType(), False),
            StructField("BUKRS", StringType(), True),
            StructField("BSTYP", StringType(), True),
            StructField("BSART", StringType(), True),
            StructField("BSAKZ", StringType(), True),
            StructField("LOEKZ", StringType(), True),
            StructField("STATU", StringType(), True),
            StructField("AEDAT", LongType(), True),
            StructField("ERNAM", StringType(), True),
            StructField("PINCR", StringType(), True),
            StructField("LPONR", StringType(), True),
            StructField("LIFNR", StringType(), True),
            StructField("SPRAS", StringType(), True),
            StructField("ZTERM", StringType(), True),
            StructField("ZBD1T", DecimalType(3,0), True),
            StructField("ZBD2T", DecimalType(3,0), True),
            StructField("ZBD3T", DecimalType(3,0), True),
            StructField("ZBD1P", DecimalType(5,3), True),
            StructField("ZBD2P", DecimalType(5,3), True),
            StructField("EKORG", StringType(), True),
            StructField("EKGRP", StringType(), True),
            StructField("WAERS", StringType(), True),
            StructField("WKURS", DecimalType(9,5), True),
            StructField("KUFIX", StringType(), True),
            StructField("BEDAT", LongType(), True),
            StructField("KDATB", LongType(), True),
            StructField("KDATE", LongType(), True),
            StructField("BWBDT", LongType(), True),
            StructField("ANGDT", LongType(), True),
            StructField("BNDDT", LongType(), True),
            StructField("GWLDT", LongType(), True),
            StructField("AUSNR", StringType(), True),
            StructField("ANGNR", StringType(), True),
            StructField("IHRAN", LongType(), True),
            StructField("IHREZ", StringType(), True),
            StructField("VERKF", StringType(), True),
            StructField("TELF1", StringType(), True),
            StructField("LLIEF", StringType(), True),
            StructField("KUNNR", StringType(), True),
            StructField("KONNR", StringType(), True),
            StructField("ABGRU", StringType(), True),
            StructField("AUTLF", StringType(), True),
            StructField("WEAKT", StringType(), True),
            StructField("RESWK", StringType(), True),
            StructField("LBLIF", StringType(), True),
            StructField("INCO1", StringType(), True),
            StructField("INCO2", StringType(), True),
            StructField("KTWRT", DecimalType(15,2), True),
            StructField("SUBMI", StringType(), True),
            StructField("KNUMV", StringType(), True),
            StructField("KALSM", StringType(), True),
            StructField("STAFO", StringType(), True),
            StructField("LIFRE", StringType(), True),
            StructField("EXNUM", StringType(), True),
            StructField("UNSEZ", StringType(), True),
            StructField("LOGSY", StringType(), True),
            StructField("UPINC", StringType(), True),
            StructField("STAKO", StringType(), True),
            StructField("FRGGR", StringType(), True),
            StructField("FRGSX", StringType(), True),
            StructField("FRGKE", StringType(), True),
            StructField("FRGZU", StringType(), True),
            StructField("FRGRL", StringType(), True),
            StructField("LANDS", StringType(), True),
            StructField("LPHIS", StringType(), True),
            StructField("ADRNR", StringType(), True),
            StructField("STCEG_L", StringType(), True),
            StructField("STCEG", StringType(), True),
            StructField("ABSGR", StringType(), True),
            StructField("ADDNR", StringType(), True),
            StructField("KORNR", StringType(), True),
            StructField("MEMORY", StringType(), True),
            StructField("PROCSTAT", StringType(), True),
            StructField("RLWRT", DecimalType(15,2), True),
            StructField("REVNO", StringType(), True),
            StructField("SCMPROC", StringType(), True),
            StructField("REASON_CODE", StringType(), True),
            StructField("MEMORYTYPE", StringType(), True),
            StructField("RETTP", StringType(), True),
            StructField("RETPC", DecimalType(5,2), True),
            StructField("DPTYP", StringType(), True),
            StructField("DPPCT", DecimalType(5,2), True),
            StructField("DPAMT", DecimalType(11,2), True),
            StructField("DPDAT", LongType(), True),
            StructField("MSR_ID", StringType(), True),
            StructField("HIERARCHY_EXISTS", StringType(), True),
            StructField("THRESHOLD_EXISTS", StringType(), True),
            StructField("LEGAL_CONTRACT", StringType(), True),
            StructField("DESCRIPTION", StringType(), True),
            StructField("RELEASE_DATE", LongType(), True),
            StructField("EXTERNALSYSTEM", StringType(), True),
            StructField("EXTERNALREFERENCEID", StringType(), True),
            StructField("EXT_REV_TMSTMP", DecimalType(21,7), True),
            StructField("FORCE_ID", StringType(), True),
            StructField("FORCE_CNT", StringType(), True),
            StructField("RELOC_ID", StringType(), True),
            StructField("RELOC_SEQ_ID", StringType(), True),
            StructField("SOURCE_LOGSYS", StringType(), True),
            StructField("PNP_UNQSHIP", StringType(), True),
            StructField("PNP_ESHIP_DATE", LongType(), True),
            StructField("PNP_LSHIP_DATE", LongType(), True),
            StructField("PNP_POL", StringType(), True),
            StructField("PNP_CORIGIN", StringType(), True),
            StructField("PNP_CTYPE", StringType(), True),
            StructField("PNP_QUANCSIZE_1", StringType(), True),            
            StructField("PNP_CSIZE_1", StringType(), True),
            StructField("PNP_CTEMP_1", StringType(), True),
            StructField("PNP_QUANCSIZE_2", StringType(), True),
            StructField("PNP_CSIZE_2", StringType(), True),
            StructField("PNP_CTEMP_2", StringType(), True),
            StructField("PNP_QUANCSIZE_3", StringType(), True),
            StructField("PNP_CSIZE_3", StringType(), True),
            StructField("PNP_CTEMP_3", StringType(), True),
            StructField("PNP_PMETHOD", StringType(), True),
            StructField("PNP_OLDPO", StringType(), True),
            StructField("PNP_EVENT", StringType(), True),
            StructField("PNP_FAGENT", StringType(), True),
            StructField("PNP_CARRIER", StringType(), True),
            StructField("PNP_GDGRP", StringType(), True),
            StructField("VZSKZ", StringType(), True),
            StructField("POHF_TYPE", StringType(), True),
            StructField("EQ_EINDT", LongType(), True),
            StructField("EQ_WERKS", StringType(), True),
            StructField("FIXPO", StringType(), True),
            StructField("EKGRP_ALLOW", StringType(), True),
            StructField("WERKS_ALLOW", StringType(), True),
            StructField("CONTRACT_ALLOW", StringType(), True),
            StructField("PSTYP_ALLOW", StringType(), True),
            StructField("FIXPO_ALLOW", StringType(), True),
            StructField("KEY_ID_ALLOW", StringType(), True),
            StructField("AUREL_ALLOW", StringType(), True),
            StructField("DELPER_ALLOW", StringType(), True),
            StructField("EINDT_ALLOW", StringType(), True),
            StructField("LTSNR_ALLOW", StringType(), True),
            StructField("OTB_LEVEL", StringType(), True),
            StructField("OTB_COND_TYPE", StringType(), True),
            StructField("KEY_ID", StringType(), True),
            StructField("OTB_VALUE", DecimalType(17,2), True),
            StructField("OTB_CURR", StringType(), True),
            StructField("OTB_RES_VALUE", DecimalType(17,2), True),
            StructField("OTB_SPEC_VALUE", DecimalType(17,2), True),
            StructField("SPR_RSN_PROFILE", StringType(), True),
            StructField("BUDG_TYPE", StringType(), True),
            StructField("OTB_STATUS", StringType(), True),
            StructField("OTB_REASON", StringType(), True),
            StructField("CHECK_TYPE", StringType(), True),
            StructField("CON_OTB_REQ", StringType(), True),
            StructField("CON_PREBOOK_LEV", StringType(), True),
            StructField("CON_DISTR_LEV", StringType(), True)])
        # NOTE:
        # The validFrom and validUntil items are currently not used in any way, but I thought
        # it could potentially be used in the future in case the version/column number could then be
        # switched out with a date instead of the number of columns.
        # It is also just a sanity check because in time, there will be corporate memory loss of when
        # a schema changed - and this should be captured somewhere.

        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 151
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