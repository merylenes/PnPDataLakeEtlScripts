"""
ACZ: s3://pnp-data-lake-dev-acz-retail-euw1/cms/md/productinplanogram_import
STZ: s3://pnp-data-lake-dev-stz-retail-euw1/md/productinplanogram_import/planogram_cms_productinplanogram_import
"""

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, LongType, DecimalType, TimestampType
from pnp.etl.datasets import dataset

# Please make sure the DATASET_NAME is lowecase - the whole thing. So NO PosCore. Rather poscore.


class rangeandspaceproductinplanogramData(dataset):

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
            'hoodie.datasource.write.recordkey.field': 'Prod_Pog_TblKey',
            'hoodie.datasource.write.partitionpath.field': 'FileExport_Date',
            'hoodie.datasource.hive_sync.partition_fields': 'FileExport_Date',
            'hoodie.datasource.hive_sync.support_timestamp': 'true'
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
            StructField("Prod_Pog_TblKey", IntegerType(), False),
            StructField("ArticleID", StringType(), True),
            StructField("Spc_PositionKey", IntegerType(), True),
            StructField("Position_X", DecimalType(32,16), True),
            StructField("Position_Y", DecimalType(32,16), True),
            StructField("Position_Z", DecimalType(32,16), True),
            StructField("MerchandisingStyle", StringType(), True),
            StructField("Orientation", StringType(), True),
            StructField("Pos_Rank_X", DecimalType(32,16), True),
            StructField("Pos_Rank_Y", DecimalType(32,16), True),
            StructField("Pos_Rank_Z", DecimalType(32,16), True),
            StructField("Pos_HFacings", IntegerType(), True),
            StructField("Pos_VFacings", IntegerType(), True),
            StructField("Pos_DFacings", IntegerType(), True),
            StructField("Pos_Prev_HFacings", IntegerType(), True),
            StructField("Pos_Prev_VFacings", IntegerType(), True),
            StructField("Pos_Prev_MerchStyle", StringType(), True),
            StructField("Pos_Prev_Orientation", StringType(), True),
            StructField("Pos_Prev_RankX", DecimalType(32,16), True),
            StructField("Pos_Prev_RankY", DecimalType(32,16), True),
            StructField("Pos_Prev_RankZ", DecimalType(32,16), True),
            StructField("Pos_Location_ID", IntegerType(), True),
            StructField("Performance_Key", IntegerType(), True),
            StructField("NumberofPositions", IntegerType(), True),
            StructField("Capacity", IntegerType(), True),
            StructField("DaysSupply", IntegerType(), True),
            StructField("Linear", DecimalType(32,16), True),
            StructField("Linear_Prct", DecimalType(32,16), True),
            StructField("Linear_Prct_Used", DecimalType(32,16), True),
            StructField("Square", DecimalType(32,16), True),
            StructField("SquarePrct", DecimalType(32,16), True),
            StructField("SquarePrcntUsed", DecimalType(32,16), True),
            StructField("Cubic", DecimalType(32,16), True),
            StructField("CubicPrcnt", DecimalType(32,16), True),
            StructField("CubicPrcntUsed", DecimalType(32,16), True),
            StructField("Spc_Perf_Facings", IntegerType(), True),
            StructField("TargetInventory", DecimalType(32,16), True),
            StructField("CPI", DecimalType(32,16), True),
            StructField("Spc_Perf_Replen_Min", DecimalType(32,16), True),
            StructField("Spc_Perf_Replen_Max", DecimalType(32,16), True),
            StructField("Spc_Perf_Change_Type", StringType(), True),
            StructField("Spc_Perf_Display_Type", StringType(), True),
            StructField("NotonPogIndicator", IntegerType(), True),
            StructField("Spc_Fixture_Key", IntegerType(), True),
            StructField("Spc_Fixture_Type", StringType(), True),
            StructField("Spc_Fix_Location_ID", IntegerType(), True),
            StructField("Spc_Fix_Linear", DecimalType(32,16), True),
            StructField("Spc_Fix_Square", DecimalType(32,16), True),
            StructField("Spc_Fix_Avbl_Linear", DecimalType(32,16), True),
            StructField("Spc_Fix_Avbl_Square", DecimalType(32,16), True),
            StructField("Spc_Fix_Avbl_Cubic", DecimalType(32,16), True),
            StructField("Spc_Fix_SegmentNumber", IntegerType(), True),
            StructField("Spc_Fix_FirstSegment", IntegerType(), True),
            StructField("Spc_Fix_LastSegment", IntegerType(), True),
            StructField("Spc_Fix_DistanceFromBase", DecimalType(32,16), True),
            StructField("Spc_Fix_Angle", DecimalType(32,16), True),
            StructField("Spc_Fix_SpaceAbove", DecimalType(32,16), True),
            StructField("Spc_Fix_Merch_Height", DecimalType(32,16), True),
            StructField("Spc_Fix_Height", DecimalType(32,16), True),
            StructField("Spc_Fix_Width", DecimalType(32,16), True),
            StructField("Spc_Fix_Depth", DecimalType(32,16), True),
            StructField("Spc_Segment_Key", IntegerType(), True),
            StructField("Spc_Segment_Height", DecimalType(32,16), True),
            StructField("Spc_Segment_Width", DecimalType(32,16), True),
            StructField("Spc_Segment_Depth", DecimalType(32,16), True),
            StructField("Spc_Segment_Number", IntegerType(), True),
            StructField("PlanogramKey", IntegerType(), True),
            StructField("FileExport_Time", TimestampType(), True),
            StructField("FileExport_Date", LongType(), True)
        ])

        # NOTE:
        # The validFrom and validUntil items are currently not used in any way, but I thought
        # it could potentially be used in the future in case the version/column number could then be
        # switched out with a date instead of the number of columns.
        # It is also just a sanity check because in time, there will be corporate memory loss of when
        # a schema changed - and this should be captured somewhere.

        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 71
        # This is set to an arbitratry date in the past
        self.schemaDict[1]['validFrom'] = '20230505'
        # This is ostensibly set to whatever date this version of schema applies until
        self.schemaDict[1]['validUntil'] = '20240505'

        # Setting up this second schema version in case it's going to be needed
        # however for a new dataset (i.e. one never seen before) we do not need version 2
        # but it's here so that when some new version does come along, it can be uncommented.

        # In theory, because we count the columns in the input dataset, we use this to determine which
        # version of a schema we're using. So for example if there's version 3 or 4 in the future
        # provided those new schemas have additional columns added to the existing schema, we can just
        # add another entry to the schemaDict (e.g. schemaDict[3] or schemaDict[4] etc.)

        # self.schemaDict[2] = {'ver': 2}
        # self.schemaDict[2]['cols'] = XXXX_SET_TO_NEW_NUMBER_OF_COLUMNS_XXXX
        # self.schemaDict[2]['validFrom'] = '20220301' # This date should start from the first ingestion date of the new schema
        # self.schemaDict[2]['validUntil'] = '20300228' # This is ostensibly set to sometime in the distant future when this could become invalid
        # self.schemaDict[2]['schema'] = StructType([
        #    XXXX_DATASET_STRUCTURE_XXXX])
