"""
ACZ: s3://pnp-data-lake-dev-acz-retail-euw1/cms/md/PlanogramInStore_Import
STZ: s3://pnp-data-lake-dev-acz-retail-euw1/md/PlanogramInStore_Import/planogram_cms_planograminstore_import
"""

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, LongType, DecimalType, TimestampType
from pnp.etl.datasets import dataset

# Please make sure the DATASET_NAME is lowecase - the whole thing. So NO PosCore. Rather poscore.


class rangeandspaceplanograminstoreimportData(dataset):

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
            'hoodie.datasource.write.recordkey.field': 'Pog_Store_TblKey,PlanogramKey,Performance_Key',
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
            StructField("Pog_Store_TblKey", IntegerType(), False),
            StructField("PlanogramKey", IntegerType(), False),
            StructField("SectionDBKey", IntegerType(), True),
            StructField("Section_X", DecimalType(32, 16), True),
            StructField("Section_Y", DecimalType(32, 16), True),
            StructField("Section_Z", DecimalType(32, 16), True),
            StructField("Section_Angle", DecimalType(32, 16), True),
            StructField("Section_Linear", DecimalType(32, 16), True),
            StructField("Section_Square", DecimalType(32, 16), True),
            StructField("Section_Cubic", DecimalType(32, 16), True),
            StructField("Section_Height", DecimalType(32, 16), True),
            StructField("Section_LocationID", IntegerType(), True),
            StructField("Section_SegmentStart", IntegerType(), True),
            StructField("Section_SegmentEnd", IntegerType(), True),
            StructField("SubPlanogramSpaceName", StringType(), True),
            StructField("Section_Width", DecimalType(16, 16), True),
            StructField("Section_LeftPlanogramName", StringType(), True),
            StructField("Section_LeftPlanogramCategory", StringType(), True),
            StructField("Section_RightPlanogramName", StringType(), True),
            StructField("Section_RightPlanogramCategory", StringType(), True),
            StructField("Section_LeftPlanogramKey", IntegerType(), True),
            StructField("Section_RightPlanogramKey", IntegerType(), True),
            StructField("Performance_Key", IntegerType(), False),
            StructField("NumberofSections", IntegerType(), True),
            StructField("Linear", DecimalType(32, 16), True),
            StructField("Square", DecimalType(32, 16), True),
            StructField("Cubic", DecimalType(32, 16), True),
            StructField("Linear_Prct", DecimalType(32, 16), True),
            StructField("SquarePrct", DecimalType(32, 16), True),
            StructField("ChangeType", StringType(), True),
            StructField("SubPlanogramsLinked", StringType(), True),
            StructField("Flr_Fixture_Key", IntegerType(), True),
            StructField("Flr_Fixture_Aisle_Space_Front", DecimalType(32, 16), True),
            StructField("Flr_Fixture_Aisle_Space_Back", DecimalType(32, 16), True),
            StructField("Flr_Fixture_Aisle_Space_Left", DecimalType(32, 16), True),
            StructField("Flr_Fixture_Aisle_Space_Right", DecimalType(32, 16), True),
            StructField("Flr_Fixture_Linear", DecimalType(32, 16), True),
            StructField("Flr_Fixture_Square", DecimalType(32, 16), True),
            StructField("Flr_Fixture_AvailableLinear", DecimalType(32, 16), True),
            StructField("Flr_Fixture_AvailableSquare", DecimalType(32, 16), True),
            StructField("Flr_Fixture_X", DecimalType(32, 16), True),
            StructField("Flr_Fixture_Y", DecimalType(32, 16), True),
            StructField("Flr_Fixture_Z", DecimalType(32, 16), True),
            StructField("Flr_Fixture_LocationID", IntegerType(), True),
            StructField("Flr_Fixture_Type_Descr", StringType(), True),
            StructField("FloorplanKey", IntegerType(), True),
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
        self.schemaDict[1]['cols'] = 50
        # This is set to an arbitratry date in the past
        self.schemaDict[1]['validFrom'] = '20230905'
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