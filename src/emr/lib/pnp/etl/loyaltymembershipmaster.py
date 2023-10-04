"""
ACZ: s3://pnp-data-lake-dev-acz-crm-euw1/bw/md/hcmsw01/
STZ: s3://pnp-data-lake-dev-stz-crm-euw1/md/member/member_bw_hcmsw01/
"""

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, DecimalType, LongType, TimestampType
from pnp.etl.datasets import dataset

class loyaltymembershipmasterData(dataset):

    def __init__(self, configParams):
        super().__init__(configParams)

        self.hudi_options = {}

        # Join all the hudi_options for this dataset
        self.hudi_options.update(dataset.hudi_options)

        # Specific hudi_options per dataset
        self.local_hudi_options = {
        'hoodie.datasource.write.recordkey.field': 'HCRMSHID',
        'hoodie.datasource.write.partitionpath.field': 'HUPD_DATE',
        'hoodie.datasource.hive_sync.partition_fields': 'HUPD_DATE'
        }

        # Join all the hudi_options for this dataset
        self.hudi_options.update(self.local_hudi_options)

        self.datasetSchemaEvo = True

        self.Schema = StructType([
            StructField("DI_REQTSN",StringType(),True, {"comment":"Data Services (DI) Request Transaction Number"}),
            StructField("DI_SEQ_NR",IntegerType(),False,{"comment":"Data Services (DI) Sequence Number"}),
            StructField("HCRMSHID",StringType(),False, {"comment":"Membership"}),
            StructField("HCRMSGUID",StringType(),True, {"comment":"Membership GUID"}),
            StructField("HCRMSHTP",StringType(),True, {"comment":"Membership Types"}),
            StructField("HCRMEMBER",StringType(),True, {"comment":"Member"}),
            StructField("HCRMSRCHL",StringType(),True, {"comment":"Membership Registration Channel"}),
            StructField("HCRMSRCHL_T",StringType(),True, {"comment":"Membership Registration Channel Text"}),
            StructField("HCRMSSSTA",StringType(),True, {"comment":"Membership System Status"}),
            StructField("HCRMSSSTA_T",StringType(),True, {"comment":"Membership System Status Text"}),
            StructField("HCRMSUSTA",StringType(),True, {"comment":"Membership User Status"}),
            StructField("HCRMSUSTA_T",StringType(),True, {"comment":"Membership User Status Text"}),
            StructField("HCRMSHNO",StringType(),True, {"comment":"Membership Number"}),
            StructField("HCRMSPOU",StringType(),True, {"comment":"Membership Purpose of Use"}),
            StructField("HCRMSPOU_T",StringType(),True, {"comment":"Membership Purpose of Use Text"}),
            StructField("HCRBTLVL",StringType(),True, {"comment":"Loyalty Basic Tier Level"}),
            StructField("HCRLBTLVL",StringType(),True, {"comment":"Loyalty Last Basic Tier Level"}),
            StructField("HCRLTTLVL",StringType(),True, {"comment":"Loyalty Life Time Tier Level"}),
            StructField("HCRLPID",StringType(),True, {"comment":"Loyalty Program ID"}),
            StructField("HCRLPID_T",StringType(),True, {"comment":"Loyalty Program ID Text"}),
            StructField("HCRMS12YN",StringType(),True, {"comment":"2012 Customer Membership"}),
            StructField("HCRMBSTDT",LongType(),True, {"comment":"Membership Start Date"}),
            StructField("HCRMBEDDT",LongType(),True, {"comment":"Membership End Date"}),
            StructField("HCRLSTXDT",LongType(),True, {"comment":"PnP Last Transaction Date"}),
            StructField("HCRSTCGDT",LongType(),True, {"comment":"Status Change Date"}),
            StructField("HCRLTXDTK",DecimalType(17,0),True,{"comment":"Last Transaction Date (KF)"}),
            StructField("HUPD_DATE",LongType(),True, {"comment":"Update Date"})
        ])
        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 27
        self.schemaDict[1]['validFrom'] = '19000101'
        self.schemaDict[1]['validUntil'] = '29991231'
