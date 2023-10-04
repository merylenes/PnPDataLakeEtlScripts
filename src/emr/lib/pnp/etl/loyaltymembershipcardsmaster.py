"""
ACZ: s3://pnp-data-lake-prod-acz-crm-euw1/bw/md/hcmcw01/
STZ: s3://pnp-data-lake-prod-stz-crm-euw1/md/member/member_bw_hcmcw01/
"""

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, LongType
from pnp.etl.datasets import dataset

class loyaltymembershipcardsmasterData(dataset):

    def __init__(self, configParams):
        super().__init__(configParams)

        self.hudi_options = {}

        # Join all the hudi_options for this dataset
        self.hudi_options.update(dataset.hudi_options)

        # Specific hudi_options per dataset
        self.local_hudi_options = {
        'hoodie.datasource.write.recordkey.field': 'HCRMSHCRD,HVALIDFRM',
        'hoodie.datasource.write.partitionpath.field': 'HCREATDAT',
        'hoodie.datasource.hive_sync.partition_fields': 'HCREATDAT'
        }

        # Join all the hudi_options for this dataset
        self.hudi_options.update(self.local_hudi_options)

        self.datasetSchemaEvo = True

        self.Schema = StructType([
            StructField("DI_REQTSN",StringType(),False, {"comment":"Data Services (DI) Request Transaction Number"}),
            StructField("DI_SEQ_NR",IntegerType(),False,{"comment":"Data Services (DI) Sequence Number"}),
            StructField("HCRMSHCRD",StringType(),False, {"comment":"Membership Card Number"}),
            StructField("HVALIDFRM",LongType(),False, {"comment":"Valid From"}),
            StructField("HVALIDTO",LongType(),True, {"comment":"Valid To"}),
            StructField("HCRMCGUID",StringType(),True, {"comment":"Membership Card GUID"}),
            StructField("HCRMCRDID",StringType(),True, {"comment":"Membership Card ID"}),
            StructField("HCRMCRDTP",StringType(),True, {"comment":"Membership Card Type"}),
            StructField("HCRMCRDTP_T",StringType(),True, {"comment":"Membership Card Type Text"}),
            StructField("HCRMCVLRM",LongType(),True, {"comment":"Card Valid From"}),
            StructField("HCRMCVLTO",LongType(),True, {"comment":"Card Valid To"}),
            StructField("HCRMCRANK",StringType(),True, {"comment":"Membership Card Rank"}),
            StructField("HCRMCRANK_T",StringType(),True, {"comment":"Membership Card Rank Text"}),
            StructField("HCRMCBPSN",StringType(),True, {"comment":"BP Fuel Seq No."}),
            StructField("HCRMCBCAI",StringType(),True, {"comment":"Block Card Authorisation ID"}),
            StructField("HCRMCUSTA",StringType(),True, {"comment":"Membership Card User Status"}),
            StructField("HCRMCUST_T",StringType(),True, {"comment":"Membership Card User Status Text"}),
            StructField("HCRMCRDTL",StringType(),True, {"comment":"Card Title"}),
            StructField("HCRMEMBER",StringType(),True, {"comment":"Member"}),
            StructField("HCRMEMBER_T",StringType(),True, {"comment":"MemberText"}),
            StructField("HCRMSHID",StringType(),True, {"comment":"Membership"}),
            StructField("HCRMSGUID",StringType(),True, {"comment":"Membership GUID"}),
            StructField("HCRLPID",StringType(),True, {"comment":"Loyalty Program ID"}),
            StructField("HCREATDAT",LongType(),True, {"comment":"Creation Date"}),
            StructField("HCAPTUSER",StringType(),True, {"comment":"Created By User"}),
            StructField("HCRTED_TS",StringType(),True, {"comment":"Created Time Stamp (UTC)"}),
            StructField("HCHANGDAT",LongType(),True, {"comment":"Change Date"}),
            StructField("HCHANUSER",StringType(),True, {"comment":"Changed By User"}),
            StructField("HCHNGE_TS",StringType(),True, {"comment":"Changed Time Stamp (UTC)"})
        ])
        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 29
        self.schemaDict[1]['validFrom'] = '19000101'
        self.schemaDict[1]['validUntil'] = '29991231'
