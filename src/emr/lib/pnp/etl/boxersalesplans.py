"""
ACZ: s3://box-data-lake-prod-acz-retail-euw1/box/td/sales_plan/
STZ: s3://box-data-lake-prod-stz-retail-euw1/td/sales/sales_box_salesplan_01/
"""

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, LongType, DecimalType
from pnp.etl.datasets import dataset

class boxersalesplansData(dataset):

    def __init__(self, configParams):
        super().__init__(configParams)

        self.hudi_options = {}

        # Join all the hudi_options for this dataset
        self.hudi_options.update(dataset.hudi_options)

        # Specific hudi_options per dataset
        self.local_hudi_options = {
            'hoodie.datasource.write.recordkey.field': 'BRANCH,DEPARTMENT_CODE,BUDGET_DATE',
            'hoodie.datasource.write.partitionpath.field': 'BUDGET_DATE',
            'hoodie.datasource.hive_sync.partition_fields': 'BUDGET_DATE',
        }

        # Join all the hudi_options for this dataset
        self.hudi_options.update(self.local_hudi_options)

        self.datasetSchemaEvo = True

        self.Schema = StructType([
            StructField("DI_REQTSN", StringType(), True),
            StructField("DI_SEQ_NR", IntegerType(), True),
            StructField("BRANCH", StringType(), True),
            StructField("DEPARTMENT_CODE", StringType(), True),
            StructField("BUDGET_DATE", LongType(), True),
            StructField("REQTNS", StringType(), True),
            StructField("BUDGET_AMOUNT", DecimalType(17,2), True)])

        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 7
        self.schemaDict[1]['validFrom'] = '19000101'
        self.schemaDict[1]['validUntil'] = '29991231'
