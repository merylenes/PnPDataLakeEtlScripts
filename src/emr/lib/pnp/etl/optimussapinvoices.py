"""
ACZ: s3://pnp-data-lake-dev-acz-ols-euw1/opt/td/sap_invoices/
STZ: s3://pnp-data-lake-dev-stz-ols-euw1/td/sap_invoices/sap_invoices_opt_sap_invoices/
"""

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, DecimalType, LongType, TimestampType
from pnp.etl.datasets import dataset

class optimussapinvoicesData(dataset):

    def __init__(self, configParams):
        super().__init__(configParams)

        self.hudi_options = {}

        # Join all the hudi_options for this dataset
        self.hudi_options.update(dataset.hudi_options)

        # Specific hudi_options per dataset
        self.local_hudi_options = {
        'hoodie.datasource.write.recordkey.field': 'ID,sap_order_number,store_code', 
        'hoodie.datasource.write.partitionpath.field': 'created_at_date',
        'hoodie.datasource.hive_sync.partition_fields': 'created_at_date',
        'hoodie.datasource.hive_sync.support_timestamp':'true'
        }

        # Join all the hudi_options for this dataset
        self.hudi_options.update(self.local_hudi_options)

        self.datasetSchemaEvo = True

        self.Schema = StructType([
            StructField("DI_REQTSN",StringType(),False),
            StructField("DI_SEQ_NR",IntegerType(),False),
            StructField("ID",IntegerType(),False),
            StructField("order_id",IntegerType(),True),
            StructField("sap_order_number",IntegerType(),False),
            StructField("billing_doc_number",IntegerType(),True),
            StructField("store_code",StringType(),False),
            StructField("total_including_vat",DecimalType(10,2),True),
            StructField("total_excluding_vat",DecimalType(8,2),True),
            StructField("status", StringType(),True),
            StructField("response_code", IntegerType(),True),
            StructField("response_msg", StringType(),True),
            StructField("created_at", TimestampType(),True),
            StructField("updated_at", TimestampType(),True),
            StructField("created_at_date", LongType(),True)
           
        ])
        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 15
        self.schemaDict[1]['validFrom'] = '20230928'
        self.schemaDict[1]['validUntil'] = '20230928'
