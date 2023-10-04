"""
ACZ: s3://pnp-data-lake-prod-acz-retail-euw1/bw/td/hrsow01
STZ: s3://pnp-data-lake-prod-stz-retail-euw1/td/sales/sales_bw_hrsow01
"""

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, LongType, DecimalType
from pnp.etl.datasets import dataset

class salesordersData(dataset):

    def __init__(self, configParams):
        super().__init__(configParams)

        self.hudi_options = {}

        # Join all the hudi_options for this dataset
        self.hudi_options.update(dataset.hudi_options)

        # Specific hudi_options per dataset
        self.local_hudi_options = {
          'hoodie.datasource.write.recordkey.field': 'HSALDOC,HSALDOCIT',
          'hoodie.datasource.write.partitionpath.field': 'CALDAY',
          'hoodie.datasource.hive_sync.partition_fields': 'CALDAY',
          'hoodie.upsert.shuffle.parallelism': '10000',
          'hoodie.memory.merge.fraction': '0.80'
        }

        # Join all the hudi_options for this dataset
        self.hudi_options.update(self.local_hudi_options)

        self.datasetSchemaEvo = True

        self.Schema = StructType([
            StructField("DI_REQTSN", StringType(),False),
            StructField("DI_SEQ_NR", IntegerType(),False),
            StructField("HSALDOC", StringType(),False),
            StructField("HSALDOCIT", StringType(),False),
            StructField("CALDAY", LongType(),False),
            StructField("HSALHLITM", StringType(),True),
            StructField("HSAL_CHA", StringType(),True),
            StructField("HFIREFDOC", StringType(),True),
            StructField("HSAL_CAT", StringType(),True),
            StructField("HSAL_ICAT", StringType(),True),
            StructField("HSAL_TYP", StringType(),True),
            StructField("HSHIPCON", StringType(),True),
            StructField("HSALSCLAS", StringType(),True),
            StructField("HPLANT", StringType(),True),
            StructField("HSHIPPNT", StringType(),True),
            StructField("HMATERIAL", StringType(),True),
            StructField("HEANUPC", StringType(),True),
            StructField("HBAGART", StringType(),True),
            StructField("HBILLTOPY", StringType(),True),
            StructField("HPAYER", StringType(),True),
            StructField("HSHIPTOPY", StringType(),True),
            StructField("HSOLDTOPY", StringType(),True),
            StructField("HCRMSHCRD", StringType(),True),
            StructField("HPADCCRD", StringType(),True),
            StructField("HPAMMCRD", StringType(),True),
            StructField("HCUSTPONR", StringType(),True),
            StructField("HCACCNUM", StringType(),True),
            StructField("HPHONE", StringType(),True),
            StructField("HCREATDAT", LongType(),True),
            StructField("HCHANGDAT", LongType(),True),
            StructField("HTRANSDAT", LongType(),True),
            StructField("HDELSLDAT", LongType(),True),
            StructField("HPCKSTDAT", LongType(),True),
            StructField("HCREDIND", StringType(),True),
            StructField("HDELIND", StringType(),True),
            StructField("HDELSTTIM", LongType(),True),
            StructField("HDELENTIM", LongType(),True),
            StructField("HPICKTIM", LongType(),True),
            StructField("HSALREJST", StringType(),True),
            StructField("SALES_UNIT", StringType(),True),
            StructField("BASE_UOM", StringType(),True),
            StructField("PO_UNIT", StringType(),True),
            StructField("UNIT_OF_WT", StringType(),True),
            StructField("VOLUMEUNIT", StringType(),True),
            StructField("DOC_CURRCY", StringType(),True),
            StructField("LOC_CURRCY", StringType(),True),
            StructField("GRP_CURRCY", StringType(),True),
            StructField("HSOQTYSU", DecimalType(17,3),True),
            StructField("HSOQTYBU", DecimalType(17,3),True),
            StructField("HSOQTYOU", DecimalType(17,3),True),
            StructField("HSOVEVDC", DecimalType(17,2),True),
            StructField("HSOVEVLC", DecimalType(17,2),True),
            StructField("HSOVEVGC", DecimalType(17,2),True),
            StructField("HSOVATDC", DecimalType(17,2),True),
            StructField("HSOVATLC", DecimalType(17,2),True),
            StructField("HSOVATGC", DecimalType(17,2),True),
            StructField("HNSALEITM", DecimalType(17,3),True),
            StructField("HSONTPRLC", DecimalType(17,2),True),
            StructField("HGROSS_WT", DecimalType(17,3),True),
            StructField("HNET_WT", DecimalType(17,3),True),
            StructField("HVOLUME", DecimalType(17,3),True),
            StructField("HEXCHGRAT", DecimalType(32,16),True)])

        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 62
        self.schemaDict[1]['validFrom'] = '19000101'
        self.schemaDict[1]['validUntil'] = '29991231'