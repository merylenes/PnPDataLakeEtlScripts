"""
ACZ: s3://pnp-data-lake-prod-acz-retail-euw1/bw/td/hrsam04
STZ: s3://pnp-data-lake-prod-stz-retail-euw1/td/sales/sales_bw_hrsam04
"""

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, LongType, DecimalType
from pnp.etl.datasets import dataset

class salesonlineshoppingdatamartData(dataset):

    def __init__(self, configParams):
        super().__init__(configParams)

        self.hudi_options = {}

        # Join all the hudi_options for this dataset
        self.hudi_options.update(dataset.hudi_options)

        # Specific hudi_options per dataset
        self.local_hudi_options = {
          'hoodie.datasource.write.recordkey.field': 'HBILDOC,HBILDOCIT',
          'hoodie.datasource.write.partitionpath.field': 'CALDAY',
          'hoodie.datasource.hive_sync.partition_fields': 'CALDAY',
          'hoodie.upsert.shuffle.parallelism': '20000',
          'hoodie.memory.merge.fraction': '0.80'
        }

        # Join all the hudi_options for this dataset
        self.hudi_options.update(self.local_hudi_options)

        self.datasetSchemaEvo = True

        self.Schema = StructType([
            StructField("DI_REQTSN", StringType(),False),
            StructField("DI_SEQ_NR", IntegerType(),False),
            StructField("HBILDOC", StringType(),False),
            StructField("HBILDOCIT", StringType(),False),
            StructField("CALDAY", LongType(),False),
            StructField("HSALDOC", StringType(),True),
            StructField("HSALDOCIT", StringType(),True),
            StructField("HSALHLITM", StringType(),True),
            StructField("HSAL_CHA", StringType(),True),
            StructField("HBILL_TYP", StringType(),True),
            StructField("HSAL_TYP", StringType(),True),
            StructField("HSAL_CAT", StringType(),True),
            StructField("HSAL_ICAT", StringType(),True),
            StructField("HSHIPCON", StringType(),True),
            StructField("HOS_PKSUB", StringType(),True),
            StructField("HSALSCLAS", StringType(),True),
            StructField("HPLANT", StringType(),True),
            StructField("HSHIPPNT", StringType(),True),
            StructField("HCO_AREA", StringType(),True),
            StructField("HPROFCTR", StringType(),True),
            StructField("HMATERIAL", StringType(),True),
            StructField("HEANUPC", StringType(),True),
            StructField("HBILLTOPY", StringType(),True),
            StructField("HSHIPTOPY", StringType(),True),
            StructField("HPAYER", StringType(),True),
            StructField("HSOLDTOPY", StringType(),True),
            StructField("HCREDIND", StringType(),True),
            StructField("HOS_NCIND", StringType(),True),
            StructField("HOS_CACC", StringType(),True),
            StructField("VOLUMEUNIT", StringType(),True),
            StructField("UNIT_OF_WT", StringType(),True),
            StructField("BASE_UOM", StringType(),True),
            StructField("SALES_UNIT", StringType(),True),
            StructField("DOC_CURRCY", StringType(),True),
            StructField("LOC_CURRCY", StringType(),True),
            StructField("GRP_CURRCY", StringType(),True),
            StructField("HSQTYBU", DecimalType(17,3),True),
            StructField("HSQTYSU", DecimalType(17,3),True),
            StructField("HSVNEVDC", DecimalType(17,2),True),
            StructField("HSVNEVLC", DecimalType(17,2),True),
            StructField("HSVNEVGC", DecimalType(17,2),True),
            StructField("HVATDC", DecimalType(17,2),True),
            StructField("HVATLC", DecimalType(17,2),True),
            StructField("HVATGC", DecimalType(17,2),True),
            StructField("HCOSDC", DecimalType(17,2),True),
            StructField("HCOSLC", DecimalType(17,2),True),
            StructField("HCOSGC", DecimalType(17,2),True),
            StructField("HDISC02DC", DecimalType(17,2),True),
            StructField("HDISC02GC", DecimalType(17,2),True),
            StructField("HDISC02LC", DecimalType(17,2),True),
            StructField("HDISC11DC", DecimalType(17,2),True),
            StructField("HDISC11GC", DecimalType(17,2),True),
            StructField("HDISC11LC", DecimalType(17,2),True),
            StructField("HNBILLITM", DecimalType(17,3),True),
            StructField("HSOQTYBU", DecimalType(17,3),True),
            StructField("HSOQTYSU", DecimalType(17,3),True),
            StructField("HSOVEVDC", DecimalType(17,2),True),
            StructField("HSOVEVLC", DecimalType(17,2),True),
            StructField("HSOVEVGC", DecimalType(17,2),True),
            StructField("HSOVATDC", DecimalType(17,2),True),
            StructField("HSOVATLC", DecimalType(17,2),True),
            StructField("HSOVATGC", DecimalType(17,2),True),
            StructField("HNSALEITM", DecimalType(17,3),True),
            StructField("HSONQTYBU", DecimalType(17,3),True),
            StructField("HSONQTYSU", DecimalType(17,3),True),
            StructField("HSOVNDC", DecimalType(17,2),True),
            StructField("HSOVNLC", DecimalType(17,2),True),
            StructField("HSOVNGC", DecimalType(17,2),True),
            StructField("HSOOQTYBU", DecimalType(17,3),True),
            StructField("HSOOQTYSU", DecimalType(17,3),True),
            StructField("HSOOVALDC", DecimalType(17,2),True),
            StructField("HSOOVALLC", DecimalType(17,2),True),
            StructField("HSOOVALGC", DecimalType(17,2),True),
            StructField("HGROSS_WT", DecimalType(17,3),True),
            StructField("HNET_WT", DecimalType(17,3),True),
            StructField("HVOLUME", DecimalType(17,3),True)])

        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 76
        self.schemaDict[1]['validFrom'] = '19000101'
        self.schemaDict[1]['validUntil'] = '29991231'
        