import re, sys, json

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DoubleType, LongType, TimestampType, DecimalType
from pyspark.sql.functions import col, concat, substring, format_string, regexp_replace

from pnp.etl.datasets import dataset

# ACZ: s3://pnp-data-lake-prod-acz-retail-euw1/bw/td/hrsaw01
# STZ: s3://pnp-data-lake-prod-stz-retail-euw1/td/sales/sales_bw_hrsaw01
class salesData(dataset):

    def __init__(self, configParams):
        super().__init__(configParams)

        self.hudi_options = {}

        # Join all the hudi_options for this dataset
        self.hudi_options.update(dataset.hudi_options)

        # Specific hudi_options per dataset
        self.local_hudi_options = {
          'hoodie.datasource.write.recordkey.field': 'HBILDOC,HBILDOCIT',
          'hoodie.datasource.hive_sync.partition_fields': 'CALDAY',
          'hoodie.datasource.write.partitionpath.field': 'CALDAY'
        }

        # Join all the hudi_options for this dataset
        self.hudi_options.update(self.local_hudi_options)

        self.datasetSchemaEvo = True

        # There can be only one ... schema, which is increasing in columns
        self.Schema = StructType([
            StructField("DI_REQTSN", StringType(), False), 
            StructField("DI_SEQ_NR", LongType(), False), 
            StructField("HBILDOC", LongType(), False), 
            StructField("HBILDOCIT", StringType(), False), 
            StructField("CALDAY", LongType(), False), 
            StructField("HSALDOC", StringType(), True), 
            StructField("HSALDOCIT", StringType(), True),
            StructField("HREFDOC", StringType(), True), 
            StructField("HREFDOCIT", StringType(), True),
            StructField("HRTCBATCH", StringType(), True), 
            StructField("HXREFDOC", StringType(), True), 
            StructField("HSAL_CHA", StringType(), True), 
            StructField("HBILL_TYP", StringType(), True), 
            StructField("HBILL_CAT", StringType(), True), 
            StructField("HSAL_CAT", StringType(), True), 
            StructField("HSAL_ICAT", StringType(), True), 
            StructField("HSHIPCON", StringType(), True), 
            StructField("HPLANT", StringType(), True), 
            StructField("HCO_AREA", StringType(), True), 
            StructField("HPROFCTR", StringType(), True), 
            StructField("HMATERIAL", StringType(), True), 
            StructField("HEANUPC", StringType(), True), 
            StructField("HBILLTOPY", StringType(), True), 
            StructField("HSHIPTOPY", StringType(), True), 
            StructField("HPAYER", StringType(), True), 
            StructField("HSOLDTOPY", StringType(), True), 
            StructField("HCREATDAT", LongType(), True), 
            StructField("HCHANGDAT", LongType(), True), 
            StructField("HTRANSDAT", LongType(), True),
            StructField("HCREDIND", StringType(), True), 
            StructField("HDELIND", StringType(), True), 
            StructField("HPROM_YN", StringType(), True), 
            StructField("HRTC_YN", StringType(), True), 
            StructField("HRT_PROMO", StringType(), True),
            StructField("BASE_UOM", StringType(), True),
            StructField("SALES_UNIT", StringType(), True),
            StructField("PO_UNIT", StringType(), True),
            StructField("UNIT_OF_WT", StringType(), True),
            StructField("VOLUMEUNIT", StringType(), True),
            StructField("DOC_CURRCY", StringType(), True),
            StructField("LOC_CURRCY", StringType(), True),
            StructField("GRP_CURRCY", StringType(), True),
            StructField("HSQTYBU", DecimalType(17,3), True),
            StructField("HSQTYSU", DecimalType(17,3), True),
            StructField("HSQTYOU", DecimalType(17,3), True),
            StructField("HSVGIVDC", DecimalType(17,2), True),
            StructField("HSVGIVLC", DecimalType(17,2), True),
            StructField("HSVGIVGC", DecimalType(17,2), True),
            StructField("HSVNEVDC", DecimalType(17,2), True),
            StructField("HSVNEVLC", DecimalType(17,2), True),
            StructField("HSVNEVGC", DecimalType(17,2), True),
            StructField("HSVNIVDC", DecimalType(17,2), True),
            StructField("HSVNIVLC", DecimalType(17,2), True),
            StructField("HSVNIVGC", DecimalType(17,2), True),
            StructField("HSVSGIVDC", DecimalType(17,2), True),
            StructField("HSVSGIVLC", DecimalType(17,2), True),
            StructField("HSVSGIVGC", DecimalType(17,2), True),
            StructField("HSVPGIVDC", DecimalType(17,2), True),
            StructField("HSVPGIVLC", DecimalType(17,2), True),
            StructField("HSVPGIVGC", DecimalType(17,2), True),
            StructField("HVATDC", DecimalType(17,2), True),
            StructField("HVATLC", DecimalType(17,2), True),
            StructField("HVATGC", DecimalType(17,2), True),
            StructField("HCOSDC", DecimalType(17,2), True),
            StructField("HCOSLC", DecimalType(17,2), True),
            StructField("HCOSGC", DecimalType(17,2), True),
            StructField("HDISC01DC", DecimalType(17,2), True),
            StructField("HDISC01LC", DecimalType(17,2), True),
            StructField("HDISC01GC", DecimalType(17,2), True),
            StructField("HDISC02DC", DecimalType(17,2), True),
            StructField("HDISC02LC", DecimalType(17,2), True),
            StructField("HDISC02GC", DecimalType(17,2), True),
            StructField("HDISC03DC", DecimalType(17,2), True),
            StructField("HDISC03LC", DecimalType(17,2), True),
            StructField("HDISC03GC", DecimalType(17,2), True),
            StructField("HDISC04DC", DecimalType(17,2), True),
            StructField("HDISC04LC", DecimalType(17,2), True),
            StructField("HDISC04GC", DecimalType(17,2), True),
            StructField("HDISC05DC", DecimalType(17,2), True),
            StructField("HDISC05LC", DecimalType(17,2), True),
            StructField("HDISC05GC", DecimalType(17,2), True),
            StructField("HDISC06DC", DecimalType(17,2), True),
            StructField("HDISC06LC", DecimalType(17,2), True),
            StructField("HDISC06GC", DecimalType(17,2), True),
            StructField("HDISC07DC", DecimalType(17,2), True),
            StructField("HDISC07LC", DecimalType(17,2), True),
            StructField("HDISC07GC", DecimalType(17,2), True),
            StructField("HDISC08DC", DecimalType(17,2), True),
            StructField("HDISC08LC", DecimalType(17,2), True),
            StructField("HDISC08GC", DecimalType(17,2), True),
            StructField("HDISC09DC", DecimalType(17,2), True),
            StructField("HDISC09LC", DecimalType(17,2), True),
            StructField("HDISC09GC", DecimalType(17,2), True),
            StructField("HDISC10DC", DecimalType(17,2), True),
            StructField("HDISC10LC", DecimalType(17,2), True),
            StructField("HDISC10GC", DecimalType(17,2), True),
            StructField("HDISC11DC", DecimalType(17,2), True),
            StructField("HDISC11LC", DecimalType(17,2), True),
            StructField("HDISC11GC", DecimalType(17,2), True),
            StructField("HDISC13DC", DecimalType(17,2), True),
            StructField("HDISC13LC", DecimalType(17,2), True),
            StructField("HDISC13GC", DecimalType(17,2), True),
            StructField("HDISC14DC", DecimalType(17,2), True),
            StructField("HDISC14LC", DecimalType(17,2), True),
            StructField("HDISC14GC", DecimalType(17,2), True),
            StructField("HDISC15DC", DecimalType(17,2), True),
            StructField("HDISC15LC", DecimalType(17,2), True),
            StructField("HDISC15GC", DecimalType(17,2), True),
            StructField("HDISCXRDC", DecimalType(17,2), True),
            StructField("HDISCXRLC", DecimalType(17,2), True),
            StructField("HDISCXRGC", DecimalType(17,2), True),
            StructField("HDISCRNDC", DecimalType(17,2), True),
            StructField("HDISCRNLC", DecimalType(17,2), True),
            StructField("HDISCRNGC", DecimalType(17,2), True),
            StructField("HNBILLITM", DecimalType(17,3), True),
            StructField("HGROSS_WT", DecimalType(17,3), True),
            StructField("HNET_WT", DecimalType(17,3), True),
            StructField("HVOLUME", DecimalType(17,3), True),
            StructField("HEXCHGRAT", DecimalType(17,16), True),
            StructField("HDISC16DC", DecimalType(17,2), True),
            StructField("HDISC16LC", DecimalType(17,2), True),
            StructField("HDISC16GC", DecimalType(17,2), True),
            StructField("HDISC17DC", DecimalType(17,2), True),
            StructField("HDISC17LC", DecimalType(17,2), True),
            StructField("HDISC17GC", DecimalType(17,2), True),
            StructField("HDISC18DC", DecimalType(17,2), True),
            StructField("HDISC18GC", DecimalType(17,2), True),
            StructField("HDISC18LC", DecimalType(17,2), True),
            StructField("HDISC19DC", DecimalType(17,2), True),
            StructField("HDISC19GC", DecimalType(17,2), True),
            StructField("HDISC19LC", DecimalType(17,2), True),
            StructField("HDISC20DC", DecimalType(17,2), True),
            StructField("HDISC20GC", DecimalType(17,2), True),
            StructField("HDISC20LC", DecimalType(17,2), True),
            StructField("HDISC21DC", DecimalType(17,2), True),
            StructField("HDISC21GC", DecimalType(17,2), True),
            StructField("HDISC21LC", DecimalType(17,2), True),
            StructField("HDISC22DC", DecimalType(17,2), True),
            StructField("HDISC22GC", DecimalType(17,2), True),
            StructField("HDISC22LC", DecimalType(17,2), True),
            StructField("HDISC23DC", DecimalType(17,2), True),
            StructField("HDISC23GC", DecimalType(17,2), True),
            StructField("HDISC23LC", DecimalType(17,2), True),
            StructField("HDISC24DC", DecimalType(17,2), True),
            StructField("HDISC24GC", DecimalType(17,2), True),
            StructField("HDISC24LC", DecimalType(17,2), True),
            StructField("HDISC25DC", DecimalType(17,2), True),
            StructField("HDISC25GC", DecimalType(17,2), True),
            StructField("HDISC25LC", DecimalType(17,2), True)])

        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 119
        self.schemaDict[1]['validFrom'] = '20160301'
        self.schemaDict[1]['validUntil'] = '20220228'

        self.schemaDict[2] = {'ver': 2}
        self.schemaDict[2]['cols'] = 149
        self.schemaDict[2]['validFrom'] = '20220301'
        self.schemaDict[2]['validUntil'] = '20300228'