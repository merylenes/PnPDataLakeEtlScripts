"""
ACZ: s3://pnp-data-lake-prod-acz-retail-euw1/erp/td/eban/
STZ: s3://pnp-data-lake-prod-stz-retail-euw1/td/eban/eban_erp_eban/
"""

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, LongType, DecimalType
from pyspark.sql import functions as F
from pnp.etl.datasets import dataset

class ebanData(dataset):

    def __init__(self, configParams):
        super().__init__(configParams)

        # Specific hudi_options per dataset
        self.hudi_options = {
          'hoodie.datasource.write.recordkey.field': 'MANDT,BANFN,BNFPO',
          'hoodie.datasource.write.partitionpath.field': 'BADAT',
          'hoodie.datasource.hive_sync.partition_fields': 'BADAT'
        }

        # Join all the hudi_options for this dataset
        self.hudi_options.update(dataset.hudi_options)

        self.datasetSchemaEvo = True

        self.Schema = StructType([
            StructField("DI_REQTSN", StringType(),False),
            StructField("DI_SEQ_NR", IntegerType(),False),
            StructField("MANDT", StringType(),False),
            StructField("BANFN", StringType(),False),
            StructField("BNFPO", StringType(),False),
            StructField("BSART", StringType(),True),
            StructField("BSTYP", StringType(),True),
            StructField("BSAKZ", StringType(),True),
            StructField("LOEKZ", StringType(),True),
            StructField("STATU", StringType(),True),
            StructField("ESTKZ", StringType(),True),
            StructField("FRGKZ", StringType(),True),
            StructField("FRGZU", StringType(),True),
            StructField("FRGST", StringType(),True),
            StructField("EKGRP", StringType(),True),
            StructField("ERNAM", StringType(),True),
            StructField("ERDAT", StringType(),True),
            StructField("AFNAM", StringType(),True),
            StructField("TXZ01", StringType(),True),
            StructField("MATNR", StringType(),True),
            StructField("EMATN", StringType(),True),
            StructField("WERKS", StringType(),True),
            StructField("LGORT", StringType(),True),
            StructField("BEDNR", StringType(),True),
            StructField("MATKL", StringType(),True),
            StructField("RESWK", StringType(),True),
            StructField("MENGE", DecimalType(13,3),True),
            StructField("MEINS", StringType(),True),
            StructField("BUMNG", DecimalType(13,0),True),
            StructField("BADAT", LongType(),True),
            StructField("LPEIN", StringType(),True),
            StructField("LFDAT", StringType(),True),
            StructField("FRGDT", StringType(),True),
            StructField("WEBAZ", DecimalType(3,0),True),
            StructField("PREIS", DecimalType(11,2),True),
            StructField("PEINH", DecimalType(5,0),True),
            StructField("PSTYP", StringType(),True),
            StructField("KNTTP", StringType(),True),
            StructField("KZVBR", StringType(),True),
            StructField("KFLAG", StringType(),True),
            StructField("VRTKZ", StringType(),True),
            StructField("TWRKZ", StringType(),True),
            StructField("WEPOS", StringType(),True),
            StructField("WEUNB", StringType(),True),
            StructField("REPOS", StringType(),True),
            StructField("LIFNR", StringType(),True),
            StructField("FLIEF", StringType(),True),
            StructField("EKORG", StringType(),True),
            StructField("VRTYP", StringType(),True),
            StructField("KONNR", StringType(),True),
            StructField("KTPNR", StringType(),True),
            StructField("INFNR", StringType(),True),
            StructField("ZUGBA", StringType(),True),
            StructField("QUNUM", StringType(),True),
            StructField("QUPOS", StringType(),True),
            StructField("DISPO", StringType(),True),
            StructField("SERNR", StringType(),True),
            StructField("BVDAT", StringType(),True),
            StructField("BATOL", DecimalType(3,0),True),
            StructField("BVDRK", DecimalType(7,0),True),
            StructField("EBELN", StringType(),True),
            StructField("EBELP", StringType(),True),
            StructField("BEDAT", StringType(),True),
            StructField("BSMNG", DecimalType(13,3),True),
            StructField("LBLNI", StringType(),True),
            StructField("BWTAR", StringType(),True),
            StructField("XOBLR", StringType(),True),
            StructField("EBAKZ", StringType(),True),
            StructField("RSNUM", StringType(),True),
            StructField("SOBKZ", StringType(),True),
            StructField("ARSNR", StringType(),True),
            StructField("ARSPS", StringType(),True),
            StructField("FIXKZ", StringType(),True),
            StructField("BMEIN", StringType(),True),
            StructField("REVLV", StringType(),True),
            StructField("VORAB", StringType(),True),
            StructField("PACKNO", StringType(),True),
            StructField("KANBA", StringType(),True),
            StructField("BPUEB", StringType(),True),
            StructField("CUOBJ", StringType(),True),
            StructField("FRGGR", StringType(),True),
            StructField("FRGRL", StringType(),True),
            StructField("AKTNR", StringType(),True),
            StructField("CHARG", StringType(),True),
            StructField("UMSOK", StringType(),True),
            StructField("VERID", StringType(),True),
            StructField("FIPOS", StringType(),True),
            StructField("FISTL", StringType(),True),
            StructField("GEBER", StringType(),True),
            StructField("KZKFG", StringType(),True),
            StructField("SATNR", StringType(),True),
            StructField("MNG02", DecimalType(13,3),True),
            StructField("DAT01", StringType(),True),
            StructField("ATTYP", StringType(),True),
            StructField("ADRNR", StringType(),True),
            StructField("ADRN2", StringType(),True),
            StructField("KUNNR", StringType(),True),
            StructField("EMLIF", StringType(),True),
            StructField("LBLKZ", StringType(),True),
            StructField("KZBWS", StringType(),True),
            StructField("WAERS", StringType(),True),
            StructField("IDNLF", StringType(),True),
            StructField("GSFRG", StringType(),True),
            StructField("MPROF", StringType(),True),
            StructField("KZFME", StringType(),True),
            StructField("SPRAS", StringType(),True),
            StructField("TECHS", StringType(),True),
            StructField("MFRPN", StringType(),True),
            StructField("MFRNR", StringType(),True),
            StructField("EMNFR", StringType(),True),
            StructField("FORDN", StringType(),True),
            StructField("FORDP", StringType(),True),
            StructField("PLIFZ", DecimalType(3,0),True),
            StructField("BERID", StringType(),True),
            StructField("UZEIT", LongType(),True),
            StructField("FKBER", StringType(),True),
            StructField("GRANT_NBR", StringType(),True),
            StructField("MEMORY", StringType(),True),
            StructField("BANPR", StringType(),True),
            StructField("RLWRT", DecimalType(15,2),True),
            StructField("BLCKD", StringType(),True),
            StructField("REVNO", StringType(),True),
            StructField("BLCKT", StringType(),True),
            StructField("BESWK", StringType(),True),
            StructField("EPROFILE", StringType(),True),
            StructField("EPREFDOC", StringType(),True),
            StructField("EPREFITM", StringType(),True),
            StructField("GMMNG", DecimalType(13,3),True),
            StructField("WRTKZ", StringType(),True),
            StructField("RESLO", StringType(),True),
            StructField("KBLNR", StringType(),True),
            StructField("KBLPOS", StringType(),True),
            StructField("PRIO_URG", StringType(),True),
            StructField("PRIO_REQ", StringType(),True),
            StructField("MEMORYTYPE", StringType(),True),
            StructField("ANZSN", IntegerType(),True),
            StructField("MHDRZ", DecimalType(4,0),True),
            StructField("IPRKZ", StringType(),True),
            StructField("NODISP", StringType(),True),
            StructField("SRM_CONTRACT_ID", StringType(),True),
            StructField("SRM_CONTRACT_ITM", StringType(),True),
            StructField("BUDGET_PD", StringType(),True),
            StructField("EXPERT_MODE", StringType(),True),
            StructField("CREATIONDATE", StringType(),True),
            StructField("CREATIONTIME", StringType(),True),
            StructField("FMFGUS_KEY", StringType(),True),
            StructField("ADVCODE", StringType(),True),
            StructField("STACODE", StringType(),True),
            StructField("BANFN_CS", StringType(),True),
            StructField("BNFPO_CS", StringType(),True),
            StructField("ITEM_CS", StringType(),True),
            StructField("BSMNG_SND", DecimalType(13,3),True),
            StructField("NO_MARD_DATA", StringType(),True),
            StructField("SERRU", StringType(),True),
            StructField("DISUB_SOBKZ", StringType(),True),
            StructField("DISUB_PSPNR", StringType(),True),
            StructField("DISUB_KUNNR", StringType(),True),
            StructField("DISUB_VBELN", StringType(),True),
            StructField("DISUB_POSNR", StringType(),True),
            StructField("DISUB_OWNER", StringType(),True),
            StructField("IUID_RELEVANT", StringType(),True)])

        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 161
        self.schemaDict[1]['validFrom'] = '19000101'
        self.schemaDict[1]['validUntil'] = '29991231'

    def convDateToLong(self, dataFrame):
        df = dataFrame.withColumn("BADAT_1", F.regexp_replace(F.col("BADAT"), '\.', '').cast(LongType())).drop("BADAT")
        return df.withColumnRenamed("BADAT_1", "BADAT")
        