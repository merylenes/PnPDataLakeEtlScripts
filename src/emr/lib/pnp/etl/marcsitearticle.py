"""
ACZ: s3://pnp-data-lake-prod-acz-retail-euw1/bw/md/hrsmw01
STZ: s3://pnp-data-lake-prod-stz-retail-euw1/md/sitearticle/sitearticle_erp_marc
"""

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, LongType, DecimalType
from pnp.etl.datasets import dataset

class marcsitearticleData(dataset):

    def __init__(self, configParams):
        super().__init__(configParams)

        self.hudi_options = {}

        # Join all the hudi_options for this dataset
        self.hudi_options.update(dataset.hudi_options)

        # Specific hudi_options per dataset
        self.local_hudi_options = {
            'hoodie.datasource.write.recordkey.field': 'MATNR,WERKS',
            'hoodie.datasource.write.partitionpath.field': 'WERKS',
            'hoodie.datasource.hive_sync.partition_fields': 'WERKS',
            'hoodie.cleaner.policy': 'KEEP_LATEST_FILE_VERSIONS',
            'hoodie.cleaner.commits.retained': 1,
            'hoodie.keep.min.commits': 2,
            'hoodie.keep.max.commits': 5,
            'hoodie.datasource.write.operation': 'insert_overwrite',
            'hoodie.combine.before.insert': 'true'
        }

        # Join all the hudi_options for this dataset
        self.hudi_options.update(self.local_hudi_options)

        self.datasetSchemaEvo = True

        self.Schema = StructType([
            StructField("DI_REQTSN", StringType(),False),
            StructField("DI_SEQ_NR", IntegerType(),False),
            StructField("MATNR", StringType(),False),
            StructField("WERKS", StringType(),False),
            StructField("PSTAT", StringType(),False),
            StructField("LVORM", StringType(),False),
            StructField("BWTTY", StringType(),False),
            StructField("XCHAR", StringType(),False),
            StructField("MMSTA", StringType(),False),
            StructField("MMSTD", LongType(),False),
            StructField("MAABC", StringType(),False),
            StructField("KZKRI", StringType(),False),
            StructField("EKGRP", StringType(),False),
            StructField("AUSME", StringType(),False),
            StructField("DISPR", StringType(),False),
            StructField("DISMM", StringType(),False),
            StructField("DISPO", StringType(),False),
            StructField("KZDIE", StringType(),False),
            StructField("PLIFZ", DecimalType(17,0),False),
            StructField("WEBAZ", DecimalType(17,0),False),
            StructField("PERKZ", StringType(),False),
            StructField("AUSSS", DecimalType(17,2),False),
            StructField("DISLS", StringType(),False),
            StructField("BESKZ", StringType(),False),
            StructField("SOBSL", StringType(),False),
            StructField("MINBE", DecimalType(17,3),False),
            StructField("EISBE", DecimalType(17,3),False),
            StructField("BSTMI", DecimalType(17,3),False),
            StructField("BSTMA", DecimalType(17,3),False),
            StructField("BSTFE", DecimalType(17,3),False),
            StructField("BSTRF", DecimalType(17,3),False),
            StructField("MABST", DecimalType(17,3),False),
            StructField("LOSFX", DecimalType(17,2),False),
            StructField("SBDKZ", StringType(),False),
            StructField("LAGPR", StringType(),False),
            StructField("ALTSL", StringType(),False),
            StructField("KZAUS", StringType(),False),
            StructField("AUSDT", LongType(),False),
            StructField("NFMAT", StringType(),False),
            StructField("KZBED", StringType(),False),
            StructField("MISKZ", StringType(),False),
            StructField("FHORI", StringType(),False),
            StructField("PFREI", StringType(),False),
            StructField("FFREI", StringType(),False),
            StructField("RGEKZ", StringType(),False),
            StructField("FEVOR", StringType(),False),
            StructField("BEARZ", DecimalType(17,2),False),
            StructField("RUEZT", DecimalType(17,2),False),
            StructField("TRANZ", DecimalType(17,2),False),
            StructField("BASMG", DecimalType(17,3),False),
            StructField("DZEIT", DecimalType(17,0),False),
            StructField("MAXLZ", DecimalType(17,0),False),
            StructField("LZEIH", StringType(),False),
            StructField("KZPRO", StringType(),False),
            StructField("GPMKZ", StringType(),False),
            StructField("UEETO", DecimalType(17,1),False),
            StructField("UEETK", StringType(),False),
            StructField("UNETO", DecimalType(17,1),False),
            StructField("WZEIT", DecimalType(17,0),False),
            StructField("ATPKZ", StringType(),False),
            StructField("VZUSL", DecimalType(17,2),False),
            StructField("HERBL", StringType(),False),
            StructField("INSMK", StringType(),False),
            StructField("SPROZ", DecimalType(17,1),False),
            StructField("QUAZT", DecimalType(17,0),False),
            StructField("SSQSS", StringType(),False),
            StructField("MPDAU", DecimalType(17,0),False),
            StructField("KZPPV", StringType(),False),
            StructField("KZDKZ", StringType(),False),
            StructField("WSTGH", DecimalType(17,0),False),
            StructField("PRFRQ", DecimalType(17,0),False),
            StructField("NKMPR", LongType(),False),
            StructField("UMLMC", DecimalType(17,3),False),
            StructField("LADGR", StringType(),False),
            StructField("XCHPF", StringType(),False),
            StructField("USEQU", StringType(),False),
            StructField("LGRAD", DecimalType(17,1),False),
            StructField("AUFTL", StringType(),False),
            StructField("PLVAR", StringType(),False),
            StructField("OTYPE", StringType(),False),
            StructField("OBJID", StringType(),False),
            StructField("MTVFP", StringType(),False),
            StructField("PERIV", StringType(),False),
            StructField("KZKFK", StringType(),False),
            StructField("VRVEZ", DecimalType(17,2),False),
            StructField("VBAMG", DecimalType(17,3),False),
            StructField("VBEAZ", DecimalType(17,2),False),
            StructField("LIZYK", StringType(),False),
            StructField("BWSCL", StringType(),False),
            StructField("KAUTB", StringType(),False),
            StructField("KORDB", StringType(),False),
            StructField("STAWN", StringType(),False),
            StructField("HERKL", StringType(),False),
            StructField("HERKR", StringType(),False),
            StructField("EXPME", StringType(),False),
            StructField("MTVER", StringType(),False),
            StructField("PRCTR", StringType(),False),
            StructField("TRAME", DecimalType(17,3),False),
            StructField("MRPPP", StringType(),False),
            StructField("SAUFT", StringType(),False),
            StructField("FXHOR", StringType(),False),
            StructField("VRMOD", StringType(),False),
            StructField("VINT1", StringType(),False),
            StructField("VINT2", StringType(),False),
            StructField("VERKZ", StringType(),False),
            StructField("STLAL", StringType(),False),
            StructField("STLAN", StringType(),False),
            StructField("PLNNR", StringType(),False),
            StructField("APLAL", StringType(),False),
            StructField("LOSGR", DecimalType(17,3),False),
            StructField("SOBSK", StringType(),False),
            StructField("FRTME", StringType(),False),
            StructField("LGPRO", StringType(),False),
            StructField("DISGR", StringType(),False),
            StructField("KAUSF", DecimalType(17,2),False),
            StructField("QZGTP", StringType(),False),
            StructField("QMATV", StringType(),False),
            StructField("TAKZT", DecimalType(17,0),False),
            StructField("RWPRO", StringType(),False),
            StructField("COPAM", StringType(),False),
            StructField("ABCIN", StringType(),False),
            StructField("AWSLS", StringType(),False),
            StructField("SERNP", StringType(),False),
            StructField("CUOBJ", StringType(),False),
            StructField("STDPD", StringType(),False),
            StructField("SFEPR", StringType(),False),
            StructField("XMCNG", StringType(),False),
            StructField("QSSYS", StringType(),False),
            StructField("LFRHY", StringType(),False),
            StructField("RDPRF", StringType(),False),
            StructField("VRBMT", StringType(),False),
            StructField("VRBWK", StringType(),False),
            StructField("VRBDT", LongType(),False),
            StructField("VRBFK", DecimalType(17,2),False),
            StructField("AUTRU", StringType(),False),
            StructField("PREFE", StringType(),False),
            StructField("PRENC", StringType(),False),
            StructField("PRENO", StringType(),False),
            StructField("PREND", LongType(),False),
            StructField("PRENE", StringType(),False),
            StructField("PRENG", LongType(),False),
            StructField("ITARK", StringType(),False),
            StructField("SERVG", StringType(),False),
            StructField("KZKUP", StringType(),False),
            StructField("STRGR", StringType(),False),
            StructField("CUOBV", StringType(),False),
            StructField("LGFSB", StringType(),False),
            StructField("SCHGT", StringType(),False),
            StructField("CCFIX", StringType(),False),
            StructField("EPRIO", StringType(),False),
            StructField("QMATA", StringType(),False),
            StructField("RESVP", DecimalType(17,0),False),
            StructField("PLNTY", StringType(),False),
            StructField("UOMGR", StringType(),False),
            StructField("UMRSL", StringType(),False),
            StructField("ABFAC", DecimalType(17,1),False),
            StructField("SFCPF", StringType(),False),
            StructField("SHFLG", StringType(),False),
            StructField("SHZET", StringType(),False),
            StructField("MDACH", StringType(),False),
            StructField("KZECH", StringType(),False),
            StructField("MEGRU", StringType(),False),
            StructField("MFRGR", StringType(),False),
            StructField("VKUMC", DecimalType(17,2),False),
            StructField("VKTRW", DecimalType(17,2),False),
            StructField("KZAGL", StringType(),False),
            StructField("FVIDK", StringType(),False),
            StructField("FXPRU", StringType(),False),
            StructField("LOGGR", StringType(),False),
            StructField("FPRFM", StringType(),False),
            StructField("GLGMG", DecimalType(17,3),False),
            StructField("VKGLG", DecimalType(17,2),False),
            StructField("INDUS", StringType(),False),
            StructField("MOWNR", StringType(),False),
            StructField("MOGRU", StringType(),False),
            StructField("CASNR", StringType(),False),
            StructField("GPNUM", StringType(),False),
            StructField("STEUC", StringType(),False),
            StructField("FABKZ", StringType(),False),
            StructField("MATGR", StringType(),False),
            StructField("VSPVB", StringType(),False),
            StructField("DPLFS", StringType(),False),
            StructField("DPLPU", StringType(),False),
            StructField("DPLHO", DecimalType(17,0),False),
            StructField("MINLS", DecimalType(17,3),False),
            StructField("MAXLS", DecimalType(17,3),False),
            StructField("FIXLS", DecimalType(17,3),False),
            StructField("LTINC", DecimalType(17,3),False),
            StructField("COMPL", StringType(),False),
            StructField("CONVT", StringType(),False),
            StructField("SHPRO", StringType(),False),
            StructField("AHDIS", StringType(),False),
            StructField("DIBER", StringType(),False),
            StructField("KZPSP", StringType(),False),
            StructField("OCMPF", StringType(),False),
            StructField("APOKZ", StringType(),False),
            StructField("MCRUE", StringType(),False),
            StructField("LFMON", StringType(),False),
            StructField("LFGJA", StringType(),False),
            StructField("EISLO", DecimalType(17,3),False),
            StructField("NCOST", StringType(),False),
            StructField("ROTATION_DATE", StringType(),False),
            StructField("UCHKZ", StringType(),False),
            StructField("UCMAT", StringType(),False),
            StructField("BWESB", DecimalType(17,3),False),
            StructField("TOLPRPL", DecimalType(17,1),False),
            StructField("TOLPRMI", DecimalType(17,1),False),
            StructField("R_PKGRP", StringType(),False),
            StructField("R_LANE_NUM", StringType(),False),
            StructField("R_PAL_VEND", StringType(),False),
            StructField("R_FORK_DIR", StringType(),False),
            StructField("IUID_RELEVANT", StringType(),False),
            StructField("IUID_TYPE", StringType(),False),
            StructField("UID_IEA", StringType(),False),
            StructField("CONS_PROCG", StringType(),False),
            StructField("GI_PR_TIME", DecimalType(17,0),False),
            StructField("MULTIPLE_EKGRP", StringType(),False),
            StructField("REF_SCHEMA", StringType(),False),
            StructField("MIN_TROC",  StringType(),False),
            StructField("MAX_TROC", StringType(),False),
            StructField("TARGET_STOCK", DecimalType(17,3),False),
            StructField("ATTR01", StringType(),False),
            StructField("ATTR02", StringType(),False),
            StructField("ATTR03", StringType(),False),
            StructField("ATTR04", StringType(),False),
            StructField("ATTR05", StringType(),False),
            StructField("ATTR06", StringType(),False),
            StructField("ATTR07", StringType(),False),
            StructField("ATTR08", StringType(),False),
            StructField("ATTR09", StringType(),False),
            StructField("ATTR10", StringType(),False),
            StructField("ATTR11", StringType(),False),
            StructField("ATTR12", StringType(),False),
            StructField("ATTR13", StringType(),False),
            StructField("EXIT_RSN", StringType(),False)])
        
        self.schemaDict[1] = {'ver': 1}
        self.schemaDict[1]['cols'] = 235
        self.schemaDict[1]['validFrom'] = '19000101'
        self.schemaDict[1]['validUntil'] = '29991231'
