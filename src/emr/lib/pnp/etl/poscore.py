from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DecimalType

from pnp.etl.datasets import dataset

# ACZ: s3://pnp-data-lake-prod-acz-pos-euw1/bw/td/hprcw01
# STZ: s3://pnp-data-lake-prod-stz-pos-euw1/td/pos/pos_bw_hprcw01

class poscoreData(dataset):

    def __init__(self, configParams):
        super().__init__(configParams)

        self.hudi_options = {}

        # Join all the hudi_options for this dataset
        self.hudi_options.update(dataset.hudi_options)
        
        # Specific hudi_options per dataset
        self.local_hudi_options = {
          'hoodie.datasource.write.recordkey.field': 'HPATXTIME,HPLANT,HPAWID,HPATNR,HPARQU,HPARSIN,HPADCIN,HPATAIN,HPALPGIN,HPATDIN,HPAPRVIND',
          'hoodie.datasource.write.partitionpath.field': 'CALDAY',
          'hoodie.datasource.hive_sync.partition_fields': 'CALDAY',
          'hoodie.upsert.shuffle.parallelism':"20000",
          'hoodie.memory.merge.fraction': '0.80',
          'hoodie.cleaner.policy': 'KEEP_LATEST_FILE_VERSIONS',
          'hoodie.cleaner.fileversions.retained': 1
        }

        # Join all the hudi_options for this dataset
        self.hudi_options.update(self.local_hudi_options)

        self.datasetSchemaEvo = True

        self.Schema = StructType([
            StructField("DI_REQTSN", StringType(), nullable = False),
            StructField("DI_SEQ_NR", IntegerType(),nullable = False),
            StructField("CALDAY", LongType(),False),
            StructField("HPATXTIME", LongType(),False),
            StructField("HPLANT", StringType(),False),
            StructField("HPAWID", StringType(),False),
            StructField("HPATNR", StringType(),False),
            StructField("HPARQU", StringType(),False),
            StructField("HPARSIN", StringType(),False),
            StructField("HPADCIN", StringType(),False),
            StructField("HPATAIN", StringType(),False),
            StructField("HPALPGIN", StringType(),False),
            StructField("HPATDIN", StringType(),False),
            StructField("HPAPRVIND", StringType(),False),
            StructField("HPABTY", StringType(),True),
            StructField("HPAPSTP", StringType(),True),
            StructField("HPATTG", StringType(),True),
            StructField("HPATTC", StringType(),True),
            StructField("HPAWIDLOC", StringType(),True),
            StructField("HPAOSTXD", StringType(),True),
            StructField("HPASUPEMP", StringType(),True),
            StructField("HPACSHEMP", StringType(),True),
            StructField("HPASUPOP", StringType(),True),
            StructField("HPACSHOP", StringType(),True),
            StructField("HPACSHRID", StringType(),True),
            StructField("HPARTG", StringType(),True),
            StructField("HPARTC", StringType(),True),
            StructField("HPARRG", StringType(),True),
            StructField("HPARRC", StringType(),True),
            StructField("HPAEMC", StringType(),True),
            StructField("HPAIAINFO", StringType(),True),
            StructField("HPAVASSVC", StringType(),True),
            StructField("HPAVASPRV", StringType(),True),
            StructField("HPAVASARN", StringType(),True),
            StructField("HPAVASRN", StringType(),True),
            StructField("HPAVASISS", StringType(),True),
            StructField("HPAVASDT", StringType(),True),
            StructField("HEANUPC", StringType(),True),
            StructField("HPAOBARCD", StringType(),True),
            StructField("HMATERIAL", StringType(),True),
            StructField("HTAXM1", StringType(),True),
            StructField("HCUSTOMER", StringType(),True),
            StructField("HPADCCRD", StringType(),True),
            StructField("HPAMMCRD", StringType(),True),
            StructField("HPALCEMC", StringType(),True),
            StructField("HCRMSHCRD", StringType(),True),
            StructField("HCRMEMBER", StringType(),True),
            StructField("HCRMSHID", StringType(),True),
            StructField("HLENTTYPE", StringType(),True),
            StructField("HLINCTYPE", StringType(),True),
            StructField("HCRMKCAMP", StringType(),True),
            StructField("HPALRWDCD", StringType(),True),
            StructField("HBPLOYFD", StringType(),True),
            StructField("HPADRTIER", StringType(),True),
            StructField("HPASALHR", StringType(),True),
            StructField("HTEDCAN", StringType(),True),
            StructField("HPARTXIND", StringType(),True),
            StructField("HSSPFL", StringType(),True),
            StructField("HSSINLFL", StringType(),True),
            StructField("HPALP1PYN", StringType(),True),
            StructField("HTXRET", StringType(),True),
            StructField("HITMRET", StringType(),True),
            StructField("HPANCSWYN", StringType(),True),
            StructField("HPASTX", StringType(),True),
            StructField("HTXCAN", StringType(),True),
            StructField("HITMCAN", StringType(),True),
            StructField("HPADVIND", StringType(),True),
            StructField("HPADCIND", StringType(),True),
            StructField("HPAMCIND", StringType(),True),
            StructField("HPROM_YN", StringType(),True),
            StructField("HPAPPIND", StringType(),True),
            StructField("HPADSCIND", StringType(),True),
            StructField("HPACDSIND", StringType(),True),
            StructField("HPAMA1IND", StringType(),True),
            StructField("HPADT1IND", StringType(),True),
            StructField("HPADI1IND", StringType(),True),
            StructField("HPATT1IND", StringType(),True),
            StructField("HPASCIND", StringType(),True),
            StructField("HPATCIND", StringType(),True),
            StructField("HPAFCIND", StringType(),True),
            StructField("HPAICIND", StringType(),True),
            StructField("HPACCIND", StringType(),True),
            StructField("HPAVASIND", StringType(),True),
            StructField("HPADRIND", StringType(),True),
            StructField("HPATXSTAT", StringType(),True),
            StructField("HPATYG", StringType(),True),
            StructField("HPATCD", StringType(),True),
            StructField("HPAANO", StringType(),True),
            StructField("HPAPCBIN", StringType(),True),
            StructField("HPAPCEMC", StringType(),True),
            StructField("HPATCVM", StringType(),True),
            StructField("HPATDRFN1", StringType(),True),
            StructField("HPATDRFNO", StringType(),True),
            StructField("HPATDRFN3", StringType(),True),
            StructField("HCRMVRCRD", StringType(),True),
            StructField("HPATDAUTH", StringType(),True),
            StructField("HPATDPCAT", StringType(),True),
            StructField("HPATDDT", StringType(),True),
            StructField("HPAACCTYP", StringType(),True),
            StructField("HPATAG", StringType(),True),
            StructField("HPATAC", StringType(),True),
            StructField("HPADTG", StringType(),True),
            StructField("HPADTC", StringType(),True),
            StructField("HPADRG", StringType(),True),
            StructField("HPADRC", StringType(),True),
            StructField("HPASLMEMP", StringType(),True),
            StructField("HRT_PROMO", StringType(),True),
            StructField("LOC_CURRCY", StringType(),True),
            StructField("BASE_UOM", StringType(),True),
            StructField("SALES_UNIT", StringType(),True),
            StructField("HPASQYBU", DecimalType(17,3),True),
            StructField("HPASQYSU", DecimalType(17,3),True),
            StructField("HPAAUPLC", DecimalType(17,2),True),
            StructField("HPANSALC", DecimalType(17,2),True),
            StructField("HPASATLC", DecimalType(17,2),True),
            StructField("HPATSGVLC", DecimalType(17,2),True),
            StructField("HPADEVLC", DecimalType(17,2),True),
            StructField("HPADIVLC", DecimalType(17,2),True),
            StructField("HPACIDELC", DecimalType(17,2),True),
            StructField("HPACIDILC", DecimalType(17,2),True),
            StructField("HPATIDELC", DecimalType(17,2),True),
            StructField("HPATIDILC", DecimalType(17,2),True),
            StructField("HPATTDELC", DecimalType(17,2),True),
            StructField("HPATTDILC", DecimalType(17,2),True),
            StructField("HPATAMLC", DecimalType(17,2),True),
            StructField("HPAITTALC", DecimalType(17,2),True),
            StructField("HPATTTALC", DecimalType(17,2),True),
            StructField("HPATATLC", DecimalType(17,2),True),
            StructField("HLENPNTS", DecimalType(17,3),True),
            StructField("HLFDPNTS", DecimalType(17,3),True),
            StructField("HPACPREDC", IntegerType(),True),
            StructField("HPALPTEPR", DecimalType(17,3),True),
            StructField("HPALPTBPR", DecimalType(17,3),True),
            StructField("HPALDEVLC", DecimalType(17,2),True),
            StructField("HPALDIVLC", DecimalType(17,2),True),
            StructField("HPALFEVLC", DecimalType(17,2),True),
            StructField("HPALFIVLC", DecimalType(17,2),True),
            StructField("HPABSEVLC", DecimalType(17,2),True),
            StructField("HPABSIVLC", DecimalType(17,2),True),
            StructField("HPADRMAMT", DecimalType(17,3),True),
            StructField("HPAPPEVLC", DecimalType(17,2),True),
            StructField("HPAPPIVLC", DecimalType(17,2),True),
            StructField("HPAITSCND", IntegerType(),True),
            StructField("HPATDDRN", IntegerType(),True),
            StructField("HPABSVTYP", StringType(), True),
            StructField("HINQCSCC", StringType(), True),
            StructField("HINQCSDET", StringType(), True),
            StructField("HINQCSINT", StringType(), True),
            StructField("HINQTYPE", StringType(), True),
            StructField("HINQOUTC", StringType(), True),
            StructField("HINQREF", StringType(), True),
            StructField("HINQPRMLC", DecimalType(17,3), True)])

        self.schemaDict[1] = {'ver':1}
        self.schemaDict[1]['cols'] = 144
        self.schemaDict[1]['validFrom'] = '20160301'
        self.schemaDict[1]['validUntil'] = '20220928'

        self.schemaDict[2] = {'ver':2}
        self.schemaDict[2]['cols'] = 152
        self.schemaDict[2]['validFrom'] = '20220928'
        self.schemaDict[2]['validUntil'] = '20300228'
        
        