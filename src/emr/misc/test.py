from lib.pnp.etl import valuatedcosData, sitearticleData

mmaConf = { "datasetName":"MARGINMANUALADJUSTMENTS",
        "destSTZ": "s3://pnp-data-lake-dev-stz-finance-euw1/td/margin_man_adj/margin_man_adj_hfglm03/",
        "sourceURI": "s3://pnp-data-lake-dev-acz-finance-euw1/bw/td/hfglm03",
        "database": "stz-finance-dev",
        "tableName": "margin_man_adj_hfglm03" }


valuatedcosConf = { "datasetName":"VALUATEDCOS",
        "destSTZ": "s3://pnp-data-lake-dev-stz-finance-euw1/td/cos/cos_bw_hfglm01/",
        "sourceURI": "s3://pnp-data-lake-dev-acz-finance-euw1/bw/td/hfglm01/",
        "database": "stz-finance-dev",
        "tableName": "cos_bw_hfglm01" }
valuatedcos = valuatedcosData(valuatedcosConf)

sitearticleConf = { "datasetName":"SITEARTICLE",
        "destSTZ": "s3://pnp-data-lake-prod-stz-retail-euw1/md/sitearticle/sitearticle_bw_sitearticle_01/",
        "sourceURI": "s3://pnp-data-lake-prod-acz-retail-euw1/bw/md/hrsmw01/",
        "database": "stz-retail-prod",
        "tableName": "sitearticle_bw_sitearticle_01" }

sitearticle = sitearticleData(sitearticleConf, "HPLANT")
print(sitearticle.hudi_options)

#ebanConf = { "datasetName":"EBAN",
#             "destSTZ": "s3://pnp-data-lake-dev-stz-retail-euw1/td/eban/eban_erp_eban/",
#             "sourceURI": "s3://pnp-data-lake-dev-acz-retail-euw1/erp/td/eban/",
#             "database": "stz-retail-dev",
#             "tableName": "eban_erp_eban" }
#
#eban = ebanData(ebanConf)
#print(valuatedcos.hudi_options)

