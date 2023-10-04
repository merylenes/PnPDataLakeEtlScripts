from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import countDistinct, col
from pyspark.sql.types import BooleanType
import os, sys, re, json, argparse, socket
from pnp import logging, utils
from pnp.etl import *

#    print(f"beginning time set to: {beginTime}")
#    return beginTime

if __name__ == '__main__':
    '''
        Calling this main program can be done using either individual config options on the command line
        e.g. allDatasets.py --destSTZ s3://blaah/ --sourceURI s3://blaah

        or it can be called with a --config JSON object specifying each of these parameters
        e.g. allDatasets.py --config {  "datasetName":"DCRCHEADER",
                                        "destSTZ": "s3://pnp-data-lake-dev-stz-retail-euw1/td/dcrc/erp_dcrc_hd/",
                                        "sourceURI": "s3://pnp-data-lake-dev-acz-retail-euw1/erp/td/dcrc_hd/",
                                        "database": "stz-retail-dev",
                                        "tableName": "erp_dcrc_hd" }
    '''

    parser = argparse.ArgumentParser()

    group1 = parser.add_argument_group('config',description='Specifying the config as a JSON object')
    group2 = parser.add_argument_group('independent config',description='Specifying the config as independent parameters')
    
    group1.add_argument("--config", "-c", help="A JSON object containing the config for the job")
    group2.add_argument("--destSTZ", "-d", help="The destination URI for the STZ data")
    group2.add_argument("--sourceURI", "-s", help="The source URI for the raw data")
    group2.add_argument("--database", "-db", help="The Glue catalog database to write data to")
    group2.add_argument("--tableName", "-t", help="The Glue catalog table to write data to")
    group2.add_argument("--datasetName", "-dn", help="The name of the dataset.")
    group2.add_argument("--hiveServer", "-hs", help="The IP/hostname of the Hive Server (used for EKS).")
    group2.add_argument("--hivePort", "-hp", help="The port of the Hive Server (used for EKS).")
    group2.add_argument("--schemaVer", "-v", help="The version of the schema for this CSV dataset.")

    
    args = parser.parse_args()

    # Whichever way allDatasets.py is called, we need to create a conf file that is a dictionary
    # we can use later in the code. This code does that

    if args.hiveServer == None:
        args.hiveServer = socket.gethostbyname(socket.gethostname())
        args.hivePort = "10000" 
        
    if args.config:
        # If a JSON conf is supplied
        jsonConf = args.config
        conf = json.loads(jsonConf)
    else:
        # If individual config options are supplied. Make a JSON struct out of the individual arguments
        conf = {"sourceURI":args.sourceURI,
                "destSTZ": args.destSTZ,
                "database": args.database,
                "tableName": args.tableName,
                "datasetName": args.datasetName,
                "hiveServer": args.hiveServer,
                "hivePort": args.hivePort,
                "schemaVer": args.schemaVer }

        jsonConf = json.dumps(conf)

    spark = SparkSession \
            .builder \
            .getOrCreate()

    # Set up logging early on. We want to track progress.
    # Note: when the job is run from a Jupyter notebook, these logs end up in the livy log file
    # while when it's run as a step, they end up in the stdout of the step
    # Where you find them depends on where they were called from. However, there's a nice way
    # to find them logs. Seach in the logfile for "<--" since each log is prefixed by this and
    # suffixed by "-->"
    
    sparkLogger = logging.Log4j(spark)
    sparkLogger.info(f"{conf['datasetName']} data PySpark script. Logger initialised")
    sparkLogger.info(f"CONFIG = {json.dumps(conf, indent=2)}")

    # We don't want to specify all the classes in a large if statement, so
    # instead we read the datasetName and lowercase it (e.g. poscore)
    # then we add the string Data (now it's poscoreData) which is the name of the class

    dSet = conf['datasetName'].lower()
    dsetData = dSet + "Data"

    # Once we have the name of the class, we file the attibutes for this class
    # this is a bit of magic to dynamically load the class and instantiate it

    thisSet = globals()[dSet]
    thisData = getattr(thisSet, dsetData)

    sparkLogger.info(f"{thisData.hudi_options}")

    try:
        inputData = thisData(conf) 
        # Note that for some datasets we have to re-instantiate this because of different
        # partition keys, but we do it here in order to read the data initially

    except Exception as e:
        sparkLogger.error(f"Dataset {conf['datasetName']} has not been defined. Error {e}")
        sys.exit(1)

    dataWritten = False

    # We expect that we're going to get this schema version as passed into the
    # allDatasets.py 
    expectedSchemaVer = int(args.schemaVer)

    # Set out all the utils we're going to require
    util = utils.utils()

    # Added this because we want some datasets to have schema evolution code while others won't
    # so we added a check here for whether the dataset actually has schema evolution. If it doesn't have
    # then we proceed as normal, while if it DOES have, then we want to get the correct schema

    try:
        # Check whether this dataset has a schema version or not
        # Just check the first schema version. If this is doesn't throw a key error, then we have a dataset 
        # schema changes
        ver = inputData.schemaDict[1]['ver']
        datasetSchemaEvo = inputData.datasetSchemaEvo
        sparkLogger.info(f"Dataset {conf['datasetName']} has an evolved schema. Found datasetSchemaEvo for job to be {datasetSchemaEvo}")

    except KeyError as e:
        # No schema dict for this dataset,
        # so we've not got to modifying this schema yet
        datasetSchemaEvo = False
        sparkLogger.info(f"Dataset {conf['datasetName']} has not got any schema evolution. Found datasetSchemaEvo for job to be {datasetSchemaEvo}")
    
    dataSchema = inputData.Schema
    
    hoodie_del_col_present = False

    if datasetSchemaEvo:

        # Count the columns from the first 2 rows of the input file
        # to determine what schema version we're talking about here.
        df, cols, hoodie_del_col_present = util.countColumns(spark, inputData.sourceURI)

        if hoodie_del_col_present:
            sparkLogger.info(f"Found _hoodie_is_deleted colum, and {cols} columns for dataset {conf['datasetName']} in the CSV")

            # We need the position of the _hoodie_is_deleted column for future proofing
            # while this may have started out as follow:
            # col1, col2, col3, colN, _hoodie_is_deleted
            # if the schema is evolved in the future, we will end up with this:
            # col1, col2, col3, colN, _hoodie_is_deleted, colN+1, colN+2
            # So we need to find the position of the _hoodie_is_deleted column so that we can adjust the schema accordingly for reading
            
            hoodie_del_col_position = df.columns.index('_hoodie_is_deleted')
            sparkLogger.info(f"Found _hoodie_is_deleted column as position {hoodie_del_col_position} for dataset {conf['datasetName']} in the CSV")

        else:
            sparkLogger.info(f"Found {cols} columns for dataset {conf['datasetName']} in the CSV")

        schemaVer = inputData.getSchemaVer(cols, df)
        dataSchema = inputData.Schema

        # I'm leaving this logic in here in case things change in the future.
        # Now if the _hoodie_is_deleted column is at the end of the columns we're ingesting,
        # then we want to simply add that to the existing schema
        # otherwise, we want to incorporate the _hoodie_is_deleted column as the correct place in the schema
        # so that the read doesn't shift columns up or down

        #if hoodie_del_col_present and (hoodie_del_col_position + 1) == cols:
        #    # It's at the end of the data, so just add it to the schema
        #    dataSchema.add('_hoodie_is_deleted', BooleanType(), False)
        #else:
        #    # It's buried somewhere in the data (i.e. the schema must have evolved after the _hoodie_is_deleted flag was added)
        #    pass

        sparkLogger.info(f"Found version {schemaVer} for dataset {conf['datasetName']} for the CSV data being processed")

        # IF the schema version supplied to the driver does not match the schema we find here
        # then we need to fail gracefully.
        if expectedSchemaVer != schemaVer:
            sparkLogger.error(f"Expected version {expectedSchemaVer} schema for dataset, but instead found version {schemaVer} for CSVs being processed. Either fix the entry passed via -v or ingest the correct CSVs")
            sys.exit(1)

    # Read the data
    try:
        df = util.readCSV(spark, inputData.sourceURI, schema=dataSchema)
    except Exception as e:
        sparkLogger.error(f"Seems there was an exception in trying to read the dataframe from CSV. Error {e}")
        sys.exit(1)
        
    # Time to see whether there's any dataset specific code we should run for this dataset
    # NOTE: this allows us to NOT have a big if-then statement for every different dataset
    # TODO: Do something similar for SITEARTICLE and DCRCITEM, but that's for next time

    if datasetSchemaEvo and hasattr(inputData, 'datasetSpecific') and callable(getattr(inputData, 'datasetSpecific')):
        df = inputData.datasetSpecific(df)

    if (df.count() == 0):
        sparkLogger.info(f"There were {df.count()} rows read. Exit gracefully")
        sys.exit(0)

    sparkLogger.info(f"Dataset name is: {conf['datasetName']}")

    # For SiteArticle, we want to partition by something other than CALDAY
    if conf['datasetName'] == "SITEARTICLE":
        inputData = thisData(conf, partition_by="HPLANT")

    # If it's DCRCItem, then we want to add a column on which we can partition and that'll be YYYY from DC_EXTR_DATE
    # ******
    # Remember that if this needs to be modified, we want to move this new column up in the data instead of 
    # adding it at the end
    # ******
    elif conf['datasetName'] == "DCRCITEM":
        sparkLogger.info(f"Dataset is DCRCItem. Adding a new partition key")
        df = inputData.partition_by_year(df)
        #inputData.dataStats(df, primKeys) 
        part_by = 'DI_EXTR_YYYY'
        inputData.hudi_options.update( \
            {'hoodie.datasource.write.partitionpath.field': f"{part_by}",
             'hoodie.datasource.hive_sync.partition_fields': f"{part_by}",})

    # ==== GET ALL THE STATS WE CAN ====
    # Get all the primary keys for this dataset
    primKeys = inputData.getKeys('hoodie.datasource.write.recordkey.field')
    # How many rows have all NULL primary keys
    (totalRows, nonNullRows, diffs) = inputData.dataStats(df, primKeys)
    inputData.datasetStats['total']['rows'] = totalRows
    inputData.datasetStats['nonNullPK']['rows'] = nonNullRows
    inputData.datasetStats['nullPK']['rows'] = diffs

    # How many rows have all NULL Partition keys keys
    partKeys = inputData.getKeys('hoodie.datasource.write.partitionpath.field')
    (totalRows, nonNullRows, diffs) = inputData.dataStats(df, partKeys)
    inputData.datasetStats['nullPartK']['rows'] = diffs

    # Log all the STATS for this dataload
    sparkLogger.info(f"\tHudi Options:\n {json.dumps(inputData.hudi_options, indent=2)}\n")
    sparkLogger.info(f"\n\t===== STATS for {conf['datasetName']} =====\n")
    sparkLogger.info(f"\tRows read: {inputData.datasetStats['total']['rows']}")
    sparkLogger.info(f"\tRows with non-NULL PrimaryKeys: {inputData.datasetStats['nonNullPK']['rows']}")
    sparkLogger.info(f"\tRows with NULL PrimaryKeys: {inputData.datasetStats['nullPK']['rows']}")
    sparkLogger.info(f"\tRows with NULL PartitionKeys: {inputData.datasetStats['nullPK']['rows']}")
    sparkLogger.info(f"\tFile type: {util.fileType}")
    sparkLogger.info(f"\tBucket: {util.bucket}")
    sparkLogger.info(f"\tKey: {util.key}")


    # We know some of the rows are going to be updated by hudi based on the precombine_key
    # and the other PKs so we get those keys here for later analysis
    # Count all Null PKs
    dupPrimKeysDF = util.findDups(spark, df, primKeys)
    inputData.datasetStats['dupPrimayKeysTotal']['rows'] = dupPrimKeysDF.count()

    # Seem that some of the data are coming in with no partition key
    # so we want to remove it

    # Remove rows with nulls in the primary key positions, if there are any
    nonNullPKdf = inputData.removeNullKeys(df, primKeys)

    # Remove rows with nulls in the partition key positions, if there are any
    finalDf = inputData.removeNullKeys(nonNullPKdf, partKeys)
    inputData.datasetStats['rowsRemaining']['rows'] = finalDf.count()

    # Add the precombine_key
    sparkLogger.info(f"Adding the precombine_key...")
    df = inputData.add_precombine_key(finalDf)

    sparkLogger.info(df.printSchema())

    if hoodie_del_col_present:
        delDf = df.filter(col("_hoodie_is_deleted") == "true")
        newDelDf = util.convertDateToPartition(spark, delDf, "date_ingested")

        # Write the deletes to the correct table 
        util.writeDels(spark, inputData, newDelDf, ["date_ingested"])

    # How many individual files did we read?
    num_files = df.select(countDistinct("fileName").alias("files")).collect()
    inputData.datasetStats['filesRead']['count'] = num_files[0].__getitem__('files')
    sparkLogger.info(f"Check before write\n{conf['datasetName']} stats:\n{json.dumps(inputData.datasetStats, indent=2)}")
    try:
        sparkLogger.info("Starting to write final data to STZ zone")


        #if datasetSchemaEvo:
        #    df = df.drop("date_ingested", "filename")

        util.writeData(spark, inputData, df) 
        dataWritten = True
        # Only write the duplicates AFTER the data has been written because if the data-write fails
        # we really don't want to have to clean up the workspace too.
        # Thus the first writeData may fail which means the try will fail before the duplicate data
        # are written to the bucket

        try:
            # Write the duplicate data to a separate space in order to be able to analyse it later
            util.writeDups(spark, inputData, dupPrimKeysDF)
        except Exception as e:
            sparkLogger.error(f"Could not write the DUPLICATES to S3 destination: {e}")
       
    except Exception as e:
        sparkLogger.error(f"Seems there was an exception in trying to write the dataframe to parquet: {e}")
        sys.exit(1)

    if dataWritten:
        sparkLogger.info("Last step. Moving files to PROCESED key")
        sparkLogger.info(f"===== {conf['datasetName']}  ETL done =====")

    sparkLogger.info(f"{conf['datasetName']} stats:\n{json.dumps(inputData.datasetStats, indent=2)}")
