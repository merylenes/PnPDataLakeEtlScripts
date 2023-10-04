"""
This Glue Job for Snapshots of Datasets
"""

import sys
from datetime import datetime
from urllib.parse import urlparse
import copy
from dateutil.relativedelta import relativedelta
import boto3

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.session import SparkSession

### Glue Job Functions ###

def replace(column, value):
    """ 
    Replace a given value with NULL in a given column 
    """
    return F.when(column != value, column).otherwise(F.lit(None))


def get_glue_args(mandatory_fields, default_optional_args):
    """ 
    The glue args are available in sys.argv with an extra '--' 
    """
    given_optional_fields_key = list(set([i[2:] for i in sys.argv]).intersection([i for i in default_optional_args]))

    args = getResolvedOptions(sys.argv,
                              mandatory_fields+given_optional_fields_key)

    #  Overwrite default value if optional args are provided
    default_optional_args.update(args)

    return default_optional_args


def get_calday(calday, previous_day = False):
    """ 
    Determine CALDAY - If the args contains a calday value use that instead of today() 
    Optional parameter 'yesterday' - if True, the function should return today's date - 1 day.
    """
    if calday is None:
        # The calday format must be YYYYmmdd.
        today = datetime.today()

        if previous_day:
            # Subtract 1 day
            yesterday_date = today - relativedelta(days=1)
            calday = str(yesterday_date.strftime('%Y%m%d'))
        else:
            calday = str(today.strftime('%Y%m%d'))
    else:
        calday = calday
    return calday


def get_table_definition(DBName, TblName):
    """
    Generic function to retrieve the table definitions
    """
    glue_client = boto3.client('glue')
    table_def = glue_client.get_table(
        DatabaseName=DBName,
        Name=TblName
    )
    return table_def


def get_table_partitions(DBName, TblName):
    """
    Generic function to retrieve the table partitions
    """
    glue_client = boto3.client('glue')
    table_partitions = glue_client.get_partitions(
        DatabaseName=DBName,
        TableName=TblName
    )['Partitions']
    return table_partitions


def purge_table(database_name, table_name, partition, main_partition_column):
    """ 
    This function removes the specific partition and empty related bucket/key 
    """
    s3 = boto3.resource('s3')
    glue_client = boto3.client('glue')

    get_table_response = get_table_definition(database_name, table_name)
    storage_descriptor = get_table_response['Table']['StorageDescriptor']

    parsed_url = urlparse(storage_descriptor['Location'])

    # Delete complete partition
    val_partition_predicate = parsed_url.path.replace('/', '', 1) + main_partition_column + "=" + str(partition) + "/"

    bucket = s3.Bucket(parsed_url.netloc)
    bucket.objects.filter(Prefix=val_partition_predicate).delete()

    partition_expression = main_partition_column + "=" + str(partition)

    paginator = glue_client.get_paginator('get_partitions')

    itr = paginator.paginate(
        DatabaseName=database_name, 
        TableName=table_name, 
        Expression=partition_expression
        )

    for page in itr:
        delete_partitions(
            database_name, 
            table_name, 
            page["Partitions"]
            )


def delete_partitions(database_name, table_name, partitions, batch=25):
    """ 
    This function batch delete partitions in batches for 25 
    """
    for i in range(0, len(partitions), batch):
        to_delete = [{k:v[k]} for k,v in zip(["Values"]*batch, partitions[i:i+batch])]
        glue_client.batch_delete_partition(
            DatabaseName=database_name,
            TableName=table_name,
            PartitionsToDelete=to_delete)


def get_current_schema(table_name, database_name):
    """ 
    This function is used to get the current schema of the table 
    """
    response = glue_client.get_table(        
        DatabaseName=database_name,
        Name=table_name)
 
    table_data = {}
    table_data['input_format'] = response['Table']['StorageDescriptor']['InputFormat']
    table_data['output_format'] = response['Table']['StorageDescriptor']['OutputFormat']
    table_data['table_location'] = response['Table']['StorageDescriptor']['Location']
    table_data['serde_info'] = response['Table']['StorageDescriptor']['SerdeInfo']
    table_data['partition_keys'] = response['Table']['PartitionKeys']

    return table_data


def create_partitions(data, database_name, table_name):
    """ 
    This function batch create partitions in batches of 100 
    """
    break_list_in_chunks = lambda data, chunk: [data[x:x+chunk] for x in range(0, len(data), chunk)]

    for i, data in enumerate(break_list_in_chunks(data, 100)):

        create_partition_response = glue_client.batch_create_partition(
                DatabaseName=database_name,
                TableName=table_name,
                PartitionInputList=data
            )


def generate_partition_input(partition, sub_partition, sub_partition_key, s3_input, table_data):
    """ 
    This function generate the input needed to create batch partitions 
    """
    part_location = s3_input + "CALDAY=" + str(partition) + "/" + str(sub_partition_key) + "=" + str(sub_partition) + '/'

    input_dict = {
        'Values': [
            partition, sub_partition
        ],
        'StorageDescriptor': {
            'Location': part_location,
            'InputFormat': table_data['input_format'],
            'OutputFormat': table_data['output_format'],
            'SerdeInfo': table_data['serde_info']
        }
    }

    return input_dict


def generate_partition_input_list(partition, sub_partitions, sub_partition_key, s3_input,table_data):
    """ 
    This function generate the list needed to create batch partitions 
    """
    input_list = []

    for sub_partition in sub_partitions:
        input_list.append(generate_partition_input(partition, sub_partition, sub_partition_key, s3_input, table_data))

    return input_list


def get_glue_partitions(database_name, table_name):
    """ 
    This function gets the partitions by using the Next Token for all values 
    """
    partitions = []

    next_token = ''
    while True:  
        resp = glue_client.get_partitions(
            DatabaseName=database_name,
            TableName=table_name,
            NextToken=next_token)
        partitions += resp['Partitions']

        if 'NextToken' not in resp:
            break
        next_token = resp['NextToken']

    return partitions

# Defining mandatory/optional args
mandatory_fields = [
    'JOB_NAME',
    'ACZ_DATABASE_NAME',
    'ACZ_TABLE_NAME',
    'STZ_DATABASE_NAME',
    'STZ_TABLE_NAME',
    'TRG_FRAMEW'
]
default_optional_args = {
    'PARTITION_KEY': None,
    'CALDAY': None,
    'DATASOURCE_S3': False,
    'S3_PATH_SOURCE': None,
    'PREVIOUS_DAY': False
}

# Retrieve args
args = get_glue_args(mandatory_fields, default_optional_args)

# Set local vars
source_db_name = str(args['ACZ_DATABASE_NAME'])
source_table_name = str(args['ACZ_TABLE_NAME'])
target_db_name = str(args['STZ_DATABASE_NAME'])
target_table_name = str(args['STZ_TABLE_NAME'])
target_framework = str(args['TRG_FRAMEW']).upper()
s3SourcePath= str(args['S3_PATH_SOURCE'])
job_name = str(args['JOB_NAME'])
partition_key = args['PARTITION_KEY']

# This is to ensure that the logic works if the correct Boolean value is not passed in args for DATASOURCE_S3
ds_s3 = False
if str(args['DATASOURCE_S3']).upper() == 'FALSE':
    ds_s3 = False
elif str(args['DATASOURCE_S3']).upper() == 'TRUE':
    ds_s3 = True

# This is to ensure that the logic works if the correct Boolean value is not passed in args for PREVIOUS_DAY
previous_day = False
if str(args['PREVIOUS_DAY']).upper() == 'FALSE':
    previous_day = False
elif str(args['PREVIOUS_DAY']).upper() == 'TRUE':
    previous_day = True

glue_client = boto3.client('glue')

CALDAY = 'CALDAY'

cal_day = get_calday(args['CALDAY'],previous_day)

if target_framework == 'EMR':

    spark = SparkSession.builder \
        .config('spark.serializer','org.apache.spark.serializer.KryoSerializer') \
        .config('spark.sql.hive.convertMetastoreParquet','false') \
        .getOrCreate()

    sc = spark.sparkContext
    glueContext = GlueContext(sc)
    job = Job(glueContext)
    job.init(job_name, args)

    hudiTableS3SourcePath = s3SourcePath

    if ds_s3 == False:

        get_table_response = glue_client.get_table(
            DatabaseName=source_db_name,
            Name=source_table_name
        )
        storage_descriptor = get_table_response['Table']['StorageDescriptor']
        hudiTableS3SourcePath = storage_descriptor['Location']

    ds = spark.read.format('hudi').load(hudiTableS3SourcePath + "/*/")

    if (partition_key != None):
        partition_key = str(partition_key)

        # Check if this partition_key exist in target Table
        partition_keys = []
        table_def_partiton_keys = get_table_definition(
            target_db_name,
            target_table_name
            )

        # Get all partition keys from the target table
        for part_key in table_def_partiton_keys['Table']['PartitionKeys']:
            partition_keys.append(part_key['Name'])

        partition_key_exist = partition_key.upper() in (name.upper() for name in partition_keys)

        # If the partition_key_exist in target table it can be used otherwise ignore
        if (partition_key_exist):
            # Check if the column is present in the source dataframe
            column_exist = partition_key.upper() in (name.upper() for name in ds.columns)

            if (column_exist):

                # Add calday as a new column
                ds = ds.withColumn(CALDAY, F.lit(cal_day))
                # Move the partition column to the back of the dataFrame
                cols = [name.upper() for name in ds.columns if name != partition_key.upper()] + [partition_key.upper()]
                ds = ds.select(cols)

    else:
        ds = ds.withColumn(CALDAY, F.lit(cal_day))

    get_table_response = get_table_definition(
        target_db_name,
        target_table_name
        )

    distinct_calday_partitions = [cal_day]

    # Get all the persisted partitions in the Target Table
    get_target_partitions = get_table_partitions(
        target_db_name,
        target_table_name
        )

    # Create a list of all the target table partitions
    target_partition_list = []

    for target_partition in get_target_partitions:
        target_partition_list.append(target_partition["Values"][0])

    for partition in distinct_calday_partitions:

        storage_descriptor = get_table_response['Table']['StorageDescriptor']
        custom_storage_descriptor = copy.deepcopy(storage_descriptor)

        # Check if the calday partition exist in the target table partition list
        current_partition_exist = any(x in partition for x in target_partition_list)

        # If the partition exist in the target table, delete the partition and purge s3 bucket folder (purge_table)
        if (current_partition_exist):
            purge_table(
                target_db_name, 
                target_table_name, 
                partition, 
                CALDAY
                )

        if (partition_key != None):
            partition_key = str(partition_key)

            # Get all the partitions in the source Table
            get_source_partitions = get_glue_partitions(
                source_db_name,
                source_table_name
                )

            # Create a list of all the source table partitions to determine the target partitions
            source_partition_list = []

            for source_partition in get_source_partitions:
                source_partition_list.append(source_partition["Values"][0])

            table_data = get_current_schema(
                target_table_name, 
                target_db_name
                )

            s3_input_glue = storage_descriptor['Location']

            data = generate_partition_input_list(
                partition, 
                source_partition_list, 
                partition_key,
                s3_input_glue, 
                table_data
                )

            create_partitions(
                data, 
                target_db_name, 
                target_table_name
                )
            # Use the target table storage_descriptor    
            s3TargetPath = storage_descriptor['Location']
            ds.write.partitionBy(CALDAY, partition_key).save("{bucket}/".format(bucket=s3TargetPath), format='parquet', mode="append")
        else:
            # Only calday is the partition
            custom_storage_descriptor['Location'] = storage_descriptor['Location'] + CALDAY + "=" + str(partition) + '/'

            create_partition_response = glue_client.create_partition(
                DatabaseName=target_db_name,
                TableName=target_table_name,
                PartitionInput={
                    'Values': [str(partition)],
                    'StorageDescriptor': custom_storage_descriptor}
            )
            # Use the target table storage_descriptor
            s3TargetPath = storage_descriptor['Location']
            ds.write.partitionBy(CALDAY).save("{bucket}/".format(bucket=s3TargetPath), format='parquet', mode="append")

# Glue
else:
    sc = SparkContext()
    glueContext = GlueContext(sc)
    job = Job(glueContext)
    job.init(job_name, args)

    # Reading Data from s3. If the parameter is set to False it will read the ACZ_TABLE_NAME.
    if ds_s3 == True:
        # csv file should contain a HEADER and column separator is a pipe -> |
        ds = glueContext.create_dynamic_frame_from_options(
            connection_type = "s3",
            connection_options = {
                "paths": [s3SourcePath],
                "recurse": True },
            format = "csv",
            format_options={
                "withHeader": True,
                "separator": "|",
                "quoteChar": '"',
                "escaper": '"'
            }
        )
    else:
        ds = glueContext.create_dynamic_frame.from_catalog(
            database = source_db_name,
            table_name = source_table_name,
            transformation_ctx = "ds"
            )

    sparkDataframe = ds.toDF()
    #Add cal_day to DataFrame
    sparkDataframe = sparkDataframe.withColumn(CALDAY, F.lit(cal_day))

    additionalOptions = {}

    get_table_response = get_table_definition(target_db_name,target_table_name)

    if (partition_key != None):
        partition_key = str(partition_key)
        # Check if this partition_key exist in target Table
        partition_keys = []

        table_def_partiton_keys = get_table_definition(
            target_db_name,
            target_table_name
        )
        # Get all partition keys from the target table
        for part_key in table_def_partiton_keys['Table']['PartitionKeys']:
            partition_keys.append(part_key['Name'])

        partition_key_exist = partition_key.upper() in (name.upper() for name in partition_keys)

        # If the partition_key_exist in target table it can be used otherwise ignore
        if (partition_key_exist):

            column_exist = partition_key.upper() in (name.upper() for name in sparkDataframe.columns)

            if (column_exist):
                # Move the column to the end of the dataframe
                cols = [name.upper() for name in sparkDataframe.columns if name != partition_key.upper()] + [partition_key.upper()]
                sparkDataframe = sparkDataframe.select(cols)

    distinct_calday_partitions = [cal_day]

    # Get all the persisted partitions in the Target Table
    get_target_partitions = get_table_partitions(
        target_db_name,
        target_table_name
        )
    # Create a list of all the target table partitions
    target_partition_list = []

    for target_partition in get_target_partitions:
        target_partition_list.append(target_partition["Values"][0])

    for partition in distinct_calday_partitions:

        storage_descriptor = get_table_response['Table']['StorageDescriptor']
        custom_storage_descriptor = copy.deepcopy(storage_descriptor)

        # Check if the calday partition exist in the target table partition list
        partition_exist = any(x in partition for x in target_partition_list)

        # If the partition exist in the target table, delete the partition and purge s3 bucket folder (purge_table)
        if (partition_exist):

            purge_table(
                target_db_name, 
                target_table_name, 
                partition, 
                CALDAY
                )

        if (partition_key != None):

            partition_key = str(partition_key)
            # Get all the partitions in the source Table
            get_source_partitions = get_glue_partitions(
                source_db_name,
                source_table_name
                )

            # Create a list for the source table partitions
            source_partition_list = []

            for source_partition in get_source_partitions:
                source_partition_list.append(source_partition["Values"][0])

            table_data = get_current_schema(
                target_table_name, 
                target_db_name
                )

            s3_input_glue = storage_descriptor['Location']

            data = generate_partition_input_list(
                partition, 
                source_partition_list, 
                partition_key,
                s3_input_glue, 
                table_data
                )

            create_partitions(
                data, 
                target_db_name, 
                target_table_name
                )

            additionalOptions["partitionKeys"] = [CALDAY, partition_key]

        else:
            # Only calday is the partition
            custom_storage_descriptor['Location'] = storage_descriptor['Location'] + CALDAY + "=" + str(partition) + '/'

            create_partition_response = glue_client.create_partition(
                DatabaseName=target_db_name,
                TableName=target_table_name,
                PartitionInput={
                    'Values': [str(partition)],
                    'StorageDescriptor': custom_storage_descriptor}
            )
            additionalOptions["partitionKeys"] = [CALDAY]

    rc = DynamicFrame.fromDF(
        sparkDataframe,
        glueContext,
        "rc"
    )

    additionalOptions["enableUpdateCatalog"] = True
    # Write the DataFrame
    sink = glueContext.write_dynamic_frame_from_catalog(
        frame=rc,
        database = target_db_name,
        table_name = target_table_name,
        additional_options=additionalOptions,
        transformation_ctx="write_sink"
    )
job.commit()
