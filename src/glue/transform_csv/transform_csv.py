import sys
import boto3
from awsglue.transforms import *

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F

from transform_enhance import transform_csv_enhance

def get_glue_args(
    mandatory_fields, 
    default_optional_args
    ):
    """
    Function to retrieve the glue job arguments
    """
    # The glue args are available in sys.argv with an extra '--'
    given_optional_fields_key = list(set([i[2:] for i in sys.argv]).intersection([i for i in default_optional_args]))

    args = getResolvedOptions(sys.argv,
                              mandatory_fields+given_optional_fields_key)

    # Overwrite default value if optional args are provided
    default_optional_args.update(args)

    return default_optional_args

def get_table_definition(
    database_name, 
    table_name
    ):
    """
    Generic function to retrieve the table definitions
    """
    glue_client = boto3.client('glue')
    table_def = glue_client.get_table(
        DatabaseName=database_name,
        Name=table_name
        )
    
    return table_def

# Defining mandatory/optional args
mandatory_fields = ['JOB_NAME',
                    'ACZ_DATABASE_NAME',
                    'ACZ_TABLE_NAME',
                    'STZ_DATABASE_NAME',
                    'STZ_TABLE_NAME'
                    ]
default_optional_args = {
    'HEADER': True
}

# Retrieve args
args = get_glue_args(mandatory_fields, default_optional_args)

# mandatory_fields
source_db_name = str(args['ACZ_DATABASE_NAME'])
source_table_name = str(args['ACZ_TABLE_NAME'])
target_db_name = str(args['STZ_DATABASE_NAME'])
target_table_name = str(args['STZ_TABLE_NAME'])

# optional_fields
header = args['HEADER']

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Get the target table definition
get_table_response = get_table_definition(target_db_name, target_table_name)
storage_descriptor = get_table_response['Table']['StorageDescriptor']

# Retrieve the target table storage location needed to write the csv file
csv_path = storage_descriptor['Location']

# Get the columns from the target Database/table and pass into to the enhancement point
# Uppercase column names
columns_exp = []
for attribute in get_table_response['Table']['StorageDescriptor']['Columns']:
    columns_exp.append((attribute['Name']).upper())

# Call the enhancement point in order to customise base on the requirements per dataset
df, csv_delimeter = transform_csv_enhance.custom_prep(
    glueContext,
    source_db_name,
    source_table_name,
    columns_exp
    )

df.write\
    .option("header",header)\
    .option("delimiter",csv_delimeter)\
    .mode("overwrite")\
    .format("csv")\
    .save(csv_path)

job.commit()