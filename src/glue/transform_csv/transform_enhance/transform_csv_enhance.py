from pyspark.sql import DataFrame, functions as F
from awsglue.context import GlueContext
import os.path
import boto3
from urllib.parse import urlparse
import datetime
import pandas as pd

now = datetime.datetime.now()
glue_client = boto3.client('glue')
s3_resource = boto3.resource('s3')

def get_table_definition(
    database_name, 
    table_name
    ):
    """
    Generic function to retrieve the table definitions
    """
    table_def = glue_client.get_table(
        DatabaseName=database_name,
        Name=table_name
        )
    
    return table_def

def get_glue_partitions(
    database_name, 
    table_name
    ):
    """ 
    This function gets the partitions by using the Next Token for all values 
    """
    partitions = []

    next_token = ''
    while True:  
        resp = glue_client.get_partitions(
            DatabaseName=database_name,
            TableName=table_name,
            NextToken=next_token
            )

        partitions += resp['Partitions']

        if 'NextToken' not in resp:
            break
        next_token = resp['NextToken']
        
    return partitions

def get_max_sub_partition_value(
    database_name, 
    table_name
    ):
    """ 
    This function gets the maximum sub-partition value 
    """
    source_partition_list = []
    get_source_partitions = get_glue_partitions(
        database_name,
        table_name
        )

    for source_partition in get_source_partitions:
                source_partition_list.append(source_partition["Values"][1])
    
    maxValue = max(source_partition_list)

    return maxValue

# def lifestyle_segmentation(
#     glueContext : GlueContext, 
#     database_name,
#     table_name,
#     columns_selected
#     ):
    
#     """
#     This function is for the Lifestyle Segmentation data from Data Science Account, 
#     we need to select the max interval_ending and rename columns for the csv file.
#     Data frequency = MONTHLY
#     """
#     # INTERVAL_ENDING is the SUB_PARTITION in the member_lifestyle_segmentation_01 table / partition Key (1)
#     condition_value = get_max_sub_partition_value(database_name, table_name)
#     predicate = "(interval_ending ==" + condition_value + ")"

#     datasource0 = glueContext.create_dynamic_frame.from_catalog(
#         database = database_name, 
#         table_name = table_name, 
#         transformation_ctx = "datasource0",
#         push_down_predicate = predicate
#         )

#     df = datasource0.toDF()
#     for col in df.columns:
#         df = df.withColumnRenamed(col, col.upper())

#     df = df.select("*")\
#     .withColumnRenamed('HCRMEMBER', 'BUSPARTNER')


#     df = df.select("*")\
#     .withColumn(
#     "LIFESTYLE_K9_SEGMENT", 
#     F.when(F.col("LS_SEGMENT") == F.lit(6), F.lit(1)).otherwise(
#         F.when(F.col("LS_SEGMENT") == F.lit(8), F.lit(2)).otherwise(
#             F.when(F.col("LS_SEGMENT") == F.lit(4), F.lit(3)).otherwise(
#                 F.when(F.col("LS_SEGMENT") == F.lit(5), F.lit(4)).otherwise(
#                     F.when(F.col("LS_SEGMENT") == F.lit(3), F.lit(5)).otherwise(
#                         F.when(F.col("LS_SEGMENT") ==F.lit(9), F.lit(6)).otherwise(
#                             F.when(F.col("LS_SEGMENT") == F.lit(7), F.lit(7)).otherwise(
#                                 F.when(F.col("LS_SEGMENT") == F.lit(2), F.lit(8)).otherwise(
#                                     F.when(F.col("LS_SEGMENT") == F.lit(1), F.lit(9)).otherwise(
#                                         F.lit(99)))))))))))

#     df = df.select("*")\
#     .withColumn(
#     "LIFESTYLE_M5_SEGMENT", 
#     F.when(F.col("LS_SEGMENT") == F.lit(6), F.lit(1)).otherwise(
#         F.when(F.col("LS_SEGMENT") == F.lit(8), F.lit(2)).otherwise(
#             F.when(F.col("LS_SEGMENT") == F.lit(4), F.lit(2)).otherwise(
#                 F.when(F.col("LS_SEGMENT") == F.lit(5), F.lit(4)).otherwise(
#                     F.when(F.col("LS_SEGMENT") == F.lit(3), F.lit(4)).otherwise(
#                         F.when(F.col("LS_SEGMENT") == F.lit(9), F.lit(3)).otherwise(
#                             F.when(F.col("LS_SEGMENT") == F.lit(7), F.lit(4)).otherwise(
#                                 F.when(F.col("LS_SEGMENT") == F.lit(2), F.lit(5)).otherwise(
#                                     F.when(F.col("LS_SEGMENT") == F.lit(1), F.lit(5)).otherwise(
#                                         F.lit(99)))))))))))
   
#     df = df.select(columns_selected)
#     # Creates one file for BW Data Services
#     df = df.coalesce(1)
#     csv_delimeter = '|'

#     return df, csv_delimeter

def engagement_segmentation(
    glueContext : GlueContext,
    database_name,
    table_name,
    columns_selected
    ):

    """
    This function is for the Engagement Segmentation data from Data Science Account, 
    we need to select the max interval_ending and rename columns for the csv file.
    Data frequency = WEEKLY
    """
    
    # INTERVAL_ENDING is the SUB_PARTITION in the member_engagement_segmentation_01 table / partition Key (1)
    
    condition_value = get_max_sub_partition_value(database_name, table_name)
    predicate = "(interval_ending ==" + condition_value + ")"

    datasource0 = glueContext.create_dynamic_frame.from_catalog(
        database = database_name, 
        table_name = table_name, 
        transformation_ctx = "datasource0",
        push_down_predicate = predicate
        )

    df = datasource0.toDF()
    for col in df.columns:
        df = df.withColumnRenamed(col, col.upper())
    
    df = df.select("*")\
    .withColumnRenamed('HCRMEMBER', 'BUSPARTNER')\
    .withColumn(
        'ENGAGE_MACRO_SEGMENT',
        F.when(F.col('ENGAGE_MACRO_SEGMENT').isin([0]),F.lit(98))\
    .otherwise(F.col('ENGAGE_MACRO_SEGMENT')))\
    .withColumn('ENGAGE_MICRO_SEGMENT',\
    F.when(F.col('ENGAGE_MICRO_SEGMENT').isin([0]),F.lit(98))\
    .otherwise(F.col('ENGAGE_MICRO_SEGMENT')))
       
        
    df = df.select(columns_selected)
    # Creates one file for BW Data Services
    df = df.coalesce(1)
    csv_delimeter = '|'

    return df, csv_delimeter

def select_all_columns(
    glueContext : GlueContext, 
    database_name,
    table_name,
    columns_selected
    ):

    """
    This function is generic just to select specific columns based on the target table.
    When passing the columns into the parameter it needs to be upper case
    and the columns needs to exist in the dataFrame otherwise the job will fail.
    """
    # Get the max interval_ending is the SUB_PARTITION 
    condition_value = get_max_sub_partition_value(database_name, table_name)
    predicate = "(interval_ending ==" + condition_value + ")"

    datasource0 = glueContext.create_dynamic_frame.from_catalog(
         database = database_name,
         table_name = table_name,
         transformation_ctx = "datasource0",
         push_down_predicate = predicate
        )

    df = datasource0.toDF()
    for col in df.columns:
        df = df.withColumnRenamed(col, col.upper())

    df = df.select(columns_selected)
    # Force all records into 1 csv file.
    df = df.coalesce(1)
    csv_delimeter = '|'

    return df, csv_delimeter

def custom_prep(
    gluecontext: GlueContext,
    database_name,
    table_name,
    columns_selected
    ) -> DataFrame:
    
    if table_name == 'member_engagement_segmentation_01':
        df, csv_delimeter = engagement_segmentation(
            gluecontext,
            database_name,
            table_name,
            columns_selected
            )
    else:
        df, csv_delimeter = select_all_columns(
            gluecontext,
            database_name,
            table_name,
            columns_selected)
        return df, csv_delimeter

    return df, csv_delimeter
