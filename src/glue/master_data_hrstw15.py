import sys
import boto3
import copy
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job


def get_glue_args(mandatory_fields, default_optional_args):
    # The glue args are available in sys.argv with an extra '--'
    given_optional_fields_key = list(set([i[2:] for i in sys.argv]).intersection([i for i in default_optional_args]))

    args = getResolvedOptions(sys.argv,
                              mandatory_fields+given_optional_fields_key)

    # Overwrite default value if optional args are provided
    default_optional_args.update(args)

    return default_optional_args


# Defining mandatory/optional args
mandatory_fields = ['JOB_NAME',
                    'ACZ_DATABASE_NAME',
                    'ACZ_TABLE_NAME',
                    'STZ_DATABASE_NAME',
                    'STZ_TABLE_NAME',
                    'PURGE_S3_PATH']
default_optional_args = {'PARTITION_KEY': None}

# Retrieve args
args = get_glue_args(mandatory_fields, default_optional_args)

partitionKeys = ["calday"]


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
glue_client = boto3.client('glue')

datasource0 = glueContext.create_dynamic_frame.from_catalog(database = args['ACZ_DATABASE_NAME'], table_name = args['ACZ_TABLE_NAME'], transformation_ctx = "datasource0")

sparkDataframe = datasource0.toDF()

get_table_response = glue_client.get_table(
    DatabaseName=args['STZ_DATABASE_NAME'],
    Name=args['STZ_TABLE_NAME']
)

distinct_calday = [x.calday for x in sparkDataframe.select('calday').distinct().collect()]

for calday in distinct_calday:
    glueContext.purge_s3_path(args['PURGE_S3_PATH'] + str(calday) + "/", {"retentionPeriod": 0})

    try:
        # Check if the partition already exists. If yes, skip adding it again
        get_partition_response = glue_client.get_partition(
            DatabaseName=args['STZ_DATABASE_NAME'],
            TableName=args['STZ_TABLE_NAME'],
            PartitionValues=[str(calday)]
        )
        print('Glue partition already exists.')

    except Exception as e:

        # Extract the existing storage descriptor and Create custom storage descriptor with new partition location
        storage_descriptor = get_table_response['Table']['StorageDescriptor']
        custom_storage_descriptor = copy.deepcopy(storage_descriptor)
        custom_storage_descriptor['Location'] = storage_descriptor['Location'] + "calday=" + str(calday) + '/'

        # Create new Glue partition in the Glue Data Catalog
        create_partition_response = glue_client.create_partition(
            DatabaseName=args['STZ_DATABASE_NAME'],
            TableName=args['STZ_TABLE_NAME'],
            PartitionInput={
                'Values': [str(calday)],
                'StorageDescriptor': custom_storage_descriptor
            }
        )

#df2 = ResolveChoice.apply(datasource0, specs = [("hsvtqtybu", "cast:decimal(10,3)"), ("hsvtqtyou", "cast:decimal(10,3)")])

additionalOptions = {"enableUpdateCatalog": False}
additionalOptions["partitionKeys"] = partitionKeys


sink = glueContext.write_dynamic_frame_from_catalog(frame=datasource0, database = args['STZ_DATABASE_NAME'],
                                                    table_name = args['STZ_TABLE_NAME'], transformation_ctx="write_sink",
                                                    additional_options=additionalOptions)
job.commit()
