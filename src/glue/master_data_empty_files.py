try:
    import sys, os, copy, ast, uuid, boto3, datetime, time, re, json, hashlib
    from awsglue.transforms import *
    from awsglue.transforms import *
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.dynamicframe import DynamicFrame
    from pyspark.sql import functions as F    
    from ast import literal_eval
    from dataclasses import dataclass
    from datetime import datetime
    import math
    import threading
    from dateutil.parser import parse
    from datetime import datetime, timedelta
    from boto3.s3.transfer import TransferConfig
    from dateutil.tz import tzutc
    from datetime import datetime, timezone
    from dateutil.parser import parse
    from dotenv import load_dotenv
    load_dotenv("dev.env")
except Exception as e:
    print(e)

## Replace a given value with NULL in a given column
def replace(column, value):
    return F.when(column != value, column).otherwise(F.lit(None))


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
                    'STZ_TABLE_NAME']
default_optional_args = {
    'PARTITION_KEY': None,
    'PURGE_S3_PATH': None}

# Retrieve args
args = get_glue_args(mandatory_fields, default_optional_args)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

glue_client = boto3.client('glue')


aczTable = glue_client.get_table(DatabaseName=args['ACZ_DATABASE_NAME'], Name=args['ACZ_TABLE_NAME'])

storage_descriptor = aczTable['Table']['StorageDescriptor']

#bookmark

class AWSS3(object):
    """Helper class to which add functionality on top of boto3 """

    def __init__(self, bucket):

        self.BucketName = bucket
        self.client = boto3.client("s3",
                                   aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
                                   aws_secret_access_key=os.getenv("AWS_SECRET_KEY"),
                                   region_name=os.getenv("AWS_REGION")
                                   )

    def put_files(self, Response=None, Key=None):
        """
        Put the File on S3
        :return: Bool
        """
        try:
            response = self.client.put_object(
                Body=Response, Bucket=self.BucketName, Key=self.prefix + Key
            )
            return "ok"
        except Exception as e:
            raise Exception("Error : {} ".format(e))

    def item_exists(self, Key):
        """Given key check if the items exists on AWS S3 """
        try:
            response_new = self.client.get_object(Bucket=self.BucketName, Key=str(Key))
            return True
        except Exception as e:
            return False

    def get_item(self, Key):

        """Gets the Bytes Data from AWS S3 """

        try:
            response_new = self.client.get_object(Bucket=self.BucketName, Key=str(Key))
            return response_new["Body"].read()

        except Exception as e:
            print("Error :{}".format(e))
            return False

    def find_one_update(self, data=None, key=None):

        """
        This checks if Key is on S3 if it is return the data from s3
        else store on s3 and return it
        """

        flag = self.item_exists(Key=key)

        if flag:
            data = self.get_item(Key=key)
            return data

        else:
            self.put_files(Key=key, Response=data)
            return data

    def delete_object(self, Key):

        response = self.client.delete_object(Bucket=self.BucketName, Key=Key, )
        return response

    def get_folder_names(self, Prefix=""):
        """
        :param Prefix: Prefix string
        :return: List of folder names
        """
        try:
            paginator = self.client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self.BucketName, Prefix=Prefix, Delimiter='/')

            folder_names = []

            for page in pages:
                for prefix in page.get('CommonPrefixes', []):
                    folder_names.append(prefix['Prefix'].rstrip('/'))

            return folder_names
        except Exception as e:
            print("error", e)
            return []

    def get_ll_keys_with_meta_data_sorted(self, Prefix="", timestamp=None, sort_order='asc'):
        """
        :param Prefix: Prefix string
        :param timestamp: datetime object
        :param sort_order: 'asc' for ascending, 'desc' for descending
        :return: Sorted keys List with full S3 path
        """
        try:
            paginator = self.client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self.BucketName, Prefix=Prefix)

            tmp = []

            for page in pages:
                for obj in page["Contents"]:
                    last_modified = obj["LastModified"].replace(tzinfo=timezone.utc)
                    if timestamp is None:
                        full_path = f's3://{self.BucketName}/{obj["Key"]}'
                        obj['full_path'] = full_path
                        tmp.append(obj)
                    else:
                        """filter out keys greater than datetime provided """
                        if last_modified > timestamp:
                            # Return full S3 path
                            full_path = f's3://{self.BucketName}/{obj["Key"]}'
                            obj['full_path'] = full_path
                            tmp.append(obj)
                        else:
                            pass
            # Sort the list based on LastModified value
            sorted_tmp = sorted(tmp, key=lambda x: x['LastModified'], reverse=(sort_order == 'desc'))
            return sorted_tmp
        except Exception as e:
            print("error", e)
            return []


class Checkpoints(AWSS3):
    def __init__(self, path):
        self.path = path
        self.bucket = self.path.split("/")[2]

        AWSS3.__init__(
            self, self.bucket
        )
        self.__data = {
            "process_time": datetime.now().__str__(),
            "last_processed_file_name": None,
            "last_processed_time_stamp_of_file": None,
            "last_processed_file_path": None,
            "last_processed_partition": None
        }
        self.prefix = self.path.split(f"{self.bucket}/")[1]
        

        self.filename = f"{hashlib.sha256(str(self.path).encode('utf-8')).hexdigest()}.json"
        self.meta_Data = []
        self.folders = None

    def __get_objects_each_folder(self, folder, timestamp=None):
        for item in self.get_ll_keys_with_meta_data_sorted(Prefix=folder, timestamp=timestamp):
            if item.get("Key").endswith('.csv'):
                self.meta_Data.append(item)

    def read(self):
        if self.checkpoint_exists():
            read_check_points = self.read_check_point()
            print("read_check_points", json.dumps(read_check_points, indent=3))

            timestamp = parse(read_check_points.get("last_processed_time_stamp_of_file")).replace(tzinfo=timezone.utc)

            """Get the folder Names """
            self.folders = self.get_folder_names(Prefix=self.prefix)
            if self.folders != []:
                for folder in self.folders:
                    self.__get_objects_each_folder(folder=folder, timestamp=timestamp)
            else:
                self.__get_objects_each_folder(folder=self.prefix, timestamp=timestamp)

            return self.meta_Data

        else:
            self.folders = self.get_folder_names(Prefix=self.prefix)
            if self.folders != []:
                for folder in self.folders:
                    self.__get_objects_each_folder(folder=folder)
            else:
                self.__get_objects_each_folder(folder=self.prefix)
            return self.meta_Data

    def commit(self):
        if self.meta_Data != []:
            if self.folders != []:
                self.create_check_points(
                    last_processed_time_stamp_of_file=str(self.meta_Data[-1].get("LastModified")),
                    last_processed_file_name=self.meta_Data[-1].get("Key"),
                    last_processed_file_path=self.meta_Data[-1].get("full_path"),
                    last_processed_partition=self.folders[-1]
                )
            else:
                self.create_check_points(
                    last_processed_time_stamp_of_file=str(self.meta_Data[-1].get("LastModified")),
                    last_processed_file_name=self.meta_Data[-1].get("Key"),
                    last_processed_file_path=self.meta_Data[-1].get("full_path"),
                    last_processed_partition=None
                )

    def read_check_point(self):
        return json.loads(self.get_item(Key=self.prefix + self.filename).decode("utf-8"))

    def checkpoint_exists(self):
        return self.item_exists(Key=self.prefix + self.filename)

    def create_check_points(self, last_processed_time_stamp_of_file, last_processed_file_name,
                            last_processed_file_path, last_processed_partition):
        print(self.folders)
        self.__data['last_processed_time_stamp_of_file'] = last_processed_time_stamp_of_file
        self.__data['last_processed_file_name'] = last_processed_file_name
        self.__data['last_processed_partition'] = last_processed_partition

        self.put_files(
            Key=self.filename,
            Response=json.dumps(
                self.__data
            )
        )
        return True

    def delete_checkpoints(self):
        self.delete_object(Key=self.filename)

helper = Checkpoints(path=storage_descriptor['Location'] + "/")
response = helper.read()

helper.commit()

files_to_process =[]
for item in response:
    # print(item.get("full_path", ""))
    files_to_process.append(item.get("full_path", ""))

print(files_to_process)

if len(response) > 0:

    if args['PURGE_S3_PATH'] != None:
        glueContext.purge_s3_path(args['PURGE_S3_PATH'], {"retentionPeriod": 0})
    datasource0 = glueContext.create_dynamic_frame.from_catalog(database = args['ACZ_DATABASE_NAME'], table_name = args['ACZ_TABLE_NAME'], transformation_ctx = "datasource0")


    ## @type: DataSource
    ## @args: [database = "acz-retail-dev", table_name = "site", transformation_ctx = "datasource0"]
    ## @return: datasource0
    ## @inputs: []
    #datasource0 = glueContext.create_dynamic_frame.from_catalog(database = args['ACZ_DATABASE_NAME'], table_name = args['ACZ_TABLE_NAME'], transformation_ctx = "datasource0")

    ## Ensure the data types on the Glue catalogue is set on the data frame to ensure the types are always correct on the parquet files.
    stzTable = glue_client.get_table(DatabaseName=args['STZ_DATABASE_NAME'], Name=args['STZ_TABLE_NAME'])

    resolveChoices = []
    for attribute in stzTable['Table']['StorageDescriptor']['Columns']:
        print(attribute)
        resolveChoices.append((attribute['Name'], "cast:" + attribute['Type']))

    dfResolved = ResolveChoice.apply(datasource0, specs = resolveChoices)

    ## The Glue library always reads empty fields in the CSV as empty strings. Currently there is no way to set this behaviour when reding the CSV files.
    ## We run through all of the columns and replace empty strings with NULL values explicitly. This is done by converting the dynamicframe to a spark frame and then back to a dynamic frame
    df = dfResolved.toDF()

    for dtype in df.dtypes:
        if dtype[1] == "string":
            df = df.withColumn(dtype[0], F.trim(replace(F.col(dtype[0]), "")))

    dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")

    ## @type: ResolveChoice
    ## @args: [choice = "make_struct", transformation_ctx = "resolvechoice2"]
    ## @return: resolvechoice2
    ## @inputs: [frame = dynamic_frame]
    resolvechoice2 = ResolveChoice.apply(frame = dynamic_frame, choice = "make_struct", transformation_ctx = "resolvechoice2")

    additionalOptions = {}

    if args['PARTITION_KEY'] != None:

        sparkDataframe = resolvechoice2.toDF()

        get_table_response = glue_client.get_table(
            DatabaseName=args['STZ_DATABASE_NAME'],
            Name=args['STZ_TABLE_NAME']
        )

        distinct_partition = [x[args['PARTITION_KEY']] for x in sparkDataframe.select(args['PARTITION_KEY']).distinct().collect()]

        for partition in distinct_partition:
            try:
                # Check if the partition already exists. If yes, skip adding it again
                get_partition_response = glue_client.get_partition(
                    DatabaseName=args['STZ_DATABASE_NAME'],
                    TableName=args['STZ_TABLE_NAME'],
                    PartitionValues=[str(partition)]
                )
                print('Glue partition already exists.')

            except Exception as e:

                # Extract the existing storage descriptor and Create custom storage descriptor with new partition location
                storage_descriptor = get_table_response['Table']['StorageDescriptor']
                custom_storage_descriptor = copy.deepcopy(storage_descriptor)
                custom_storage_descriptor['Location'] = storage_descriptor['Location'] + args['PARTITION_KEY'] + "=" + str(partition) + '/'

                # Create new Glue partition in the Glue Data Catalog
                create_partition_response = glue_client.create_partition(
                    DatabaseName=args['STZ_DATABASE_NAME'],
                    TableName=args['STZ_TABLE_NAME'],
                    PartitionInput={
                        'Values': [str(partition)],
                        'StorageDescriptor': custom_storage_descriptor
                    }
                )

        additionalOptions["partitionKeys"] = [args['PARTITION_KEY']]

    sink = glueContext.write_dynamic_frame_from_catalog(frame=resolvechoice2, database = args['STZ_DATABASE_NAME'],
                                                        table_name = args['STZ_TABLE_NAME'], additional_options=additionalOptions, transformation_ctx="write_sink")

    job.commit()