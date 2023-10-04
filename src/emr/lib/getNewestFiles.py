import boto3, re, json
from dateutil.relativedelta import *
from dateutil.rrule import rrule, DAILY
from datetime import date
import argparse

def which_files_to_copy(days_ago, srcURI, destURI):

    # Processed files live in /processed/yyyyMMdd/
    where_processed_files_live = 'processed/[0-9]{8}'

    src = re.match(r's3://([^/]*)/(.*)',srcURI)
    srcBucket = src.group(1)
    srcPrefix = src.group(2)

    dest = re.match(r's3://([^/]*)/(.*)', destURI)
    destBucket = dest.group(1)
    destPrefix = dest.group(2)

    # Start from today
    today = date.today()


    # Now work out how many days to go back to check
    # for new files that may not have already been processed
    days_back = today - relativedelta(days=days_ago)
    #print(days_back)
    
    # Get the last n days (but this excludes today, so we need to include that too
    # hence the append(today))

    # Get the days to check that we may be interested in    
    days_to_check = list(rrule(freq=DAILY, count=days_ago, dtstart=days_back))


    # Append today to the list because we certainly want to process today's file
    days_to_check.append(today)

    #print(f"Days to check list: {days_to_check}")

    # A lamba to convert datetime object into yyyyMMdd in order to do comparisons easly
    # later in the code    
    convert_date = lambda datelist: [f"{d.year}{d.month:02}{d.day:02}" for d in datelist]

    # Now we should have a list of all days that we want to check against
    # Have we processed these dates yet? 
    valid_dates = convert_date(days_to_check)
    #print(f"Converted dates: {valid_dates}")
    
    src_bucket = s3.Bucket(srcBucket)

    # Get a list of all files in the source bucket that fulfill the prefix (bw/td/hrsaw01/ most probably)
    # and that are only DELTAs (DLT) or FULLs (FUL) are selected

    #remove_processed_from_path = re.compile(r'(.*)processed/[0-9]{8}/(.*)')
    remove_processed_from_path = re.compile(r'(.*)processed/.*/(.*)')

    src_objs = [obj.key for obj in src_bucket.objects.filter(Prefix=srcPrefix) if any(f in obj.key for f in ['DLT','FUL'])]
    new_src_objs = []
    for obj in src_objs:
        m = remove_processed_from_path.match(obj)
        try:
            thekey = f"{m.group(1)}{m.group(2)}"
        except:
            thekey=obj 
        finally:
            new_src_objs.append(thekey)
    src_objs = new_src_objs
    #print(f"Objects in the bucket: {src_objs}")
     
    # Deal with the Destination bucket. A little more complex here because 
    # we move the data into a processed and the date of processing
    # so we need to remove all that crud
    
    dest_bucket = s3.Bucket(destBucket)

    # Now we need to check the destination bucket and remove the processed/yyyyMMdd/ path 
    dest_objs = []
    

    for obj in dest_bucket.objects.filter(Prefix=destPrefix):
        # If the file is DLT/FUL and it's not in any processed bucket then add it as a potential of 
        # files that should be processed
        if 'processed' not in obj.key: #and any(f in obj.key for f in ['DLT','FUL']):
            dest_objs.append(obj.key)
        # otherwise, if it's a DLT/FUL file, then add it as a potential file to process
        # but remove the processed/yyyyMMdd/ from the filename
        #elif any(f in obj.key for f in ['DLT','FUL']):
        else:
            m = remove_processed_from_path.match(obj.key)
            thekey = f"{m.group(1)}{m.group(2)}"
            dest_objs.append(thekey)

    #print(f"Objects in the DESTINATION bucket: {dest_objs}")
    # Get the resulting list of objects we will copy
    # by subtracting the files destination bucket (including processed files) from
    # the list of files in the source
    to_copy = list(set(src_objs) - set(dest_objs))

    #print(f"Files to copy: {to_copy}")
    #print(src_objs, "\n\n" ,dest_objs)

    dlt_date_of_extract = re.compile(r'.*_DLT_([0-9]{8}).*\.csv')
    ful_date_of_extract = re.compile(r'.*_FUL_([0-9]{8})_[0-9]{14}.*\.csv')

    # BW_HRSMW01XX_FUL_B100toBW1Y_20210802_20210802094134.csv
    sitearticle_date_of_extract = re.compile(r'.*_FUL_[^_]*_([0-9]{8})_[0-9]{14}.*\.csv')


    # Now only keep those files that are within the
    # set of valid_dates that I want to process over
    # which will normally be 7 days.
    # After 7 days the destination bucket may no longer have files that will be in the
    # original source bucket 


    #print(f"Valid dates: {valid_dates}")
    files_to_keep = []

    for file in to_copy:
        try:
            dd = dlt_date_of_extract.match(file).group(1)
            if dd in valid_dates:
                    #print(f"{dd} day discovered\t\tKeeping: {file} length of files_to_keep {len(files_to_keep)}")
                    files_to_keep.append(file)
        except AttributeError as attribErr:
            #print(f"There are no DLTs in the bucket; {attribErr}")
            try:
                dd = ful_date_of_extract.match(file).group(1)
                if dd in valid_dates:
                    #print(f"{dd} day discovered\t\tKeeping: {file} length of files_to_keep {len(files_to_keep)}")
                    files_to_keep.append(file)
            except AttributeError as attribErr:
                dd = sitearticle_date_of_extract.match(file).group(1)
                if dd in valid_dates:
                    #print(f"{dd} day discovered\t\tKeeping: {file} length of files_to_keep {len(files_to_keep)}")
                    files_to_keep.append(file)
                    #print(f"This {file} is an incorrect format")

    return files_to_keep

def copy_files(filelist, srcURI, destURI):

#    my_session = boto3.session.Session(profile_name='pnp-emr-1')
#    s3 = my_session.resource('s3')

    dest = re.match(r's3://([^/]*)/(.*)', destURI)
    destBucket = dest.group(1)
 
#    dest_bucket = s3.Bucket(destBucket)
    
    src = re.match(r's3://([^/]*)/(.*)',srcURI)
    srcBucket = src.group(1)

   # src_bucket = s3.Bucket(srcBucket)
   # dest_bucket = s3.Bucket(destBucket)
    
    for s3_file in filelist:
        print(f"{s3_file} {destURI}")
        #s3_copy_result = s3.meta.client.copy_object(Bucket=destBucket, Key=s3_file, CopySource=f"{srcBucket}/{s3_file}", MetadataDirective='COPY')
        #if (s3_copy_result['ResponseMetadata']['HTTPStatusCode'] != 200 or s3_copy_result['CopyObjectResult'] != None):
        #    print(f"Copy of file {s3_file} to {destURI} done")

if __name__ == '__main__':

    """
        This script will look at the source bucket/prefix, as well as the destination bucket/prefix. The thing is,
        in the destination bucket/prefix the files are moved to processed/yyyyMMdd/ once they have been processed
        so this script will removed the processed/yyyyMMdd from the destination files to determine whether
        the files from the source have already been done.

        Additionally, the customer does not want to keep those processed files forever, so after a week they are deleted
        and so if the files are NOT in the destination, but are still in the source, we want to limit comparing source and
        destination to X days of each other. Thus if a file is not in the destination, but it's been more than a week then 
        we want to ignore those files.
    """

    parser = argparse.ArgumentParser()
    
    parser.add_argument("--sourceURI", "-s", help="The source URI to read these data")
    parser.add_argument("--destinationURI", "-d", help="The destination URI for these data")
    parser.add_argument("--age", "-a", help="How many days we need to look back in time. We ignore files older than this number of days")
    parser.add_argument("--dryrun", "-dry", default=True, help="Just tell us what you're going to do, don't do it")
    
    args = parser.parse_args()
    
    srcURI = args.sourceURI
    destURI = args.destinationURI
    daysAgo = int(args.age)

    my_session = boto3.session.Session()
    s3 = my_session.resource('s3')
    files = which_files_to_copy(daysAgo, srcURI, destURI)
    if args.dryrun:
        # Get the source bucket name from the srcURI
        src = re.match(r's3://([^/]*)/.*',srcURI)
        srcBucket = src.group(1)

        # Get the destination bucket name from the srcURI
        dest = re.match(r's3://([^/]*)/.*', destURI)
        destBucket = dest.group(1)

        # Since this is a dry-run, simply print what would be done
        # but don't do anything
        for f in files: 
            print(f"s3://{srcBucket}/{f} s3://{destBucket}/{f}")
    else:
        copy_files(files, srcURI, destURI)
