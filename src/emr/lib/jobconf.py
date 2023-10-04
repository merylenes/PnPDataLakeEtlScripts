#!/usr/bin/env python3

import json, os, sys
import argparse

def sparkConf(sparkJsonConf):
    conf=""
    for k, v in sparkJsonConf['conf'].items():
        thisconf = f"--conf {k}={v}"
        conf = thisconf + " " + conf
    return conf.rstrip()

def sparkJars(sparkJsonConf):
    jars = "--jars " + ",".join(sparkJsonConf['jars'])
    return(jars)

if __name__ == '__main__':

    """
        This script is a helper script for the step.sh to be able to parse the JSON file
        and create environment variables that can be used by step.sh to run the codes
    """

    parser = argparse.ArgumentParser()
    
    parser.add_argument("--jobconfJson", "-j", help="The JSON file specifying the dataset. This should be the jobconfig.json path included")
    parser.add_argument("--dataset", "-d", help="The dataset you need to work on in this iteration (could be pos, forcasting, sales, etc.")
    
    args = parser.parse_args()

    with open(args.jobconfJson) as f:
        jobJson = json.load(f)
    f.close()

    thisJob = jobJson[args.dataset]
    #print(f"{thisJob}")

    env_vars={}
    env_vars['DATASET']=str(args.dataset)
    env_vars['DATASET_NAME']=args.dataset.upper()
    env_vars['DESC']=thisJob["description"]

    stage = thisJob["dataset_stage"]
    env_vars['STAGE'] = stage

    try:
        env_vars['SNOWFLAKE'] = thisJob[stage]["snowflake"]
    except KeyError as kErr:
        None

    env_vars['SPARK_JOB'] = thisJob["spark_job"]

    #jsonConf = json.loads(jobJson)

    if stage == 'dev':
        env_vars['SRC_ACZ_BUCKET'] = thisJob["prod"]["bucket"]["acz"]
        env_vars['DST_ACZ_BUCKET'] = thisJob["dev"]["bucket"]["acz"]
        env_vars['ACZ_PREFIX'] = thisJob["acz_prefix"]
        env_vars['DAYS'] = thisJob[stage]["data_age"]
        env_vars['SRC_ACZ_S3_URI'] = f"s3://{env_vars['SRC_ACZ_BUCKET']}/{env_vars['ACZ_PREFIX']}"
        env_vars['DST_ACZ_S3_URI'] = f"s3://{env_vars['DST_ACZ_BUCKET']}/{env_vars['ACZ_PREFIX']}"

    stz_prefix = thisJob["stz_prefix"]
    stz_bucket = thisJob[stage]["bucket"]["stz"]
    env_vars['STZ_S3'] = f"s3://{stz_bucket}/{stz_prefix}/"

    acz_processed_prefix = thisJob['processed_prefix']
    acz_prefix = thisJob['acz_prefix']
    acz_bucket = thisJob[stage]["bucket"]['acz']
    env_vars['ACZ_S3'] = f"s3://{acz_bucket}/{acz_prefix}/"
    env_vars['ACZ_S3_PROCESSED'] = f"{env_vars['ACZ_S3']}{acz_processed_prefix}/"

    env_vars['STZ_DB'] = thisJob[stage]["db"]
    env_vars['STZ_TABLE'] = thisJob["table"]

    # Spark configs and jars from the spark key
    try:
        spark = thisJob["spark"]
    except KeyError as kErr:
        None 
    else:
        conf=sparkConf(spark)
        jars = sparkJars(spark)
        print(f"export SPARKCONF='{conf}';")
        print(f"export SPARKJARS='{jars}';")

for k,v in env_vars.items():
    print(f"export {k}={v};")