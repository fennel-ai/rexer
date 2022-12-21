""" Script to crawl all the json files written by kafka s3 connector in a day and writing them as a single parquet file.

The script is meant to be run on AWS GLUE.

The script makes the following assumptions:
    1. assumes that the source is structured using UTC timezone (which our kafka is currently configured as)
    2. AND the script is run in the next day i.e. say the data has to be transformed for 04/05/2022, then the script is assumed to be run on 04/06/2022 
    3. assumes that `BUCKET_NAME` and `TIER_ID` are passed as program arguments
    4. writes parquet files in the `day` directory

NOTE: This is potentially a temporary solution till the official support for writing messages using schema in confluent go client.
see: https://coda.io/d/_dby7ZnsqEiy/S3-Connector-v-s-Self-hosted-service_su9yb
"""
from datetime import datetime, timedelta

import sys

import s3fs
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BUCKET_NAME', 'TIER_ID'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()

# since we are interested in the files generated in the previous day,
# subtract few hours from the current time and fetch the yyyy,mm,dd
utc_past = datetime.utcnow() - timedelta(hours=23, minutes=59)

year = utc_past.strftime("%Y")
month = utc_past.strftime("%m")
day = utc_past.strftime("%d")

print(f'======== Reading data from date: year={year}/month={month}/day={day}\n')

# the format of the s3 objects: `s3://{BUCKET_NAME}/daily/t_{TIER_ID}_featurelog/year=2022/month=04/day=05/hour=18/xyz.json`
# read JSON files using wildcard to fetch all the files

# use default credentials which in this case would be derived from GLUE job IAM role which has access to the S3 buckets 
fs = s3fs.S3FileSystem(anon=False)
feature_logs_path = f's3://{args["BUCKET_NAME"]}/daily/t_{args["TIER_ID"]}_featurelog/year={year}/month={month}/day={day}/*/*.json'
if fs.glob(feature_logs_path):
    features_df = spark.read.json(feature_logs_path)
    feature_parquet_path = f's3://{args["BUCKET_NAME"]}/daily/t_{args["TIER_ID"]}_featurelog/year={year}/month={month}/day={day}/features.parquet'
    features_df.write.mode('overwrite').parquet(feature_parquet_path)

action_logs_path = f's3://{args["BUCKET_NAME"]}/daily/t_{args["TIER_ID"]}_actionlog_json/year={year}/month={month}/day={day}/*/*.json'
if fs.glob(action_logs_path):
    actions_df = spark.read.json(action_logs_path)
    actions_parquet_path = f's3://{args["BUCKET_NAME"]}/daily/t_{args["TIER_ID"]}_actionlog_json/year={year}/month={month}/day={day}/actions.parquet'
    actions_df.write.mode('overwrite').parquet(actions_parquet_path)
