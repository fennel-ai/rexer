""" Script to crawl all the json files written by kafka s3 connector in a day and writing them as a single parquet file.

The script is meant to be run on AWS GLUE.

The script makes the following assumptions:
    1. assumes that the source is structured using UTC timezone (which our kafka is currently configured as)
    2. AND the script is run in the next day i.e. say the data has to be transformed for 04/05/2022, then the script is assumed to be run on 04/06/2022 
    3. assumes that `PLANE_ID` and `TIER_ID` are passed as program arguments
    4. writes parquet files in the `day` directory

NOTE: This is potentially a temporary solution till the official support for writing messages using schema in confluent go client.
see: https://coda.io/d/_dby7ZnsqEiy/S3-Connector-v-s-Self-hosted-service_su9yb
"""
from datetime import datetime, timedelta

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'PLANE_ID', 'TIER_ID'])

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

# the format of the s3 objects: `s3://p-{PLANE_ID}-training-data/daily/t_{TIER_ID}_featurelog/year=2022/month=04/day=05/hour=18/xyz.json`
# read JSON files using wildcard to fetch all the files
feature_logs_path = f's3://p-{args["PLANE_ID"]}-training-data/daily/t_{args["TIER_ID"]}_featurelog/year={year}/month={month}/day={day}/*/*.json'
action_logs_path = f's3://p-{args["PLANE_ID"]}-training-data/daily/t_{args["TIER_ID"]}_actionlog_json/year={year}/month={month}/day={day}/*/*.json'

features_df = spark.read.json(feature_logs_path)
actions_df = spark.read.json(action_logs_path)

feature_parquet_path = f's3://p-{args["PLANE_ID"]}-training-data/daily/t_{args["TIER_ID"]}_featurelog/year={year}/month={month}/day={day}/features.parquet'
actions_parquet_path = f's3://p-{args["PLANE_ID"]}-training-data/daily/t_{args["TIER_ID"]}_actionlog_json/year={year}/month={month}/day={day}/actions.parquet'

# write to parquet in the same day directory and overwrite existing file (if one exists)
features_df.write.mode('overwrite').parquet(feature_parquet_path)
actions_df.write.mode('overwrite').parquet(actions_parquet_path)
