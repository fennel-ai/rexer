# This file is meant for being uploading into S3 as a script and create a job out of it. 
# CreateTrigger will then schedule these jobs according to the cron schedule specified by the user. 

from datetime import datetime, timedelta, timezone

import sys
import json
import s3fs
import boto3

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.functions import col, collect_list, array_join
from pyspark.sql.functions import arrays_zip

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'INPUT_BUCKET', 'OUTPUT_BUCKET', 'TIER_ID', 'AGGREGATE_NAME', 'DURATION', 'AGGREGATE_TYPE', 'LIMIT'])
print("All args", args)
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()

utc_past = datetime.utcnow() - timedelta(hours=23, minutes=59)

year = utc_past.strftime("%Y")
month = utc_past.strftime("%m")
day = utc_past.strftime("%d")

print(f'======== Reading data from date: year={year}/month={month}/day={day}\n')
# read JSON files using wildcard to fetch all the files

# use default credentials which in this case would be derived from GLUE job IAM role which has access to the S3 buckets 
fs = s3fs.S3FileSystem(anon=False)
transformed_actions_path = f's3://{args["INPUT_BUCKET"]}/daily/t_{args["TIER_ID"]}_aggr_offline_transform/year={year}/month={month}/*/*/*.json'
if not fs.glob(transformed_actions_path):
    print("No data found for the given date")

actions = spark.read.json(transformed_actions_path)

now_utc = datetime.now(timezone.utc)
lower_time_bound = int(now_utc.timestamp()) - int(args['DURATION'])
time_filter = "timestamp>{}".format(lower_time_bound)
actions = actions.filter("aggregate='{}'".format(args["AGGREGATE_NAME"]))
actions = actions.filter(time_filter)
ca_df = actions.withColumn("key", F.col("value.item")).withColumn("score", F.col("value.score")).select("key", "score", "groupkey")
ca_df.createOrReplaceTempView("ACTIONS")

sql_str = """
select groupkey as key, collect_list(key) as item, collect_list(total_score) as score
from (
  select cast(groupkey AS string), cast(key as string), cast(total_score as double),
  rank() over (partition by groupkey order by total_score desc) as rank
  from (
      select groupkey, key, sum(score) as total_score
      from ACTIONS
      group by groupkey, key
  )
)
where rank < {}
group by groupkey
""".format(args["LIMIT"])

topk = spark.sql(sql_str)
zip_topk = topk.withColumn("item_list", arrays_zip("item","score")).select("key","item_list")

folder_name = f'{args["AGGREGATE_NAME"]}-{args["DURATION"]}'

topk_aggregate_path = f's3://{args["OUTPUT_BUCKET"]}/t_{args["TIER_ID"]}/{folder_name}/day={day}/{now_utc.strftime("%H:%M")}/{args["AGGREGATE_TYPE"]}'
zip_topk.write.mode('overwrite').parquet(topk_aggregate_path)

# Write SUCCESS file to S3
client = boto3.client('s3')
some_binary_data = b'data'
cur_timestamp = int(datetime.utcnow().timestamp())
client.put_object(Body=some_binary_data, Bucket=args["OUTPUT_BUCKET"], Key=f't_{args["TIER_ID"]}/{folder_name}/day={day}/{now_utc.strftime("%H:%M")}/{args["AGGREGATE_TYPE"]}/_SUCCESS-{cur_timestamp}')
