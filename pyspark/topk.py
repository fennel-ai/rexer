# This file is meant for being uploading into S3 as a script and create a job out of it. 
# CreateTrigger will then schedule these jobs according to the cron schedule specified by the user. 

from datetime import date, datetime, timedelta, timezone

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
from pyspark.sql.types import IntegerType
from functools import reduce  # For Python 3.x
from pyspark.sql import DataFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv,
    ['JOB_NAME', 'INPUT_BUCKET', 'OUTPUT_BUCKET', 'TIER_ID', 'AGGREGATE_NAME',
     'DURATION', 'AGGREGATE_TYPE', 'LIMIT'])
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

now_utc = datetime.now(timezone.utc)
lower_time_bound = int(now_utc.timestamp()) - int(args['DURATION'])
time_filter = "timestamp>{}".format(lower_time_bound)

transformed_actions_prefix = f's3://{args["INPUT_BUCKET"]}/daily/t_{args["TIER_ID"]}_aggr_offline_transform'
paths = []
day = 0
fs = s3fs.S3FileSystem(anon=False)
while True:
    past_day = date.today() - timedelta(days=day)
    pt = datetime(past_day.year, past_day.month, past_day.day)
    # One day buffer for safety
    if pt.timestamp() < lower_time_bound - 86400:
        break
    day += 1

    d = f'{past_day.day}'
    if past_day.day < 10:
        d = f'0{past_day.day}'

    m = f'{past_day.month}'
    if past_day.month < 10:
        m = f'0{past_day.month}'

    path = f'{transformed_actions_prefix}/year={past_day.year}/month={m}/day={d}/*/*.json'
    if not fs.glob(path):
        print("No data found for path", path)
    else:
        paths.append(path)


def unionAll(dfs):
    return reduce(DataFrame.unionAll, dfs)


dfs = []
print("Going to union")
print(paths)
for path in paths:
    filtered_df = spark.read.json(path).filter(
        "aggregate='{}'".format(args["AGGREGATE_NAME"]))
    if filtered_df.count() != 0:
        dfs.append(filtered_df)
actions = unionAll(dfs)
print("Union post filtering is done:", actions.count())

actions = actions.filter(time_filter)
print("Time filter: ", actions.count())
if actions.filter(col("groupkey").cast("int").isNull()).count() == 0:
    actions = actions.withColumn("groupkey",
        actions["groupkey"].cast(IntegerType()))

ca_df = actions.withColumn("key", F.col("value.item")).withColumn("score",
    F.col("value.score")).select("key", "score", "groupkey")

print("Num rows: ", ca_df.count())
ca_df.createOrReplaceTempView("ACTIONS")

sql_str = """
select groupkey as key, collect_list(key) as item, collect_list(total_score) as score
from (
  select groupkey, key, cast(total_score as double),
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
zip_topk = topk.withColumn("value", arrays_zip("item", "score")).select("key",
    "value")

folder_name = f'{args["AGGREGATE_NAME"]}-{args["DURATION"]}'

cur_time = datetime.utcnow()
month = cur_time.strftime("%m")
day = cur_time.strftime("%d")

topk_aggregate_path = f's3://{args["OUTPUT_BUCKET"]}/t_{args["TIER_ID"]}/{folder_name}/month={month}/day={day}/{now_utc.strftime("%H:%M")}/{args["AGGREGATE_TYPE"]}'
zip_topk.write.mode('overwrite').json(topk_aggregate_path)

# Write SUCCESS file to S3
client = boto3.client('s3')
some_binary_data = b'data'
cur_timestamp = int(cur_time.timestamp())
client.put_object(Body=some_binary_data, Bucket=args["OUTPUT_BUCKET"],
    Key=f't_{args["TIER_ID"]}/{folder_name}/month={month}/day={day}/{now_utc.strftime("%H:%M")}/{args["AGGREGATE_TYPE"]}/_SUCCESS-{cur_timestamp}')
