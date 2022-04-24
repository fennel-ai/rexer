from datetime import datetime, timedelta, timezone

import sys
import json
import s3fs
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.functions import col, collect_list, array_join

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'PLANE_ID', 'TIER_ID', 'AGGREGATE_NAME', 'DURATION', 'AGGREGATE_TYPE', 'PARAMS'])
print("All args", args)
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

utc_past = datetime.utcnow() - timedelta(hours=23, minutes=59)

year = utc_past.strftime("%Y")
month = utc_past.strftime("%m")
day = utc_past.strftime("%d")

params = json.loads(args['PARAMS'])

print(f'======== Reading data from date: year={year}/month={month}/day={day}\n')
# the format of the s3 objects: `s3://p-{PLANE_ID}-training-data/daily/t_{TIER_ID}_featurelog/year=2022/month=04/day=05/hour=18/xyz.json`
# read JSON files using wildcard to fetch all the files

# use default credentials which in this case would be derived from GLUE job IAM role which has access to the S3 buckets 
fs = s3fs.S3FileSystem(anon=False)
transformed_actions_path = f's3://offlineaggregatestorage/topics/t_{args["TIER_ID"]}_aggr_offline_transform/year={year}/month={month}/*/*/*.json'
if not fs.glob(transformed_actions_path):
    print("No data found for the given date")

actions = spark.read.json(transformed_actions_path)

now_utc = datetime.now(timezone.utc)
lower_time_bound = int(now_utc.timestamp()) - int(args['DURATION'])
time_filter = "timestamp>{}".format(lower_time_bound)
actions = actions.filter("aggregate='{}'".format(args["AGGREGATE_NAME"]))
actions = actions.filter(time_filter)
ca_df = actions.withColumn("key", F.col("value.key")).withColumn("score", F.col("value.score")).select("key", "score", "groupkey")
ca_df.createOrReplaceTempView("ACTIONS")

sql_str = """
select groupkey, collect_list(k) as topk
from (
  select groupkey, key as k, total_score,
  rank() over (PARTITION by groupkey order by total_score) as rank
  from (
      select groupkey, key, sum(score) as total_score
      from ACTIONS
      group by groupkey, key
  )
)
where rank < {}
group by groupkey
""".format(params["k"])

topk = spark.sql(sql_str)
topk_aggregate_path = f's3://offline-aggregate-output/t_{args["TIER_ID"]}/{args["AGGREGATE_NAME"]}/{args["AGGREGATE_TYPE"]}.json'
topk.write.mode('overwrite').parquet(topk_aggregate_path)

job.commit()
