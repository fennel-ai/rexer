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
from pyspark.sql.functions import arrays_zip, concat_ws

success_file_name = "_SUCCESS-"
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'INPUT_BUCKET', 'OUTPUT_BUCKET', 'TIER_ID', 'AGGREGATE_NAME', 'DURATION', 'AGGREGATE_TYPE', 'LIMIT', 'HYPERPARAMETERS'])
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

params = json.loads(args["HYPERPARAMETERS"])
print("Hyperparameters used - ", params)

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


actions = actions.withColumn("vlist", F.col("value.context")).withColumn("weight", F.col("value.weight")).withColumn("context", concat_ws("::", "vlist")).select("groupkey", "context", "weight")
actions.createOrReplaceTempView("ACTIONS")

# Condense actions
sql_str = """
select groupkey, context, sum(weight) as weight
from ACTIONS
group by groupkey, context
"""
actions = spark.sql(sql_str)

## Overall idea - 
## P(O1, O2) = Sum across Movies P(O1 | C) * P(C | O2 ) = P(C, O)/P(C) * P(O2,C) / P(O2)



## Sum of weight for objects -> P(O)

if params["object_normalization_func"] == "none":
    normalization_func = ""
else:
    normalization_func = params["object_normalization_func"]
sql_str = """
select groupkey, p_obj from
(
select groupkey, count(context) as obj_cnt, {}(DOUBLE(sum(weight))) as p_obj
from ACTIONS
group by groupkey
) where obj_cnt > {}
""".format(normalization_func, params["min_co_occurence"])
p_obj = spark.sql(sql_str)
p_obj.createOrReplaceTempView("P_OBJ")

## Sum of weight for context -> P(OBJ)

sql_str = """
select * from
(
select context, DOUBLE(sum(weight)) as p_context
from ACTIONS
group by context
)
"""
p_context = spark.sql(sql_str)
p_context.createOrReplaceTempView("P_CONTEXT")

## O2C P(O1, C)/P(C) (  Normalized by Context )

sql_str = """
select  a.context, a.groupkey , weight/b.p_context as p_o2c
from ACTIONS as a
inner join P_CONTEXT as b
ON a.context = b.context
"""

p_o2c = spark.sql(sql_str)
p_o2c.createOrReplaceTempView("P_O2C")

## C2O P(O2,C) / P(O2) ( Normalized by Object)

sql_str = """

select a.context, a.groupkey, weight/b.p_obj as p_c2o
from ACTIONS as a
inner join P_OBJ as b
ON a.groupkey = b.groupkey

"""

p_c2o = spark.sql(sql_str)
p_c2o.createOrReplaceTempView("P_C2O")


## Cross Join within context and spit out O2O pairs 

sql_str = """

select o1, o2, sum(p) as score
from (
select a.groupkey as o1, b.groupkey as o2, p_o2c * p_c2o as p
from P_O2C as a
join P_C2O as b
ON a.context = b.context
where a.groupkey != b.groupkey
) 
group by o1, o2
"""

cross_join = spark.sql(sql_str)
cross_join.createOrReplaceTempView("CROSS_JOIN_CONTEXT")

## Group By Tag and list the other tags in decreasing order of probability

sql_str = """

select o1 as groupkey, collect_list(o2) as item, collect_list(score) as score
from(
    select cast(o1 as string), casta(o2 as string), cast(score as double),
    rank() over (PARTITION by o1 order by score desc) as rank
    from CROSS_JOIN_CONTEXT
)
where rank < {}
group by o1
""".format(args["LIMIT"])

cf = spark.sql(sql_str)
cf.createOrReplaceTempView("CF")

zip_cf = cf.withColumn("item_list", arrays_zip("item","score")).select("groupkey","item_list")
folder_name = f'{args["AGGREGATE_NAME"]}-{args["DURATION"]}'

aggregate_path = f's3://{args["OUTPUT_BUCKET"]}/t_{args["TIER_ID"]}/{folder_name}/day={day}/{now_utc.strftime("%H:%M")}/{args["AGGREGATE_TYPE"]}'
zip_cf.write.mode('overwrite').parquet(aggregate_path)

s3 = boto3.resource('s3')
cur_timestamp = int(datetime.utcnow().timestamp())
object = s3.Object({args["OUTPUT_BUCKET"]}, f't_{args["TIER_ID"]}/{folder_name}/day={day}/{now_utc.strftime("%H:%M")}/{args["AGGREGATE_TYPE"]}/_SUCCESS-'+str(cur_timestamp))
object.put(Body=str(cur_timestamp))

# Write SUCCESS file to S3
client = boto3.client('s3')
some_binary_data = b'Here we have some data'
cur_timestamp = int(datetime.utcnow().timestamp())
client.put_object(Body=some_binary_data, Bucket=args["OUTPUT_BUCKET"], Key=f't_{args["TIER_ID"]}/{folder_name}/day={day}/{now_utc.strftime("%H:%M")}/{args["AGGREGATE_TYPE"]}/_SUCCESS-{cur_timestamp}')
