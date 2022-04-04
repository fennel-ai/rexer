from aifc import Error
import json
from textwrap import indent

import boto3
import pandas as pd
from pyspark.sql import SparkSession 
from pyarrow import json as arrow_json


_S3_BUCKET = "training-data-feature-log"
_KEY = "s3://training-data-feature-log/feature_logs/daily/t_106_featurelog/year=2022/month=04/day=04/hour=01/t_106_featurelog+0+0000050001.json"
_FILE_PATH = "s3a://training-data-feature-log/feature_logs/daily/t_106_featurelog/year=2022/month=04/day=04/hour=01/*"
_FILE = "t_106_featurelog+0+0000030001.json"
_PARQUET_FILE = "t_106_featurelog+0+0000030001.parquet"


def parse_json(data):
    # Decode UTF-8 bytes to Unicode, and convert single quotes 
    # to double quotes to make it valid JSON
    lists = []
    for j in data.iter_lines():
        lists.append(json.loads(j))
    # Load the JSON to a Python list & dump it back out as formatted JSON
    print(lists)
    print('\n=========\n')


# Reading from JSON to a in-memory store
def read_json_as_arrow():
    table = arrow_json.read_json(_FILE)
    print(f'======= Reading pyarrow json: \n{table.to_pandas()}\n')


# Reading from JSON, writing as Parquet file and reading it using pandas
def read_json_as_parquet():
    # These will need to be configured properly..
    sc = SparkSession.builder.appName("SparkReadJsonFiles")\
            .config("spark.sql.shuffle.partitions", "50")\
            .config("spark.driver.maxResultSize","5g")\
            .config("spark.sql.execution.arrow.pyspark.enabled", "true").getOrCreate()
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAQOLFGTNXEHPEUE4U")
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
    sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.us-west-2.amazonaws.com")

    df = sc.read.json(_FILE_PATH)
    print(f'===== schema of the dataset loaded \n{df.dtypes}\n')

    print('writing to parquet files..')
    df.write.parquet(_PARQUET_FILE, mode="overwrite")

    pd_df = pd.read_parquet(_PARQUET_FILE)
    print(f'===== Reading Parquet file: \n{pd_df}\n')


def get_file(s3_client):
    response = s3_client.get_object(Bucket=_S3_BUCKET, Key=_KEY)
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if status == 200:
        print(f"Successful S3 get_object response. Status - {status}")
        return response.get("Body")
    else:
        raise Error(f'Could not read S3 file: {_KEY}, Status - {status}')


def main():
    s3_client = boto3.client("s3")
    # data = get_file(s3_client=s3_client)
    # parse_json(data=data)
    read_json_as_arrow()
    read_json_as_parquet()
    return


if __name__ == '__main__':
    main()