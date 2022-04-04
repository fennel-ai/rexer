from aifc import Error
import json
from textwrap import indent

import boto3
from pyarrow import json as arrow_json


_S3_BUCKET = "training-data-feature-log"
_KEY = "s3://training-data-feature-log/feature_logs/daily/t_106_featurelog/year=2022/month=04/day=04/hour=01/t_106_featurelog+0+0000050001.json"
_FILE = "t_106_featurelog+0+0000030001.json"


def parse_json(data):
    # Decode UTF-8 bytes to Unicode, and convert single quotes 
    # to double quotes to make it valid JSON
    lists = []
    for j in data.iter_lines():
        lists.append(json.loads(j))
    # Load the JSON to a Python list & dump it back out as formatted JSON
    print(lists)
    print('\n=========\n')


def read_parquet():
    table = arrow_json.read_json(_KEY)
    df = table.to_pandas()
    print(df)


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
    read_parquet()
    return


if __name__ == '__main__':
    main()