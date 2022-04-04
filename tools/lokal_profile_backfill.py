import os
import sys
from typing import List

import argparse
from venv import create
import boto3
import datetime
import dateutil.parser as dp 
import pandas as pd
from tqdm import tqdm

from rexerclient import client as rexclient
from rexerclient.models import profile


parser = argparse.ArgumentParser()
parser.add_argument("--s3_bucket",
    help="S3 bucket where the csv files are stored and should be written to.")
# users
parser.add_argument("--users_path",
    help="path to the csv file to load the user profile data from."
    "If the file is being read from S3 bucket, this is the path to the object in the bucket."
    "Assumes this file exists.")
# posts
parser.add_argument("--post_categories_path",
    help="path to the csv file to load post categories from."
    "If the file is being read from S3 bucket, this is the path to the object in the bucket."
    "Assumes this file exists.")
parser.add_argument("--post_constituencies_path",
    help="path to the csv file to load post constituencies from."
    "If the file is being read from S3 bucket, this is the path to the object in the bucket."
    "Assumes this file exists.")
parser.add_argument("--post_districts_path",
    help="path to the csv file to load post districts from."
    "If the file is being read from S3 bucket, this is the path to the object in the bucket."
    "Assumes this file exists.")
parser.add_argument("--post_states_path",
    help="path to the csv file to load post states from."
    "If the file is being read from S3 bucket, this is the path to the object in the bucket."
    "Assumes this file exists.")
parser.add_argument("--post_updated_on_path",
    help="path to the csv file to load post updated_at from."
    "If the file is being read from S3 bucket, this is the path to the object in the bucket."
    "Assumes this file exists.")
parser.add_argument("--post_tags_path",
    help="path to the csv file to load post tags from."
    "If the file is being read from S3 bucket, this is the path to the object in the bucket."
    "Assumes this file exists.")

args = parser.parse_args()

_S3_BUCKET = args.s3_bucket
# users
_USERS_PATH: os.PathLike = args.users_path
# posts
_POST_CATEGORIES_PATH: os.PathLike = args.post_categories_path
_POST_CONSTITUENCIES_PATH: os.PathLike = args.post_constituencies_path
_POST_DISTRICTS_PATH: os.PathLike = args.post_districts_path
_POST_STATES_PATH: os.PathLike = args.post_states_path
_POST_UPDATED_ON_PATH: os.PathLike = args.post_updated_on_path
_POST_TAGS_PATH: os.PathLike = args.post_tags_path

_USER_OTYPE = "users"
_USER_DISTRICT = "district"
_USER_CONSTITUENCY = "constituency"
_USER_CREATED_ON = "created_on"

_POST_OTYPE = "posts"
_POST_CATEGORIES = "categories"
_POST_CONSTITUENCIES = "constituencies"
_POST_DISTRICTS = "districts"
_POST_STATES = "states"
_POST_TAGS = "tags"
_POST_UPDATED_AT = "updated_at"
_POST_CREATED_AT = "created_at"

_URL = "http://k8s-t106-aest106e-8954308bfc-65423d0e968f5435.elb.us-west-2.amazonaws.com/data"

_CSV_EXTENSION = ".csv"


def get_csv(s3_client: boto3.client, file_path: str) -> pd.DataFrame:
    file_path = file_path + _CSV_EXTENSION
    if _S3_BUCKET:
        response = s3_client.get_object(Bucket=_S3_BUCKET, Key=file_path)
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == 200:
            print(f"Successful S3 get_object response. Status - {status}")
            return pd.read_csv(response.get("Body"))
        else:
            print(f"Could not read S3 file: {file_path}, Status - {status}")
    else:
        return pd.read_csv(filepath_or_buffer=file_path)


def users(s3_client: boto3.client, batch_size: int = 10000) -> List[profile.Profile]:
    users_df = get_csv(s3_client=s3_client, file_path=_USERS_PATH)
    total_entries = len(users_df)
    profiles = []
    with tqdm(total = total_entries, file = sys.stdout) as pbar:
        for row in users_df.itertuples(index=False):
            oid = getattr(row, 'id')

            district = getattr(row, 'location_id')
            if not pd.isnull(district):
                profiles.append(profile.Profile(otype=_USER_OTYPE, oid=oid, key=_USER_DISTRICT, value=district))

            constituency = getattr(row, 'microlocation_id')
            if not pd.isnull(constituency):
                profiles.append(profile.Profile(otype=_USER_OTYPE, oid=oid, key=_USER_CONSTITUENCY, value=constituency))

            created_on = getattr(row, 'created_on')
            if not pd.isnull(created_on):
                profiles.append(profile.Profile(otype=_USER_OTYPE, oid=oid, key=_USER_CREATED_ON, value=dp.parse(created_on).timestamp()))

            if len(profiles) > batch_size:
                yield profiles
                del profiles[:]
            pbar.update(1)

    yield profiles


def posts_categories(s3_client: boto3.client, batch_size: int = 10000) -> List[profile.Profile]:
    posts_df = get_csv(s3_client=s3_client, file_path=_POST_CATEGORIES_PATH)
    total_entries = len(posts_df)
    profiles = []
    with tqdm(total = total_entries, file = sys.stdout) as pbar:
        for row in posts_df.itertuples(index=False):
            oid = getattr(row, 'post_id')
            categories = getattr(row, 'categories')
            if not pd.isnull(categories):
                profiles.append(profile.Profile(otype=_POST_OTYPE, oid=oid, key=_POST_CATEGORIES, value=categories))

            if len(profiles) > batch_size:
                yield profiles
                del profiles[:]
            pbar.update(1)

    yield profiles


def posts_constituencies(s3_client: boto3.client, batch_size: int = 10000) -> List[profile.Profile]:
    posts_df = get_csv(s3_client=s3_client, file_path=_POST_CONSTITUENCIES_PATH)
    total_entries = len(posts_df)
    profiles = []
    with tqdm(total = total_entries, file = sys.stdout) as pbar:
        for row in posts_df.itertuples(index=False):
            oid = getattr(row, 'post_id')
            constituencies = getattr(row, 'constituencies')
            if not pd.isnull(constituencies):
                profiles.append(profile.Profile(otype=_POST_OTYPE, oid=oid, key=_POST_CONSTITUENCIES, value=constituencies))

            if len(profiles) > batch_size:
                yield profiles
                del profiles[:]
            pbar.update(1)

    yield profiles


def posts_locations(s3_client: boto3.client, key: str, file_path: str, batch_size: int = 10000) -> List[profile.Profile]:
    posts_df = get_csv(s3_client=s3_client, file_path=file_path)
    total_entries = len(posts_df)
    profiles = []
    with tqdm(total = total_entries, file = sys.stdout) as pbar:
        for row in posts_df.itertuples(index=False):
            oid = getattr(row, 'post_id')
            locations = getattr(row, 'locations')
            if not pd.isnull(locations):
                profiles.append(profile.Profile(otype=_POST_OTYPE, oid=oid, key=key, value=locations))

            if len(profiles) > batch_size:
                yield profiles
                del profiles[:]
            pbar.update(1)

    yield profiles


def posts_tags(s3_client: boto3.client, batch_size: int = 10000) -> List[profile.Profile]:
    posts_df = get_csv(s3_client=s3_client, file_path=_POST_TAGS_PATH)
    total_entries = len(posts_df)
    profiles = []
    with tqdm(total = total_entries, file = sys.stdout) as pbar:
        for row in posts_df.itertuples(index=False):
            oid = getattr(row, 'post_id')
            tags = getattr(row, 'tags')
            if not pd.isnull(tags):
                profiles.append(profile.Profile(otype=_POST_OTYPE, oid=oid, key=_POST_TAGS, value=tags))

            if len(profiles) > batch_size:
                yield profiles
                del profiles[:]
            pbar.update(1)

    yield profiles


def posts_updated_on(s3_client: boto3.client, batch_size: int = 10000) -> List[profile.Profile]:
    posts_df = get_csv(s3_client=s3_client, file_path=_POST_UPDATED_ON_PATH)
    total_entries = len(posts_df)
    profiles = []
    with tqdm(total = total_entries, file = sys.stdout) as pbar:
        for row in posts_df.itertuples(index=False):
            oid = getattr(row, 'id')
            updated_on = getattr(row, 'updated_on')
            if not pd.isnull(updated_on):
                profiles.append(profile.Profile(otype=_POST_OTYPE, oid=oid, key=_POST_UPDATED_AT, value=dp.parse(updated_on).timestamp()))

            if len(profiles) > batch_size:
                yield profiles
                del profiles[:]
            pbar.update(1)

    yield profiles


def posts_created_on(s3_client: boto3.client, batch_size: int = 10000) -> List[profile.Profile]:
    posts_df = get_csv(s3_client=s3_client, file_path=_POST_TAGS_PATH)
    total_entries = len(posts_df)
    profiles = []
    with tqdm(total = total_entries, file = sys.stdout) as pbar:
        for row in posts_df.itertuples(index=False):
            oid = getattr(row, 'post_id')
            created_on = getattr(row, 'created_on')
            if not pd.isnull(created_on):
                profiles.append(profile.Profile(otype=_POST_OTYPE, oid=oid, key=_POST_CREATED_AT, value=dp.parse(created_on).timestamp()))

            if len(profiles) > batch_size:
                yield profiles
                del profiles[:]
            pbar.update(1)

    yield profiles


def main():
    start = datetime.datetime.now().timestamp()
    s3_client = boto3.client("s3")
    rexer_client = rexclient.Client(_URL)

    print('========== processing user profiles ==========\n')
    for batch in users(s3_client=s3_client):
        rexer_client.set_profiles(batch)

    print('========== processing posts_categories ==========\n')
    for batch in posts_categories(s3_client):
        rexer_client.set_profiles(batch)

    print('========== processing posts_constituencies ==========\n')
    for batch in posts_constituencies(s3_client):
        rexer_client.set_profiles(batch)

    print('========== processing posts_states ==========\n')
    for batch in posts_locations(s3_client, key=_POST_STATES, file_path=_POST_STATES_PATH):
        rexer_client.set_profiles(batch)

    print('========== processing posts_districts ==========\n')
    for batch in posts_locations(s3_client, key=_POST_DISTRICTS, file_path=_POST_DISTRICTS_PATH):
        rexer_client.set_profiles(batch)

    # print('========== processing posts_tags ==========\n')
    # for batch in posts_tags(s3_client):
    #     rexer_client.set_profiles(batch)

    print('========== processing posts_updated_on ==========\n')
    for batch in posts_updated_on(s3_client):
        rexer_client.set_profiles(batch)

    print('========== processing posts_created_on ==========\n')
    for batch in posts_created_on(s3_client):
        rexer_client.set_profiles(batch)

    print(f'========== took: {datetime.datetime.now().timestamp() - start} seconds ==========\n')

main()