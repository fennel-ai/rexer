import os
import sys
from typing import List

import argparse
import ast
from venv import create
import boto3
import datetime
import dateutil.parser as dp 
import pandas as pd
import pytz
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
    help="path to the csv file to load post updated_on from."
    "If the file is being read from S3 bucket, this is the path to the object in the bucket."
    "Assumes this file exists.")
parser.add_argument("--post_tags_path",
    help="path to the csv file to load post tags from."
    "If the file is being read from S3 bucket, this is the path to the object in the bucket."
    "Assumes this file exists.")
parser.add_argument("--post_created_on_path",
    help="path to the csv file to load post created on from."
    "If the file is being read from S3 bucket, this is the path to the object in the bucket."
    "Assumes this file exists.")
parser.add_argument("--post_reporter_path", help="path to the csv file to load post reporters from.")

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
_POST_CREATED_ON_PATH: os.PathLike = args.post_created_on_path
_POST_REPORTER_PATH: os.PathLike = args.post_reporter_path

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
_POST_UPDATED_ON = "updated_on"
_POST_CREATED_ON = "created_on"
_POST_REPORTER = "reporter"


# We set the update time to be around the time when Lokal's backfill data was generated
# This helps avoid overwriting data from their live traffic with older data
# 
# However there is a scenario where the data held by Fennel is not consistent with Lokal
# Say, the backfill data was created at `T` timestamp and say we backfill by setting the
# update timestamp as `T-2`, given how profiles are updated in Fennel (the update time of the incoming
# request has to be greater than existing update time); it is possible that we continue with 
# stale data - since we set the update time of a profile actually modified at `T` as `T-2`, any transition
# which happened at `T-1` will be considered the latest
# 
# NOTE: As of last backfill, this is not an issue - profiles were backfilled to fill in missing data, while
# live data was being sent to the system. Backfill data was sent to us at `May 10, 2022, ~20:40`
_UPDATE_TIME = datetime.datetime(year=2022, month=5, day=10, hour=20, second=1, tzinfo=pytz.UTC)

_URL = "http://k8s-t107-aest107e-c969e2b35d-e2cc681d58e1e1ca.elb.ap-south-1.amazonaws.com/data"

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
                profiles.append(profile.Profile(otype=_USER_OTYPE, oid=int(oid), key=_USER_DISTRICT, value=district, update_time=_UPDATE_TIME))

            constituency = getattr(row, 'microlocation_id')
            if not pd.isnull(constituency):
                # Few of the columns (potentially from the test data or so) has constituency value as `float`. However their live traffic sends us ints, hence explicitly type casting this to int
                profiles.append(profile.Profile(otype=_USER_OTYPE, oid=int(oid), key=_USER_CONSTITUENCY, value=int(constituency), update_time=_UPDATE_TIME))

            created_on = getattr(row, 'created_on')
            if not pd.isnull(created_on):
                profiles.append(profile.Profile(otype=_USER_OTYPE, oid=int(oid), key=_USER_CREATED_ON, value=dp.parse(created_on).timestamp(), update_time=_UPDATE_TIME))

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
                # set is not JSON serializable, we convert it to list
                profiles.append(profile.Profile(otype=_POST_OTYPE, oid=int(oid), key=_POST_CATEGORIES, value=list(ast.literal_eval(categories)), update_time=_UPDATE_TIME))

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
                profiles.append(profile.Profile(otype=_POST_OTYPE, oid=int(oid), key=_POST_CONSTITUENCIES, value=list(ast.literal_eval(constituencies)), update_time=_UPDATE_TIME))

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
                profiles.append(profile.Profile(otype=_POST_OTYPE, oid=int(oid), key=key, value=list(ast.literal_eval(locations)), update_time=_UPDATE_TIME))

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
                profiles.append(profile.Profile(otype=_POST_OTYPE, oid=int(oid), key=_POST_TAGS, value=list(ast.literal_eval(tags)), update_time=_UPDATE_TIME))

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
                profiles.append(profile.Profile(otype=_POST_OTYPE, oid=int(oid), key=_POST_UPDATED_ON, value=dp.parse(updated_on).timestamp(), update_time=_UPDATE_TIME))

            if len(profiles) > batch_size:
                yield profiles
                del profiles[:]
            pbar.update(1)

    yield profiles


def posts_created_on(s3_client: boto3.client, batch_size: int = 10000) -> List[profile.Profile]:
    posts_df = get_csv(s3_client=s3_client, file_path=_POST_CREATED_ON_PATH)
    total_entries = len(posts_df)
    profiles = []
    with tqdm(total = total_entries, file = sys.stdout) as pbar:
        for row in posts_df.itertuples(index=False):
            oid = getattr(row, 'post_id')
            created_on = getattr(row, 'created_on')
            if not pd.isnull(created_on):
                profiles.append(profile.Profile(otype=_POST_OTYPE, oid=int(oid), key=_POST_CREATED_ON, value=dp.parse(created_on).timestamp(), update_time=_UPDATE_TIME))

            if len(profiles) > batch_size:
                yield profiles
                del profiles[:]
            pbar.update(1)

    yield profiles


def post_reporters(s3_client: boto3.client, batch_size: int = 10000) -> List[profile.Profile]:
    posts_df = get_csv(s3_client=s3_client, file_path=_POST_REPORTER_PATH)
    total_entries = len(posts_df)
    profiles = []
    with tqdm(total = total_entries, file = sys.stdout) as pbar:
        for row in posts_df.itertuples(index=False):
            oid = getattr(row, 'id')
            reported_id = getattr(row, 'reporter_id')
            if not pd.isnull(reported_id):
                profiles.append(profile.Profile(otype=_POST_OTYPE, oid=int(oid), key=_POST_REPORTER, value=int(reported_id), update_time=_UPDATE_TIME))

            if len(profiles) > batch_size:
                yield profiles
                del profiles[:]
            pbar.update(1)

    yield profiles


def main():
    start = datetime.datetime.now().timestamp()
    s3_client = boto3.client("s3")
    rexer_client = rexclient.Client(_URL)

    if _USERS_PATH:
        print('========== processing user profiles ==========\n')
        for batch in users(s3_client=s3_client):
            rexer_client.set_profiles(batch)

    if _POST_CATEGORIES_PATH:
        print('========== processing posts_categories ==========\n')
        for batch in posts_categories(s3_client):
            rexer_client.set_profiles(batch)

    if _POST_CONSTITUENCIES_PATH:
        print('========== processing posts_constituencies ==========\n')
        for batch in posts_constituencies(s3_client):
            rexer_client.set_profiles(batch)

    if _POST_STATES_PATH:
        print('========== processing posts_states ==========\n')
        for batch in posts_locations(s3_client, key=_POST_STATES, file_path=_POST_STATES_PATH):
            rexer_client.set_profiles(batch)

    if _POST_DISTRICTS_PATH:
        print('========== processing posts_districts ==========\n')
        for batch in posts_locations(s3_client, key=_POST_DISTRICTS, file_path=_POST_DISTRICTS_PATH):
            rexer_client.set_profiles(batch)

    if _POST_TAGS_PATH:
        print('========== processing posts_tags ==========\n')
        for batch in posts_tags(s3_client):
            rexer_client.set_profiles(batch)

    if _POST_UPDATED_ON_PATH:
        print('========== processing posts_updated_on ==========\n')
        for batch in posts_updated_on(s3_client):
            rexer_client.set_profiles(batch)

    if _POST_CREATED_ON_PATH:
        print('========== processing posts_created_on ==========\n')
        for batch in posts_created_on(s3_client):
            rexer_client.set_profiles(batch)

    if _POST_REPORTER_PATH:
        print('========== processing post_reporter_path ==========\n')
        for batch in post_reporters(s3_client):
            rexer_client.set_profiles(batch)

    print(f'========== took: {datetime.datetime.now().timestamp() - start} seconds ==========\n')

main()