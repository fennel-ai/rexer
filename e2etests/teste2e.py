import contextlib
import string
import sys

import functools

import os
import random
import subprocess
import time
import unittest

from rexerclient.rql import Var, Int, String, it, Ops, List, Cond
from rexerclient import client, models


ROOT = os.getenv("FENNEL_ROOT")
GODIR = os.path.join(ROOT, 'go/fennel')
URL = 'http://localhost:2425'


class TestTier(object):
    def __init__(self, tier_id):
        self.env = os.environ.copy()
        self.env['TIER_ID'] = str(tier_id)

    def __enter__(self):
        with gorun('fennel/test/cmds/tiergod', 'dynamic,integration', self.env, flags=['--mode', 'create'], wait=True):
            pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        with gorun('fennel/test/cmds/tiergod', 'dynamic,integration', self.env, flags=['--mode', 'destroy'], wait=True):
            pass


@contextlib.contextmanager
def gorun(path, tags, env, flags=None, wait=False, sleep=0):
    if flags is None:
        flags = []

    dir = ''.join(random.choice(string.ascii_lowercase) for _ in range(8))
    binary = '/tmp/%s/%s' % (dir, path)
    print('going to build:', path)
    b = subprocess.Popen(['go', 'build', '--tags', tags, '-o', binary, path], cwd=GODIR)
    b.wait()
    print('build: ', 'success' if b.returncode == 0 else 'fail')
    print('going to run:', path, ' '.join(flags))
    p = subprocess.Popen([binary] + flags, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL, env=env)
    # p = subprocess.Popen([binary] + flags, stderr=sys.stderr, stdout=sys.stdout, env=env)
    if wait:
        p.wait()
    if sleep:
        time.sleep(sleep)
    try:
        yield
    finally:
        print('going to kill:', path, ' '.join(flags))
        p.kill()
        p.wait()
        os.remove(binary)
        print('done killing:', path, ' '.join(flags))


def tiered(wrapped):
    @functools.wraps(wrapped)
    def fn(*args, **kwargs):
        tier_id = random.randint(0, 1e8)
        env = os.environ.copy()
        env['TIER_ID'] = str(tier_id)
        with TestTier(tier_id):
            env['METRICS_PORT'] = str(2436)
            with gorun('fennel/service/http', 'dynamic,integration', env, sleep=20):
                env['METRICS_PORT'] = str(2446)
                with gorun('fennel/service/countaggr', 'dynamic,integration', env):
                    return wrapped(*args, **kwargs)
    return fn


class TestEndToEnd(unittest.TestCase):
    @tiered
    def test_lokal(self):
        c = client.Client(URL)
        uid = 12312
        content_id = 456
        category = 'sports'

        # Open rate for the user by the hour in the last 7 days:
        q = Var('args').actions.apply(
            Ops.std.filter(where=(it.action_type == 'notif_send') | (it.action_type == 'notif_open')),
            Ops.time.addTimeBucketOfDay(name='hour', timestamp=it.timestamp, bucket=3600),
            Ops.std.addField(name='groupkey', value=[it.actor_id, it.hour]),
            Ops.std.addField(name='value', value=Cond(it.action_type == 'notif_send', [0, 1], [1, 0]))
        )
        options = {'aggregate_type': 'rate', 'duration': 7*24*3600, 'normalize': True}
        c.store_aggregate('user_notif_open_rate_by_hour_7days', q, options)

        # User CTR on notifs belonging to category X. Last 7 days.
        q = Var('args').actions.apply(
            Ops.std.filter(where=(it.action_type == 'notif_send') | (it.action_type == 'notif_open')),
            Ops.profile.addField(name='category', otype='content', oid=it.target_id, key='category'),
            Ops.std.addField(name='groupkey', value=[it.actor_id, it.category]),
            Ops.std.addField(name='value', value=Cond(it.action_type == 'notif_send', [0, 1], [1, 0]))
        )
        options = {'aggregate_type': 'rate', 'duration': 7*24*3600, 'normalize': True}
        c.store_aggregate('user_notif_open_rate_by_category_hour_7days', q, options)

        # total reactions on a piece of content
        q = Var('args').actions.apply(
            Ops.std.filter(where=it.action_type == 'react'),
            Ops.std.addField(name='groupkey', value=it.target_id),
            Ops.std.addField(name='value', value=1)
        )
        options = {'aggregate_type': 'count', 'duration': 3*3600}
        c.store_aggregate('content_num_reactions_last_3hours', q, options)

        # num of notifs opened by user in the last 3 days
        q = Var('args').actions.apply(
          Ops.std.filter(where=it.action_type == 'notif_open'),
          Ops.std.addField(name='groupkey', value=it.actor_id),
          Ops.std.addField(name='value', value=1),
        )
        options = {'aggregate_type': 'count', 'duration': 3*24*3600}
        c.store_aggregate('user_num_notif_opens_last_3days', q, options)

        c.set_profile("content", content_id, "category", category)
        self.assertEqual(category, c.get_profile("content", content_id, "category"))

        ts = int(time.time())
        c.log(actor_type='user', actor_id=uid, target_type='content', target_id=content_id, action_type='notif_send',
              request_id=1, timestamp=ts)
        c.log(actor_type='user', actor_id=uid, target_type='content', target_id=content_id, action_type='notif_send',
              request_id=1, timestamp=ts+1)
        c.log(actor_type='user', actor_id=uid, target_type='content', target_id=content_id, action_type='notif_open',
              request_id=1, timestamp=ts+2)
        c.log(actor_type='user', actor_id=uid, target_type='content', target_id=content_id, action_type='react',
            request_id=2, timestamp=ts+3)
        # second action was logged 8 days in history so should not apply towards any aggregate
        c.log(actor_type='user', actor_id=uid, target_type='content', target_id=content_id, action_type='notif_send',
              request_id=7, timestamp=ts-8*24*3600)
        b = int((ts % (24*3600)) / 3600)

        # now sleep for upto a minute and verify count processing worked
        # we could also just sleep for full minute but this rolling sleep
        # allows test to end earlier in happy cases
        slept = 0
        passed = False
        expected1 = 0.09452865480086611 # normalized for 1 in 2
        expected2 = 0.09452865480086611 # normalized for 1 in 2
        expected3 = 1
        expected4 = 1
        while slept < 120:
            found1 = c.aggregate_value(
                'user_notif_open_rate_by_hour_7days',
                [uid, b],
            )
            found2 = c.aggregate_value('user_notif_open_rate_by_category_hour_7days', [uid, category])
            found3 = c.aggregate_value('content_num_reactions_last_3hours', content_id)
            found4 = c.aggregate_value('user_num_notif_opens_last_3days', uid)
            if found1 == expected1 and found2 == expected2 and found3 == expected3 and found4 == expected4:
                passed = True
                break
            time.sleep(5)
            slept += 5
        self.assertTrue(passed)
        print('all checks passed...')

    @tiered
    def test_end_to_end(self):
        c = client.Client(URL)
        uid = 12312
        video_id = 456
        city = 'delhi'
        gender = 1
        age_group = 3
        creator_id = 567

        # set some profiles
        c.set_profile("user", uid, "city", city)
        c.set_profile("user", uid, "gender", gender)
        c.set_profile("user", uid, "age_group", age_group)
        c.set_profile('video', video_id, "creatorId", creator_id)

        self.assertEqual(city, c.get_profile("user", uid, "city"))
        self.assertEqual(gender, c.get_profile("user", uid, "gender"))
        self.assertEqual(age_group, c.get_profile("user", uid, "age_group"))
        self.assertEqual(creator_id, c.get_profile("video", video_id, "creatorId"))

        # Total views gained by a video in last 2 days for given city+gender+age_group
        q1 = Var('args').actions.apply(
          Ops.std.filter(where=(it.action_type == 'view') & (it.target_type == 'video')),
          Ops.profile.addField(name='city', otype='user', oid=it.actor_id, key='city'),
          Ops.profile.addField(name='gender', otype='user', oid=it.actor_id, key='gender'),
          Ops.profile.addField(name='age_group', otype='user', oid=it.actor_id, key='age_group'),
          Ops.std.addField(name='groupkey', value=[it.target_id, it.city, it.gender, it.age_group]),
          Ops.std.addField(name='value', value=1),
        )
        options = {'duration': 3600*24*2, 'aggregate_type': 'count', }
        c.store_aggregate('trail_view_by_city_gender_agegroup_2days', q1, options)

        # average watch time of uid on videos created by creator_id by 2 hour windows
        q2 = Var('args').actions.apply(
            Ops.std.filter(where=it.action_type == 'view'),
            Ops.profile.addField(name='creator_id', otype='video', oid=it.target_id, key='creatorId'),
            Ops.time.addTimeBucketOfDay(name='time_bucket', timestamp=it.timestamp, bucket=2*3600),
            Ops.std.addField(name='groupkey', value=[it.actor_id, it.creator_id, it.time_bucket]),
            Ops.std.addField(name='value', value=it.metadata.watch_time),
        )
        options = {'aggregate_type': 'average', 'duration': 3600*24*30}
        c.store_aggregate('user_creator_avg_watchtime_by_2hour_windows_30days', q2, options)

        ts = int(time.time())
        c.log(actor_type='user', actor_id=uid, target_type='video', target_id=video_id, action_type='view',
              request_id=1, timestamp=ts, metadata={'watch_time': 20})
        # second action was logged 3 days in history so should not apply towards agg1 but only to agg2
        c.log(actor_type='user', actor_id=uid, target_type='video', target_id=video_id, action_type='view',
              request_id=1, timestamp=ts-3*24*3600, metadata={'watch_time': 22})
        b = int((ts % (24*3600)) / (2*3600))

        # while countaggr is processing the action, check that query call is working
        cond = Cond(Int(1) <= 5, "correct", "incorrect")
        found = c.query(cond)
        self.assertEqual("correct", found)

        # now sleep for upto a minute and verify count processing worked
        # we could also just sleep for full minute but this rolling sleep
        # allows test to end earlier in happy cases
        slept = 0
        passed = False
        expected1 = 1
        expected2 = 21
        while slept < 120:
            found1 = c.aggregate_value(
                'trail_view_by_city_gender_agegroup_2days',
                [video_id, city, gender, age_group],
            )
            found2 = c.aggregate_value('user_creator_avg_watchtime_by_2hour_windows_30days', [uid, creator_id, b])
            if found1 == expected1 and found2 == expected2:
                passed = True
                break
            time.sleep(5)
            slept += 5
        self.assertTrue(passed)

        # test with batch_aggregate_value()
        req1 = ('trail_view_by_city_gender_agegroup_2days', [video_id, city, gender, age_group])
        req2 = ('user_creator_avg_watchtime_by_2hour_windows_30days', [uid, creator_id, b])
        found1, found2 = c.batch_aggregate_value([req1, req2])
        self.assertEqual(expected1, found1)
        self.assertEqual(expected2, found2)

        print('all checks passed...')


@unittest.skip
class TestLoad(unittest.TestCase):
    @tiered
    def test_load(self):
        c = client.Client(URL)
        self.set_aggregates(c)
        with gorun('fennel/test/cmds/loadtest', 'dynamic', os.environ.copy(), wait=True):
            pass

    def set_aggregates(self, c: client.Client):
        # Total views gained by a Trail on last 2 days for given city+gender+age_group
        q = Var('args').actions.apply(
            Ops.std.filter(where=(it.action_type == 'view') & (it.target_type == 'video')),
            Ops.profile.addField(name='city', otype='user', oid=it.actor_id, key='city'),
            Ops.profile.addField(name='gender', otype='user', oid=it.actor_id, key='gender'),
            Ops.profile.addField(name='age_group', otype='user', oid=it.actor_id, key='age_group'),
            Ops.std.addField(name='groupkey', value=List(it.target_id, it.city, it.gender, it.age_group)),
        )
        options = {'duration': 3600*24*2, 'aggregate_type': 'count', }
        c.store_aggregate('trail_view_by_city_gender_agegroup_2days', q, options)

        # Avg-watchtime of a  video for given country+OS+city_sate+mobile_brand+gender in 30 days
        q = Var('args').actions.apply(
            Ops.std.filter(where=it.action_type == 'view'),
            Ops.profile.addField(name='country', otype='user', oid=it.actor_id, key='country'),
            Ops.profile.addField(name='os', otype='user', oid=it.actor_id, key='os'),
            Ops.profile.addField(name='city', otype='user', oid=it.actor_id, key='city'),
            Ops.profile.addField(name='mobile_brand', otype='user', oid=it.actor_id, key='mobile_brand'),
            Ops.profile.addField(name='gender', otype='user', oid=it.actor_id, key='gender'),
            Ops.time.addDayOfWeek(name='day_of_week', timestamp=it.timestamp),
            Ops.time.addTimeBucketOfDay(name='time_bucket', timestamp=it.timestamp, bucket=3600),
            Ops.std.addField(name='amount', value=it.metadata.watch_time),
            Ops.std.addField(name='groupkey', value=[
                it.target_id, it.country, it.os, it.city, it.mobile_brand, it.gender, it.day_of_week, it.time_bucket
            ]),
            Ops.std.addField(name='value', value=it.metadata.watch_time),
        )
        options = {'aggregate_type': 'average', 'duration': 3600*24*30}
        c.store_aggregate('video_avg_watchtime_by_country_os_citystate_mobile_gender_30days', q, options)

        # Avg-watchtime of a user id  for creatorId in 2-hour window averaged over 30 days
        q = Var('args').actions.apply(
            Ops.std.filter(where=(it.action_type == 'view') & (it.target_type == 'video')),
            Ops.profile.addField(name='creator_id', otype='user', oid=it.actor_id, key='creatorId'),
            Ops.time.addTimeBucketOfDay(name='time_bucket', timestamp=it.timestamp, bucket=2*3600),
            Ops.std.addField(name='value', value=it.metadata.watch_time),
            Ops.std.addField(name='groupkey', value=[it.actor_id, it.creator_id, it.time_bucket]),
        )
        options = {'aggregate_type': 'average', 'duration': 3600*24*30}
        c.store_aggregate('user_creator_avg_watchtime_by_2hour_windows_30days', q, options)


if __name__ == '__main__':
    unittest.main()
