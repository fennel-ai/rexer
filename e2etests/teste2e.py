import contextlib
import string
import sys

import functools

import os
import random
import subprocess
import time
import unittest

from rexerclient.rql import Var, Ops, Cond
from rexerclient import client
from rexerclient.models import action, profile


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
        actions = Var('args').actions
        notif_events = Ops.std.filter(actions, var='a', where=(Var('a').action_type == 'notif_send') |(Var('a').action_type == 'notif_open'))
        with_time = Ops.time.addTimeBucketOfDay(notif_events, var='e', name='hour', timestamp=Var('e').timestamp, bucket=3600)
        with_key = Ops.std.addField(with_time, var='e', name='groupkey', value=[Var('e').actor_id, Var('e').hour])
        with_val = Ops.std.addField(with_key, var='e', name='value', value=Cond(Var('e').action_type == 'notif_send', [0, 1], [1, 0]))
        options = {'aggregate_type': 'rate', 'durations': [4*24*3600, 7*24*3600], 'normalize': True}
        c.store_aggregate('user_notif_open_rate_by_hour_7days', with_val, options)

        # User CTR on notifs belonging to category X. Last 7 days.
        q = Ops.profile.addField(notif_events, name='category', otype='content', key='category', var='e', oid=Var('e').target_id)
        q = Ops.std.addField(q, var='e', name='groupkey', value=[Var('e').actor_id, Var('e').category])
        q = Ops.std.addField(q, var='e', name='value', value=Cond(Var('e').action_type == 'notif_send', [0, 1], [1, 0]))

        options = {'aggregate_type': 'rate', 'durations': [7*24*3600], 'normalize': True}
        c.store_aggregate('user_notif_open_rate_by_category_hour_7days', q, options)

        # total reactions on a piece of content
        q = Ops.std.filter(actions, var='a', where=Var('a').action_type == 'react')
        q = Ops.std.addField(q, var='e', name='groupkey', value=Var('e').target_id)
        q = Ops.std.addField(q, name='value', value=1)
        options = {'aggregate_type': 'sum', 'durations': [3*3600]}
        c.store_aggregate('content_num_reactions_last_3hours', q, options)
        #
        # # num of notifs opened by user in the last 3 days
        q = Ops.std.filter(actions, var='a', where=Var('a').action_type == 'notif_open')
        q = Ops.std.addField(q, var='e', name='groupkey', value=Var('e').actor_id)
        q = Ops.std.addField(q, name='value', value=1)
        options = {'aggregate_type': 'sum', 'durations': [3*24*3600]}
        c.store_aggregate('user_num_notif_opens_last_3days', q, options)

        c.set_profile("content", content_id, "category", category)
        self.assertEqual(category, c.get_profile("content", content_id, "category"))

        ts = int(time.time())
        a1 = action.Action(actor_type='user', actor_id=uid, target_type='content', target_id=content_id,
                           action_type='notif_send', request_id=1, timestamp=ts, dedup_key="a1")
        a2 = action.Action(actor_type='user', actor_id=uid, target_type='content', target_id=content_id,
                           action_type='notif_send', request_id=1, timestamp=ts+1, dedup_key="a2")
        a3 = action.Action(actor_type='user', actor_id=uid, target_type='content', target_id=content_id,
                           action_type='notif_open', request_id=1, timestamp=ts+2, dedup_key="a3")
        a4 = action.Action(actor_type='user', actor_id=uid, target_type='content', target_id=content_id,
                           action_type='react', request_id=2, timestamp=ts+3, dedup_key="a4")
        # log multiple times with dedup
        for i in range(5):
            c.log(a1)
            c.log(a2)
            c.log(a3)
            c.log(a4)
        # this action was logged 8 days in history so should not apply towards any aggregate
        c.log(action.Action(actor_type='user', actor_id=uid, target_type='content', target_id=content_id,
                            action_type='notif_send', request_id=7, timestamp=ts-8*24*3600))
        b = int((ts % (24*3600)) / 3600)

        # now sleep for upto a minute and verify count processing worked
        # we could also just sleep for full minute but this rolling sleep
        # allows test to end earlier in happy cases
        slept = 0
        passed = False
        expected1 = 0.09452865480086611  # normalized for 1 in 2
        expected2 = 0.09452865480086611  # normalized for 1 in 2
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

        # set some profiles using set_profiles
        c.set_profiles([
            profile.Profile(otype="user", oid=uid, key="city", value=city),
            profile.Profile(otype="user", oid=uid, key="gender", value=gender),
            profile.Profile(otype="user", oid=uid, key="age_group", value=age_group),
            profile.Profile(otype="video", oid=video_id, key="creatorId", value=creator_id),
        ])

        slept = 0
        passed = False
        while slept < 120:
            passed = (
                city == c.get_profile("user", uid, "city") and
                gender == c.get_profile("user", uid, "gender") and
                age_group == c.get_profile("user", uid, "age_group") and
                creator_id == c.get_profile("video", video_id, "creatorId")
            )
            if passed:
                break

            time.sleep(5)
            slept += 5
        self.assertTrue(passed)

        # Total views gained by a video in last 2 days for given city+gender+age_group
        actions = Var('args').actions
        q1 = Ops.std.filter(actions, var='a', where=(Var('a').action_type == 'view') & (Var('a').target_type == 'video'))
        q1 = Ops.profile.addField(q1, var='e', name='city', otype='user', oid=Var('e').actor_id, key='city')
        q1 = Ops.profile.addField(q1, name='gender', otype='user', var='e', oid=Var('e').actor_id, key='gender')
        q1 = Ops.profile.addField(q1, name='age_group', otype='user', var='e', oid=Var('e').actor_id, key='age_group')
        q1 = Ops.std.addField(q1, name='groupkey', var=('e', ), value=[Var('e').target_id, Var('e').city, Var('e').gender, Var('e').age_group])
        q1 = Ops.std.addField(q1, name='value', value=1)

        options = {'durations': [3600*24*2], 'aggregate_type': 'sum', }
        c.store_aggregate('trail_view_by_city_gender_agegroup_2days', q1, options)

        # average watch time of uid on videos created by creator_id by 2 hour windows
        q2 = Ops.std.filter(actions, var='a', where=Var('a').action_type == 'view')
        q2 = Ops.profile.addField(q2, var='e', name='creator_id', otype='video', oid=Var('e').target_id, key='creatorId')
        q2 = Ops.time.addTimeBucketOfDay(q2, var='e', name='time_bucket', timestamp=Var('e').timestamp, bucket=2*3600)
        q2 = Ops.std.addField(q2, name='groupkey', var='e', value=[Var('e').actor_id, Var('e').creator_id, Var('e').time_bucket])
        q2 = Ops.std.addField(q2, name='value', var='e', value=Var('e').metadata.watch_time)
        options = {'aggregate_type': 'average', 'durations': [3600*24*30]}
        c.store_aggregate('user_creator_avg_watchtime_by_2hour_windows_30days', q2, options)

        ts = int(time.time())

        # send multiple times with dedup keys
        actions = [
            action.Action(actor_type='user', actor_id=uid, target_type='video', target_id=video_id, action_type='view',
                          request_id=1, timestamp=ts, metadata={'watch_time': 20}, dedup_key="action1"),
            action.Action(actor_type='user', actor_id=uid, target_type='video', target_id=video_id, action_type='view',
                          request_id=1, timestamp=ts - 3*24*3600, metadata={'watch_time': 22}, dedup_key="action2"),
        ]
        c.log_multi(actions)
        c.log_multi(actions)
        c.log_multi(actions)
        b = int((ts % (24*3600)) / (2*3600))

        # now sleep for upto a minute and verify count processing worked
        # we could also just sleep for full minute but this rolling sleep
        # allows test to end earlier in happy cases
        slept = 0
        passed = False
        expected1 = 1
        expected2 = 21
        expected3 = expected4 = 20
        kwargs = {"duration": 1200}
        while slept < 120:
            found1 = c.aggregate_value(
                'trail_view_by_city_gender_agegroup_2days',
                [video_id, city, gender, age_group],
            )
            q1 = Ops.aggregate.addField(
                [{'uid': uid, 'creator_id': creator_id, 'b': b}], name='found',
                aggregate='user_creator_avg_watchtime_by_2hour_windows_30days', var='e', groupkey=[Var('e').uid, Var('e').creator_id, Var('e').b]
            )[0].found
            found2 = c.query(q1)
            found3 = c.aggregate_value(
                'user_creator_avg_watchtime_by_2hour_windows_30days',
                [uid, creator_id, b], kwargs
            )
            q2 = Ops.aggregate.addField([{'uid': uid, 'creator_id': creator_id, 'b': b}], name='found',
                aggregate='user_creator_avg_watchtime_by_2hour_windows_30days', var='e',
                groupkey=[Var('e').uid, Var('e').creator_id, Var('e').b], kwargs=kwargs
            )[0].found
            found4 = c.query(q2)

            if found1 == expected1 and found2 == expected2 and found3 == expected3 and found4 == expected4:
                passed = True
                break
            time.sleep(5)
            slept += 5
        self.assertTrue(passed)

        # test with batch_aggregate_value()
        req1 = ('trail_view_by_city_gender_agegroup_2days', [video_id, city, gender, age_group], None)
        req2 = ('user_creator_avg_watchtime_by_2hour_windows_30days', [uid, creator_id, b], None)
        req3 = ('user_creator_avg_watchtime_by_2hour_windows_30days', [uid, creator_id, b], kwargs)
        found1, found2, found3 = c.batch_aggregate_value([req1, req2, req3])
        self.assertEqual(expected1, found1)
        self.assertEqual(expected2, found2)
        self.assertEqual(expected3, found3)

        print('all checks passed...')

    @unittest.skip
    @tiered
    def test_queries(self):
        c = client.Client(URL)
        cond = Cond(Var('args').x <= 5, "correct", "incorrect")
        found = c.query(cond, {'x': 5})
        self.assertEqual("correct", found)


@unittest.skip
class TestLoad(unittest.TestCase):
    @tiered
    def test_load(self):
        c = client.Client(URL)
        self.set_aggregates(c)
        with gorun('fennel/test/cmds/loadtest', 'dynamic', os.environ.copy(), wait=True):
            pass

    def set_aggregates(self, c: client.Client):
        # Total views gained by a video in last 2 days for given city+gender+age_group
        actions = Var('args').actions
        q1 = Ops.std.filter(actions, var='a', where=(Var('a').action_type == 'view') & (Var('a').target_type == 'video'))
        q1 = Ops.profile.addField(q1, var='e', name='city', otype='user', oid=Var('e').actor_id, key='city')
        q1 = Ops.profile.addField(q1, name='gender', otype='user', var='e', oid=Var('e').actor_id, key='gender')
        q1 = Ops.profile.addField(q1, name='age_group', otype='user', var='e', oid=Var('e').actor_id, key='age_group')
        q1 = Ops.std.addField(q1, name='groupkey', var=('e', ), value=[Var('e').target_id, Var('e').city, Var('e').gender, Var('e').age_group])
        q1 = Ops.std.addField(q1, name='value', value=1)

        options = {'durations': [3600*24*2], 'aggregate_type': 'sum', }
        c.store_aggregate('trail_view_by_city_gender_agegroup_2days', q1, options)

        # average watch time of uid on videos created by creator_id by 2 hour windows
        q2 = Ops.std.filter(actions, var='a', where=Var('a').action_type == 'view')
        q2 = Ops.profile.addField(q2, var='e', name='creator_id', otype='video', oid=Var('e').target_id, key='creatorId')
        q2 = Ops.time.addTimeBucketOfDay(q2, var='e', name='time_bucket', timestamp=Var('e').timestamp, bucket=2*3600)
        q2 = Ops.std.addField(q2, name='groupkey', var='e', value=[Var('e').actor_id, Var('e').creator_id, Var('e').time_bucket])
        q2 = Ops.std.addField(q2, name='value', var='e', value=Var('e').metadata.watch_time)
        options = {'aggregate_type': 'average', 'durations': [3600*24*30]}
        c.store_aggregate('user_creator_avg_watchtime_by_2hour_windows_30days', q2, options)


if __name__ == '__main__':
    unittest.main()
