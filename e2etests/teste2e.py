import functools

import os
import random
import time
import unittest

import lib

from rexerclient.rql import var, op, cond
from rexerclient import client
from rexerclient.models import action, profile


URL = 'http://localhost:2425'


class TestTier(object):
    def __init__(self, tier_id):
        self.env = os.environ.copy()
        self.env['TIER_ID'] = str(tier_id)

    def __enter__(self):
        with lib.gorun('fennel/test/cmds/tiergod', 'dynamic,integration', self.env, flags=['--mode', 'create'], wait=True):
            pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        with lib.gorun('fennel/test/cmds/tiergod', 'dynamic,integration', self.env, flags=['--mode', 'destroy'], wait=True):
            pass


def tiered(wrapped):
    @functools.wraps(wrapped)
    def fn(*args, **kwargs):
        tier_id = random.randint(0, 1e8)
        env = os.environ.copy()
        env['TIER_ID'] = str(tier_id)
        with TestTier(tier_id):
            env['METRICS_PORT'] = str(2436)
            with lib.gorun('fennel/service/http', 'dynamic,integration', env):
                env['METRICS_PORT'] = str(2446)
                with lib.gorun('fennel/service/countaggr', 'dynamic,integration', env):
                    # Wait for the services to be up.
                    time.sleep(10)
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
        actions = var('args').actions
        notif_events = op.std.filter(actions, var='a', where=(var('a').action_type == 'notif_send') |(var('a').action_type == 'notif_open'))
        with_time = op.time.addTimeBucketOfDay(notif_events, var='e', name='hour', timestamp=var('e').timestamp, bucket=3600)
        with_key = op.std.addField(with_time, var='e', name='groupkey', value=[var('e').actor_id, var('e').hour])
        with_val = op.std.addField(with_key, var='e', name='value', value=cond(var('e').action_type == 'notif_send', [0, 1], [1, 0]))
        options = {'aggregate_type': 'rate', 'durations': [4*24*3600, 7*24*3600], 'normalize': True}
        c.store_aggregate('user_notif_open_rate_by_hour_7days', with_val, options)

        # User CTR on notifs belonging to category X. Last 7 days.
        q = op.std.profile(notif_events, field='category', otype='content', key='category', var='e', oid=var('e').target_id)
        q = op.std.addField(q, var='e', name='groupkey', value=[var('e').actor_id, var('e').category])
        q = op.std.addField(q, var='e', name='value', value=cond(var('e').action_type == 'notif_send', [0, 1], [1, 0]))

        options = {'aggregate_type': 'rate', 'durations': [7*24*3600], 'normalize': True}
        c.store_aggregate('user_notif_open_rate_by_category_hour_7days', q, options)

        # total reactions on a piece of content
        q = op.std.filter(actions, var='a', where=var('a').action_type == 'react')
        q = op.std.addField(q, var='e', name='groupkey', value=var('e').target_id)
        q = op.std.addField(q, name='value', value=1)
        options = {'aggregate_type': 'sum', 'durations': [3*3600]}
        c.store_aggregate('content_num_reactions_last_3hours', q, options)
        #
        # # num of notifs opened by user in the last 3 days
        q = op.std.filter(actions, var='a', where=var('a').action_type == 'notif_open')
        q = op.std.addField(q, var='e', name='groupkey', value=var('e').actor_id)
        q = op.std.addField(q, name='value', value=1)
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
    def test_e2e_tuple(self):
        def compare_lists(l1, l2):
            if len(l1) != len(l2):
                return False
            l1_set = set(l1)
            for i in range(len(l2)):
                if l2[i] not in l1_set:
                    return False
            return True

        c = client.Client(URL)
        uid = 12312
        uid2 = 453
        # This test does not make logical sense, but is purely to test tuples
        # hence cities and gender is a tuple
        city = ('mumbai', ('delhi','bangalore'))
        city2 = (('la','nd'), ('sf','ny'))
        gender = (0, 1)
        video_id = 456
        # Phase 1 of tests, write a tuple from profile and read from profile
        c.set_profiles([
            profile.Profile(otype="user", oid=uid, key="city", value=city),
            profile.Profile(otype="user", oid=uid, key="gender", value=gender),
            profile.Profile(otype="user", oid=uid2, key="city", value=city2),
            profile.Profile(otype="user", oid=uid2, key="gender", value=gender),
        ])

        slept = 0
        passed = False
        while slept < 120:
            passed = (
                city == c.get_profile("user", uid, "city") and
                gender == c.get_profile("user", uid, "gender")
            )
            if passed:
                break

            time.sleep(5)
            slept += 5
        self.assertTrue(passed)

        # Phase 2 of tests, group by a tuple and read aggregate keyed on a tuple

        # Total views gained by a video in last 2 days for given city+gender
        actions = var('args').actions
        q1 = op.std.filter(actions, var='a', where=(var('a').action_type == 'view') & (var('a').target_type == 'video'))
        q1 = op.std.profile(q1, var='e', field='city', otype='user', oid=var('e').actor_id, key='city')
        q1 = op.std.profile(q1, var='e', field='gender', otype='user', oid=var('e').actor_id, key='gender')
        # Group by a tuple of tuples
        q1 = op.std.addField(q1, name='groupkey', var=('e', ), value=(var('e').city, var('e').gender))
        q1 = op.std.addField(q1, name='value', value=1)

        options = {'durations': [3600*24*3], 'aggregate_type': 'sum', }
        # Group key is tuple
        c.store_aggregate('trail_view_by_city_gender_agegroup_2days', q1, options)
        ts = int(time.time())

        actions = var('args').actions
        q1 = op.std.filter(actions, var='a', where=(var('a').action_type == 'view') & (var('a').target_type == 'video'))
        q1 = op.std.profile(q1, var='e', field='city', otype='user', oid=var('e').actor_id, key='city')
        q1 = op.std.profile(q1, var='e', field='gender', otype='user', oid=var('e').actor_id, key='gender')
        q1 = op.std.addField(q1, name='groupkey', var = ('e',), value=var('e').gender)
        q1 = op.std.addField(q1, name='value', var=('e', ), value=(var('e').city))
        # Group value is tuple
        options = {'durations': [3600*24*3*7], 'aggregate_type': 'list', }
        c.store_aggregate('list_of_cities', q1, options)

        # send multiple times with dedup keys
        actions = [
            action.Action(actor_type='user', actor_id=uid, target_type='video', target_id=video_id, action_type='view',
                          request_id=1, timestamp=ts, metadata={'watch_time': 20}, dedup_key="action1"),
            action.Action(actor_type='user', actor_id=uid, target_type='video', target_id=video_id, action_type='view',
                          request_id=1, timestamp=ts - 24*3600, metadata={'watch_time': 22}, dedup_key="action2"),
            action.Action(actor_type='user', actor_id=uid2, target_type='video', target_id=video_id, action_type='view',
                          request_id=21, timestamp=ts - 4*24*3600, metadata={'watch_time': 24}, dedup_key="action3"),
        ]
        c.log_multi(actions)

        b = int((ts % (24*3600)) / (2*3600))

        # now sleep for upto a minute and verify count processing worked
        # we could also just sleep for full minute but this rolling sleep
        # allows test to end earlier in happy cases
        slept = 0
        passed = False
        expected1 = 2
        expected2 = 2
        expected3 = [(('la','nd'), ('sf','ny')), ('mumbai', ('delhi', 'bangalore')), ('mumbai', ('delhi', 'bangalore'))]

        kwargs = {"duration": 1200}
        time.sleep(5)
        while slept < 120:
            found1 = c.aggregate_value(
                'trail_view_by_city_gender_agegroup_2days',
                 (city, gender),
            )
            q1 = op.std.aggregate(
                [{'uid': uid, 'b': b, 'city': city, 'gender': gender}], field='found',
                aggregate='trail_view_by_city_gender_agegroup_2days', var='e', groupkey=(var('e').city, var('e').gender)
            )[0].found
            found2 = c.query(q1)    
            q2 = op.std.aggregate([{'gender': gender}],
                field='found', aggregate='list_of_cities', var='e', groupkey=var('e').gender)[0].found
            found3 = c.query(q2)

            print("REsults for found3")
            print(found3)

            if found1 == expected1 and found2 == expected2 and compare_lists(found3, expected3):
                passed = True
                break
            time.sleep(5)
            slept += 5
        self.assertTrue(passed)
        print('all checks passed. ...')

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
        actions = var('args').actions
        q1 = op.std.filter(actions, var='a', where=(var('a').action_type == 'view') & (var('a').target_type == 'video'))
        q1 = op.std.profile(q1, var='e', field='city', otype='user', oid=var('e').actor_id, key='city')
        q1 = op.std.profile(q1, var='e', field='gender', otype='user', oid=var('e').actor_id, key='gender')
        q1 = op.std.profile(q1, var='e', field='age_group', otype='user', oid=var('e').actor_id, key='age_group')
        q1 = op.std.addField(q1, name='groupkey', var=('e', ), value=[var('e').target_id, var('e').city, var('e').gender, var('e').age_group])
        q1 = op.std.addField(q1, name='value', value=1)

        options = {'durations': [3600*24*2], 'aggregate_type': 'sum', }
        c.store_aggregate('trail_view_by_city_gender_agegroup_2days', q1, options)

        # average watch time of uid on videos created by creator_id by 2 hour windows
        q2 = op.std.filter(actions, var='a', where=var('a').action_type == 'view')
        q2 = op.std.profile(q2, var='e', field='creator_id', otype='video', oid=var('e').target_id, key='creatorId')
        q2 = op.time.addTimeBucketOfDay(q2, var='e', name='time_bucket', timestamp=var('e').timestamp, bucket=2*3600)
        q2 = op.std.addField(q2, name='groupkey', var='e', value=[var('e').actor_id, var('e').creator_id, var('e').time_bucket])
        q2 = op.std.addField(q2, name='value', var='e', value=var('e').metadata.watch_time)
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
            q1 = op.std.aggregate(
                [{'uid': uid, 'creator_id': creator_id, 'b': b}], field='found',
                aggregate='user_creator_avg_watchtime_by_2hour_windows_30days', var='e', groupkey=[var('e').uid, var('e').creator_id, var('e').b]
            )[0].found
            found2 = c.query(q1)
            found3 = c.aggregate_value(
                'user_creator_avg_watchtime_by_2hour_windows_30days',
                [uid, creator_id, b], kwargs
            )
            q2 = op.std.aggregate([{'uid': uid, 'creator_id': creator_id, 'b': b}], field='found',
                aggregate='user_creator_avg_watchtime_by_2hour_windows_30days', var='e',
                groupkey=[var('e').uid, var('e').creator_id, var('e').b], kwargs=kwargs
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
        cond = cond(var('args').x <= 5, "correct", "incorrect")
        found = c.query(cond, {'x': 5})
        self.assertEqual("correct", found)


@unittest.skip
class TestLoad(unittest.TestCase):
    @tiered
    def test_load(self):
        c = client.Client(URL)
        self.set_aggregates(c)
        with lib.gorun('fennel/test/cmds/loadtest', 'dynamic', os.environ.copy(), wait=True):
            pass

    def set_aggregates(self, c: client.Client):
        # Total views gained by a video in last 2 days for given city+gender+age_group
        actions = var('args').actions
        q1 = op.std.filter(actions, var='a', where=(var('a').action_type == 'view') & (var('a').target_type == 'video'))
        q1 = op.std.profile(q1, var='e', field='city', otype='user', oid=var('e').actor_id, key='city')
        q1 = op.std.profile(q1, var='e', field='gender', otype='user', oid=var('e').actor_id, key='gender')
        q1 = op.std.profile(q1, var='e', field='age_group', otype='user', oid=var('e').actor_id, key='age_group')
        q1 = op.std.addField(q1, name='groupkey', var=('e', ), value=[var('e').target_id, var('e').city, var('e').gender, var('e').age_group])
        q1 = op.std.addField(q1, name='value', value=1)

        options = {'durations': [3600*24*2], 'aggregate_type': 'sum', }
        c.store_aggregate('trail_view_by_city_gender_agegroup_2days', q1, options)

        # average watch time of uid on videos created by creator_id by 2 hour windows
        q2 = op.std.filter(actions, var='a', where=var('a').action_type == 'view')
        q2 = op.std.profile(q2, var='e', field='creator_id', otype='video', oid=var('e').target_id, key='creatorId')
        q2 = op.time.addTimeBucketOfDay(q2, var='e', name='time_bucket', timestamp=var('e').timestamp, bucket=2*3600)
        q2 = op.std.addField(q2, name='groupkey', var='e', value=[var('e').actor_id, var('e').creator_id, var('e').time_bucket])
        q2 = op.std.addField(q2, name='value', var='e', value=var('e').metadata.watch_time)
        options = {'aggregate_type': 'average', 'durations': [3600*24*30]}
        c.store_aggregate('user_creator_avg_watchtime_by_2hour_windows_30days', q2, options)


if __name__ == '__main__':
    unittest.main()
