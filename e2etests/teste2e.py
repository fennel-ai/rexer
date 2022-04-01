import functools
import os
import random
import unittest

import time

import lib
import rexerclient as rex
from rexerclient import client
from rexerclient.models import action, profile
from rexerclient.rql import var, cond, in_, len_, op

URL = 'http://localhost:2425'


class TestTier(object):
    def __init__(self, tier_id):
        self.env = os.environ.copy()
        self.env['TIER_ID'] = str(tier_id)

    def __enter__(self):
        with lib.gorun('fennel/test/cmds/tiergod', 'dynamic,integration', self.env, flags=['--mode', 'create'],
                       wait=True):
            pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        with lib.gorun('fennel/test/cmds/tiergod', 'dynamic,integration', self.env, flags=['--mode', 'destroy'],
                       wait=True):
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
                time.sleep(30)
                with lib.gorun('fennel/service/countaggr', 'dynamic,integration', env):
                    # Wait for the services to be up.
                    time.sleep(15)
                    return wrapped(*args, **kwargs)

    return fn


class TestEndToEnd(unittest.TestCase):
    @tiered
    def test_lokal(self):
        c = client.Client(URL)
        uid = 12312
        content_id = 456
        category = 'sports'

        @rex.aggregate(
            name='user_notif_open_rate_by_hour',
            aggregate_type='rate', action_types=['notif_send', 'notif_open'],
            config={'durations': [4 * 24 * 3600, 7 * 24 * 3600], 'normalize': True},
        )
        def agg_user_notif_open_rate_by_hour(actions):
            with_time = op.std.set(actions, var='e', name='hour', value=var('e').timestamp % (24 * 3600) // 3600)
            with_key = op.std.set(with_time, var='e', name='groupkey', value=[var('e').actor_id, var('e').hour])
            return op.std.set(with_key, var='e', name='value',
                              value=cond(var('e').action_type == 'notif_send', [0, 1], [1, 0]))

        agg_user_notif_open_rate_by_hour.store(client=c)

        @rex.aggregate(
            name='user_notif_open_rate_by_category',
            aggregate_type='rate', action_types=['notif_send', 'notif_open'],
            config={'durations': [4 * 24 * 3600, 7 * 24 * 3600], 'normalize': True},
        )
        def agg_user_notif_open_rate_by_category(actions):
            q = op.std.profile(actions, field='category', otype='content', key='category', var='e',
                               oid=var('e').target_id)
            q = op.std.set(q, var='e', name='groupkey', value=[var('e').actor_id, var('e').category])
            return op.std.set(q, var='e', name='value',
                              value=cond(var('e').action_type == 'notif_send', [0, 1], [1, 0]))

        agg_user_notif_open_rate_by_category.store(client=c)

        @rex.aggregate(
            name='content_num_reactions',
            aggregate_type='sum', action_types=['react'], config={'durations': [7 * 24 * 3600]},
        )
        def agg_reactions_by_post(actions):
            q = op.std.set(actions, var='e', name='groupkey', value=var('e').target_id)
            return op.std.set(q, name='value', value=1)

        agg_reactions_by_post.store(client=c)

        @rex.aggregate(
            name='user_num_notif_opens',
            aggregate_type='sum', action_types=['notif_open'], config={'durations': [7 * 24 * 3600]},
        )
        def agg_user_num_notif_opens(actions):
            q = op.std.set(actions, var='e', name='groupkey', value=var('e').actor_id)
            return op.std.set(q, name='value', value=1)

        agg_user_num_notif_opens.store(client=c)

        p1 = profile.Profile(otype="content", oid=content_id, key="category", value=category)

        ts = int(time.time())
        a1 = action.Action(actor_type='user', actor_id=uid, target_type='content', target_id=content_id,
                           action_type='notif_send', request_id=1, timestamp=ts, dedup_key="a1")
        a2 = action.Action(actor_type='user', actor_id=uid, target_type='content', target_id=content_id,
                           action_type='notif_send', request_id=1, timestamp=ts + 1, dedup_key="a2")
        a3 = action.Action(actor_type='user', actor_id=uid, target_type='content', target_id=content_id,
                           action_type='notif_open', request_id=1, timestamp=ts + 2, dedup_key="a3")
        a4 = action.Action(actor_type='user', actor_id=uid, target_type='content', target_id=content_id,
                           action_type='react', request_id=2, timestamp=ts + 3, dedup_key="a4")

        # verify that test of actions works well
        mock = {'Profiles': [p1]}
        actions = [a1, a2, a3, a4]
        expected = [
            {'action_id': 1, 'action_type': 'notif_send', 'actor_id': 12312, 'actor_type': 'user', 'category': 'sports',
             'groupkey': [12312, 'sports'], 'metadata': {}, 'request_id': 1, 'target_id': 456,
             'target_type': 'content', 'timestamp': ts, 'value': [0, 1]},
            {'action_id': 2, 'action_type': 'notif_send', 'actor_id': 12312, 'actor_type': 'user', 'category': 'sports',
             'groupkey': [12312, 'sports'], 'metadata': {}, 'request_id': 1, 'target_id': 456,
             'target_type': 'content', 'timestamp': ts + 1, 'value': [0, 1]},
            {'action_id': 3, 'action_type': 'notif_open', 'actor_id': 12312, 'actor_type': 'user', 'category': 'sports',
             'groupkey': [12312, 'sports'], 'metadata': {}, 'request_id': 1, 'target_id': 456,
             'target_type': 'content', 'timestamp': ts + 2, 'value': [1, 0]}
        ]
        self.assertEqual(expected, agg_user_notif_open_rate_by_category.test(actions, client=c, mock=mock))

        c.set_profile("content", content_id, "category", category)
        self.assertEqual(category, c.get_profile("content", content_id, "category"))
        # log multiple times with dedup
        for i in range(5):
            c.log(a1)
            c.log(a2)
            c.log(a3)
            c.log(a4)
        # this action was logged 8 days in history so should not apply towards any aggregate
        c.log(action.Action(actor_type='user', actor_id=uid, target_type='content', target_id=content_id,
                            action_type='notif_send', request_id=7, timestamp=ts - 8 * 24 * 3600))

        b = int((ts % (24 * 3600)) / 3600)

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
            found1 = c.aggregate_value('user_notif_open_rate_by_hour', [uid, b], {'duration': 7 * 24 * 3600})
            found2 = c.aggregate_value('user_notif_open_rate_by_category', [uid, category], {'duration': 7 * 24 * 3600})
            found3 = c.aggregate_value('content_num_reactions', content_id, {'duration': 7 * 24 * 3600})
            found4 = c.aggregate_value('user_num_notif_opens', uid, {'duration': 7 * 24 * 3600})
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
        city = ('mumbai', ('delhi', 'bangalore'))
        city2 = (('la', 'nd'), ('sf', 'ny'))
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
        q1 = op.std.set(q1, name='groupkey', var=('e',), value=(var('e').city, var('e').gender))
        q1 = op.std.set(q1, name='value', value=1)

        options = {'durations': [3600 * 24 * 3], 'aggregate_type': 'sum', }

        @rex.aggregate(
            name='trail_view_by_city_gender_agegroup_2days',
            aggregate_type='sum', action_types=['view'], config={'durations': [2 * 24 * 3600]},
        )
        def agg1(actions):
            q = op.std.filter(actions, var='a', where=var('a').target_type == 'video')
            q = op.std.profile(q, var='e', field='city', otype='user', oid=var('e').actor_id, key='city')
            q = op.std.profile(q, var='e', field='gender', otype='user', oid=var('e').actor_id, key='gender')
            q = op.std.set(q, name='groupkey', var=('e',), value=(var('e').city, var('e').gender))
            return op.std.set(q, name='value', var=('e',), value=1)

        agg1.store(client=c)

        # Group key is tuple
        ts = int(time.time())

        # Total views gained by a video in last 2 days for given city+gender+age_group
        @rex.aggregate(
            name='list_of_cities',
            aggregate_type='list', action_types=['view'], config={'durations': [7 * 24 * 3600]},
        )
        def agg2(actions):
            q = op.std.filter(actions, var='a', where=var('a').target_type == 'video')
            q = op.std.profile(q, var='e', field='city', otype='user', oid=var('e').actor_id, key='city')
            q = op.std.profile(q, var='e', field='gender', otype='user', oid=var('e').actor_id, key='gender')
            q = op.std.set(q, name='groupkey', var=('e',), value=var('e').gender)
            return op.std.set(q, name='value', var=('e',), value=var('e').city)

        agg2.store(client=c)

        # send multiple times with dedup keys
        actions = [
            action.Action(actor_type='user', actor_id=uid, target_type='video', target_id=video_id, action_type='view',
                          request_id=1, timestamp=ts, metadata={'watch_time': 20}, dedup_key="action1"),
            action.Action(actor_type='user', actor_id=uid, target_type='video', target_id=video_id, action_type='view',
                          request_id=1, timestamp=ts - 24 * 3600, metadata={'watch_time': 22}, dedup_key="action2"),
            action.Action(actor_type='user', actor_id=uid2, target_type='video', target_id=video_id, action_type='view',
                          request_id=21, timestamp=ts - 4 * 24 * 3600, metadata={'watch_time': 24},
                          dedup_key="action3"),
        ]
        c.log_multi(actions)

        b = int((ts % (24 * 3600)) / (2 * 3600))

        # now sleep for upto a minute and verify count processing worked
        # we could also just sleep for full minute but this rolling sleep
        # allows test to end earlier in happy cases
        slept = 0
        passed = False
        expected1 = 2
        expected2 = 2
        expected3 = [(('la', 'nd'), ('sf', 'ny')), ('mumbai', ('delhi', 'bangalore')),
                     ('mumbai', ('delhi', 'bangalore'))]

        kwargs = {"duration": 1200}
        time.sleep(5)
        while slept < 120:
            found1 = c.aggregate_value(
                'trail_view_by_city_gender_agegroup_2days',
                (city, gender),
                {'duration': 2 * 24 * 3600},
            )
            q1 = op.std.aggregate(
                [{'uid': uid, 'b': b, 'city': city, 'gender': gender}], field='found',
                aggregate='trail_view_by_city_gender_agegroup_2days',
                var='e',
                groupkey=(var('e').city, var('e').gender),
                kwargs={'duration': 2 * 24 * 3600},
            )[0].found
            found2 = c.query(q1)
            q2 = op.std.aggregate(
                [{'gender': gender}],
                field='found',
                aggregate='list_of_cities',
                var='e',
                groupkey=var('e').gender,
                kwargs={'duration': 7 * 24 * 3600},
            )[0].found
            found3 = c.query(q2)

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
        @rex.aggregate(
            name='video_view_by_city_gender_agegroup',
            aggregate_type='sum', action_types=['view'], config={'durations': [2 * 24 * 3600]},
        )
        def agg1(actions):
            q = op.std.filter(actions, var='a', where=var('a').target_type == 'video')
            q = op.std.profile(q, var='e', field='city', otype='user', oid=var('e').actor_id, key='city')
            q = op.std.profile(q, var='e', field='gender', otype='user', oid=var('e').actor_id, key='gender')
            q = op.std.profile(q, var='e', field='age_group', otype='user', oid=var('e').actor_id, key='age_group')
            q = op.std.set(q, name='groupkey', var=('e',), value=[
                var('e').target_id, var('e').city, var('e').gender, var('e').age_group
            ])
            return op.std.set(q, name='value', value=1)

        agg1.store(client=c)

        @rex.aggregate(
            name='user_creator_avg_watchtime_by_2hour_windows',
            aggregate_type='average', action_types=['view'], config={'durations': [30 * 24 * 3600, 1200]},
        )
        def agg2(actions):
            q = op.std.filter(actions, var='a', where=var('a').action_type == 'view')
            q = op.std.profile(q, var='e', field='creator_id', otype='video', oid=var('e').target_id, key='creatorId')
            q = op.std.set(q, var='e', name='time_bucket', value=var('e').timestamp % (24 * 3600) // (2 * 3600))
            q = op.std.set(q, name='groupkey', var='e',
                           value=[var('e').actor_id, var('e').creator_id, var('e').time_bucket])
            return op.std.set(q, name='value', var='e', value=var('e').metadata.watch_time)

        agg2.store(client=c)

        ts = int(time.time())
        b = int((ts % (24 * 3600)) / (2 * 3600))
        # send multiple times with dedup keys
        actions = [
            action.Action(actor_type='user', actor_id=uid, target_type='video', target_id=video_id, action_type='view',
                          request_id=1, timestamp=ts, metadata={'watch_time': 20}, dedup_key="action1"),
            action.Action(actor_type='user', actor_id=uid, target_type='video', target_id=video_id, action_type='view',
                          request_id=1, timestamp=ts - 3 * 24 * 3600, metadata={'watch_time': 22}, dedup_key="action2"),
        ]
        c.log_multi(actions)
        c.log_multi(actions)
        c.log_multi(actions)

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
                'video_view_by_city_gender_agegroup',
                [video_id, city, gender, age_group],
                {'duration': 2 * 24 * 3600},
            )
            q1 = op.std.aggregate(
                [{'uid': uid, 'creator_id': creator_id, 'b': b}], field='found',
                aggregate='user_creator_avg_watchtime_by_2hour_windows',
                var='e',
                groupkey=[var('e').uid, var('e').creator_id, var('e').b],
                kwargs={'duration': 30 * 24 * 3600},
            )[0].found
            found2 = c.query(q1)
            found3 = c.aggregate_value(
                'user_creator_avg_watchtime_by_2hour_windows',
                [uid, creator_id, b],
                {'duration': 1200}
            )
            q2 = op.std.aggregate(
                [{'uid': uid, 'creator_id': creator_id, 'b': b}], field='found',
                aggregate='user_creator_avg_watchtime_by_2hour_windows', var='e',
                groupkey=[var('e').uid, var('e').creator_id, var('e').b],
                kwargs={"duration": 1200}
            )[0].found
            found4 = c.query(q2)

            if found1 == expected1 and found2 == expected2 and found3 == expected3 and found4 == expected4:
                passed = True
                break
            time.sleep(5)
            slept += 5
        self.assertTrue(passed)

        # test with batch_aggregate_value()
        req1 = ('video_view_by_city_gender_agegroup', [video_id, city, gender, age_group], {"duration": 2 * 24 * 3600})
        req2 = ('user_creator_avg_watchtime_by_2hour_windows', [uid, creator_id, b], {"duration": 30 * 24 * 3600})
        req3 = ('user_creator_avg_watchtime_by_2hour_windows', [uid, creator_id, b], {"duration": 1200})
        found1, found2, found3 = c.batch_aggregate_value([req1, req2, req3])
        self.assertEqual(expected1, found1)
        self.assertEqual(expected2, found2)
        self.assertEqual(expected3, found3)

        print('all checks passed...')

    @tiered
    def test_queries(self):
        c = client.Client(URL)
        cond_ = cond(var('x') <= 5, "correct", "incorrect")
        found = c.query(cond_, {'x': 5})
        self.assertEqual("correct", found)

        found = c.query(in_(3, [2, 4, len_([1, 2, 3])]))
        self.assertTrue(found)

        found = c.query(in_('hi', [2, 4, len_([1, 2, 3])]))
        self.assertFalse(found)

        found = c.query(in_('hi', {'hi': 1, 'bye': 'great'}))
        self.assertTrue(found)

        found = c.query(in_('missing', {'hi': 1, 'bye': 'great'}))
        self.assertFalse(found)

        self.assertTrue(c.query(in_(len_([1, 2, 3]), [2, 5, 'hi', 3])))

        # test a complex combination of groupby, sortby and map
        q = [{'a': 1, 'b': 'one'}, {'a': 2, 'b': 'one'}, {'a': 3, 'b': 'two'}, {'a': 4, 'b': 'three'}]
        q = op.std.group(q, var='e', by=var('e').b)
        q = op.std.sort(q, var='g', by=len_(var('g').elements), reverse=True)
        q = op.std.map(q, var='e', to={'x': var('e').group, 'y': var('e').elements[0].a})
        self.assertEqual([{'x': 'one', 'y': 1}, {'x': 'two', 'y': 3}, {'x': 'three', 'y': 4}], c.query(q))


@unittest.skip
class TestLoad(unittest.TestCase):
    @tiered
    def test_load(self):
        c = client.Client(URL)
        self.set_aggregates(c)
        with lib.gorun('fennel/test/cmds/loadtest', 'dynamic', os.environ.copy(), wait=True):
            pass

    def set_aggregates(self, c: client.Client):
        @rex.aggregate(
            name='video_view_by_city_gender_agegroup',
            aggregate_type='sum', action_types=['view'], config={'durations': [2 * 24 * 3600]},
        )
        def agg1(actions):
            q = op.std.filter(actions, var='a', where=var('a').target_type == 'video')
            q = op.std.profile(q, var='e', field='city', otype='user', oid=var('e').actor_id, key='city')
            q = op.std.profile(q, var='e', field='gender', otype='user', oid=var('e').actor_id, key='gender')
            q = op.std.profile(q, var='e', field='age_group', otype='user', oid=var('e').actor_id, key='age_group')
            q = op.std.set(q, name='groupkey', var=('e',), value=[
                var('e').target_id, var('e').city, var('e').gender, var('e').age_group
            ])
            return op.std.set(q, name='value', value=1)

        agg1.store(client=c)

        @rex.aggregate(
            name='user_creator_avg_watchtime_by_2hour_windows',
            aggregate_type='average', action_types=['view'], config={'durations': [30 * 24 * 3600]},
        )
        def agg2(actions):
            q = op.std.filter(actions, var='a', where=var('a').action_type == 'view')
            q = op.std.profile(q, var='e', field='creator_id', otype='video', oid=var('e').target_id, key='creatorId')
            q = op.std.set(q, var='e', name='time_bucket', value=var('e').timestamp % (24 * 3600) // (2 * 3600))
            q = op.std.set(q, name='groupkey', var='e',
                           value=[var('e').actor_id, var('e').creator_id, var('e').time_bucket])
            return op.std.set(q, name='value', var='e', value=var('e').metadata.watch_time)

        agg2.store(client=c)


if __name__ == '__main__':
    unittest.main()
