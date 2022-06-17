import functools
import os
import random
import unittest

import time
from datetime import datetime, timezone, timedelta
import lib
import rexerclient as rex
from rexerclient import client
from rexerclient.models import action, profile
from rexerclient.rql import var, op, cond, in_, len_, op

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
        env['DISABLE_CACHE'] = '1'
        with TestTier(tier_id):
            env['METRICS_PORT'] = str(2436)
            env['PPROF_PORT'] = str(2437)
            with lib.gorun('fennel/service/http', 'dynamic,integration', env):
                env['METRICS_PORT'] = str(2446)
                env['PPROF_PORT'] = str(2467)
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

        @rex.aggregate(
            name='user_notif_open_rate_by_hour',
            aggregate_type='rate', action_types=['notif_send', 'notif_open'],
            config={'durations': [4 * 24 * 3600, 7 * 24 * 3600], 'normalize': True},
        )
        def agg_user_notif_open_rate_by_hour(actions):
            with_time = op.std.set(actions, var='e', field='hour', value=var('e').timestamp % (24 * 3600) // 3600)
            with_key = op.std.set(with_time, var='e', field='groupkey', value=[var('e').actor_id, var('e').hour])
            return op.std.set(with_key, var='e', field='value',
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
            q = op.std.set(q, var='e', field='groupkey', value=[var('e').actor_id, var('e').category])
            return op.std.set(q, var='e', field='value',
                              value=cond(var('e').action_type == 'notif_send', [0, 1], [1, 0]))

        agg_user_notif_open_rate_by_category.store(client=c)

        @rex.aggregate(
            name='content_num_reactions',
            aggregate_type='sum', action_types=['react'], config={'durations': [7 * 24 * 3600]},
        )
        def agg_reactions_by_post(actions):
            q = op.std.set(actions, var='e', field='groupkey', value=var('e').target_id)
            return op.std.set(q, field='value', value=1)

        agg_reactions_by_post.store(client=c)

        @rex.aggregate(
            name='user_num_notif_opens',
            aggregate_type='sum', action_types=['notif_open'], config={'durations': [7 * 24 * 3600]},
        )
        def agg_user_num_notif_opens(actions):
            q = op.std.set(actions, var='e', field='groupkey', value=var('e').actor_id)
            return op.std.set(q, field='value', value=1)

        agg_user_num_notif_opens.store(client=c)

        p1 = profile.Profile(otype="content", oid=content_id, key="category", value=category)

        ts = datetime.now().astimezone(timezone.utc)
        a1 = action.Action(actor_type='user', actor_id=uid, target_type='content', target_id=content_id,
                           action_type='notif_send', request_id=1, timestamp=ts, dedup_key="a1")
        a2 = action.Action(actor_type='user', actor_id=uid, target_type='content', target_id=content_id,
                           action_type='notif_send', request_id=1, timestamp=ts + timedelta(seconds=1), dedup_key="a2")
        a3 = action.Action(actor_type='user', actor_id=uid, target_type='content', target_id=content_id,
                           action_type='notif_open', request_id=1, timestamp=ts + timedelta(seconds=2), dedup_key="a3")
        a4 = action.Action(actor_type='user', actor_id=uid, target_type='content', target_id=content_id,
                           action_type='react', request_id=2, timestamp=ts + timedelta(seconds=3), dedup_key="a4")

        # verify that test of actions works well
        mock = {'profiles': [p1]}
        actions = [a1, a2, a3, a4]
        expected = [
            {'action_type': 'notif_send', 'actor_id': 12312, 'actor_type': 'user', 'category': 'sports',
             'groupkey': [12312, 'sports'], 'metadata': {}, 'request_id': 1, 'target_id': 456,
             'target_type': 'content', 'timestamp': int(ts.timestamp()), 'value': [0, 1]},
            {'action_type': 'notif_send', 'actor_id': 12312, 'actor_type': 'user', 'category': 'sports',
             'groupkey': [12312, 'sports'], 'metadata': {}, 'request_id': 1, 'target_id': 456,
             'target_type': 'content', 'timestamp': int((ts + timedelta(seconds=1)).timestamp()), 'value': [0, 1]},
            {'action_type': 'notif_open', 'actor_id': 12312, 'actor_type': 'user', 'category': 'sports',
             'groupkey': [12312, 'sports'], 'metadata': {}, 'request_id': 1, 'target_id': 456,
             'target_type': 'content', 'timestamp': int((ts + timedelta(seconds=2)).timestamp()), 'value': [1, 0]}
        ]
        self.assertEqual(expected, agg_user_notif_open_rate_by_category.test(actions, client=c, mock=mock))

        c.set_profile("content", content_id, "category", category)
        slept = 0
        passed = False
        while slept < 120:
            if category == c.get_profile("content", content_id, "category"):
                passed = True
                break
            time.sleep(2)
            slept += 2
        self.assertTrue(passed)

        # log multiple times with dedup
        for i in range(5):
            c.log(a1)
            c.log(a2)
            c.log(a3)
            c.log(a4)
        # this action was logged 8 days in history so should not apply towards any aggregate
        c.log(action.Action(actor_type='user', actor_id=uid, target_type='content', target_id=content_id,
                            action_type='notif_send', request_id=7, timestamp=ts - timedelta(days=8)))

        # Number of hours in the ts. 
        b = int((ts.timestamp() % (24 * 3600)) / 3600)

        # now sleep for upto 3 minutes and verify count processing worked
        # we could also just sleep for full minute but this rolling sleep
        # allows test to end earlier in happy cases
        slept = 0
        passed = False
        expected1 = 0.09452865480086611  # normalized for 1 in 2
        expected2 = 0.09452865480086611  # normalized for 1 in 2
        expected3 = 1
        expected4 = 1
        time.sleep(10)
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
        while slept < 60:
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
            q = op.std.set(q, field='groupkey', var=('e',), value=[
                var('e').target_id, var('e').city, var('e').gender, var('e').age_group
            ])
            return op.std.set(q, field='value', value=1)

        agg1.store(client=c)

        @rex.aggregate(
            name='user_creator_avg_watchtime_by_2hour_windows',
            aggregate_type='average', action_types=['view'], config={'durations': [30 * 24 * 3600, 1200]},
        )
        def agg2(actions):
            q = op.std.filter(actions, var='a', where=var('a').action_type == 'view')
            q = op.std.profile(q, var='e', field='creator_id', otype='video', oid=var('e').target_id, key='creatorId')
            q = op.std.set(q, var='e', field='time_bucket', value=var('e').timestamp % (24 * 3600) // (2 * 3600))
            q = op.std.set(q, field='groupkey', var='e',
                           value=[var('e').actor_id, var('e').creator_id, var('e').time_bucket])
            return op.std.set(q, field='value', var='e', value=var('e').metadata.watch_time)

        agg2.store(client=c)

        ts = datetime.now().astimezone(timezone.utc)

        b = int((ts.timestamp() % (24 * 3600)) / (2 * 3600))

        # send multiple times with dedup keys
        actions = [
            action.Action(actor_type='user', actor_id=uid, target_type='video', target_id=video_id, action_type='view',
                          request_id=1, timestamp=ts, metadata={'watch_time': 20}, dedup_key="action1"),
            action.Action(actor_type='user', actor_id=uid, target_type='video', target_id=video_id, action_type='view',
                          request_id=1, timestamp=ts - timedelta(days=3), metadata={'watch_time': 22},
                          dedup_key="action2"),
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
        while slept < 120:
            found1 = c.aggregate_value(
                'video_view_by_city_gender_agegroup',
                [video_id, city, gender, age_group],
                {'duration': 2 * 24 * 3600},
            )
            q1 = op.std.aggregate(
                [{'uid': uid, 'creator_id': creator_id, 'b': b}], field='found',
                name='user_creator_avg_watchtime_by_2hour_windows', var='e',
                groupkey=[var('e').uid, var('e').creator_id, var('e').b], kwargs={'duration': 30 * 24 * 3600},
            )[0].found
            found2 = c.query(q1)
            found3 = c.aggregate_value('user_creator_avg_watchtime_by_2hour_windows', [uid, creator_id, b],
                                       {'duration': 1200})
            q2 = op.std.aggregate([{'uid': uid, 'creator_id': creator_id, 'b': b}], field='found',
                                  name='user_creator_avg_watchtime_by_2hour_windows', var='e',
                                  groupkey=[var('e').uid, var('e').creator_id, var('e').b], kwargs={"duration": 1200})[
                0].found
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

        # test explode
        q = [{'a': 1, 'b': 'one'}, {'a': 2, 'b': ['two', 'three']}, {'a': 3, 'b': 'four'}]
        e1 = op.std.explode(q, field=['b'])
        e2 = op.std.explode(q, field='b')
        self.assertEqual([{'a': 1, 'b': 'one'}, {'a': 2, 'b': 'two'}, {'a': 2, 'b': 'three'}, {'a': 3, 'b': 'four'}],
                         c.query(e1))
        self.assertEqual([{'a': 1, 'b': 'one'}, {'a': 2, 'b': 'two'}, {'a': 2, 'b': 'three'}, {'a': 3, 'b': 'four'}],
                         c.query(e2))

        # test storing and running queries
        q = cond(var('x') < 3, 'left', 'right')
        q_name = 'query_name'
        c.store_query(q_name, q)
        ret = c.run_query(q_name, {'x': 1})
        self.assertEqual('left', ret)
        ret = c.run_query(q_name, {'x': 5})
        self.assertEqual('right', ret)

    @tiered
    def test_features(self):
        c = client.Client(URL)
        # first set some data
        uid = 1
        post_ids = [100 + i for i in range(10)]
        topics = ['topic1', 'topic2']
        for p in post_ids:
            c.set_profile('post', p, 'topic', topics[p % 2])
        
        slept = 0
        while slept < 60:
            passed = True
            time.sleep(2)
            slept += 2
            for p in post_ids:
                if topics[p % 2] != c.get_profile('post', p, 'topic'):
                    passed = False
                    break
            if passed:
                break
        self.assertTrue(passed)

        # and log a few actions
        now = datetime.now().astimezone(timezone.utc)

        for p in post_ids:
            # one action for 1 day ago (so applies to both 4 day and 7 day windows)
            c.log(action.Action(actor_type='user', actor_id=uid, target_type='post', target_id=p,
                                action_type='click', request_id=1, timestamp=now - timedelta(days=1)))
            # one action for 6 day ago (so applies to only 7 day windows)
            c.log(action.Action(actor_type='user', actor_id=uid, target_type='post', target_id=p,
                                action_type='click', request_id=1, timestamp=now - timedelta(days=6)))

        # now store some aggregates
        @rex.aggregate(
            name='user_clicks', action_types=['click'],
            aggregate_type='sum', config={'durations': [4 * 24 * 3600, 7 * 24 * 3600]},
        )
        def agg1(events):
            q = op.std.set(events, field='groupkey', var='e', value=var('e').actor_id)
            return op.std.set(q, field='value', value=1)

        agg1.store(client=c)

        @rex.aggregate(
            name='user_topic_clicks', action_types=['click'],
            aggregate_type='sum', config={'durations': [4 * 24 * 3600, 7 * 24 * 3600]},
        )
        def agg2(events):
            q = op.std.profile(events, field='topic', var='e', otype='post', oid=var('e').target_id, key='topic')
            q = op.std.set(q, field='groupkey', var='e', value=[var('e').actor_id, var('e').topic])
            return op.std.set(q, field='value', value=1)

        agg2.store(client=c)

        # now define some features
        @rex.feature.register('f_num_user_click_4day')
        def f1(context, candidates):
            groupkeys = op.std.map(candidates, to=context.uid)
            return agg1.compute(groupkeys, duration=4 * 24 * 3600)

        @rex.feature.register('f_num_user_click_7day')
        def f2(context, candidates):
            groupkeys = op.std.map(candidates, to=context.uid)
            return agg1.compute(groupkeys, duration=7 * 24 * 3600)

        @rex.feature.register('f_num_user_topic_click_4day')
        def f3(context, candidates):
            topics = op.std.profile(candidates, otype='post', var='e', oid=var('e').post_id, key='topic')
            groupkeys = op.std.map(topics, to=[context.uid, var('t')], var='t')
            return agg2.compute(groupkeys, duration=4 * 24 * 3600)

        @rex.feature.register('f_num_user_topic_click_7day')
        def f4(context, candidates):
            topics = op.std.profile(candidates, otype='post', var='e', oid=var('e').post_id, key='topic')
            groupkeys = op.std.map(topics, to=[context.uid, var('t')], var='t')
            return agg2.compute(groupkeys, duration=7 * 24 * 3600)

        @rex.feature.register('f_content_topic')
        def f5(context, candidates):
            return op.std.profile(candidates, otype='post', var='e', oid=var('e').post_id, key='topic')

        context = {'uid': uid}
        candidates = [{'post_id': p} for p in post_ids]
        names = [f3.name, f4.name, f5.name, f1.name, f2.name]
        expected_vec = [[5, 10, topics[p % 2], 10, 20] for p in post_ids]
        expcted_dict = [{f3.name: 5, f4.name: 10, f5.name: topics[p % 2], f1.name: 10, f2.name: 20} for p in post_ids]

        slept = 0
        found = False
        while not found and slept < 120:
            found_dict_query = rex.feature.extract(context, candidates, names=names)
            found_vec = op.std.collect(found_dict_query, fields=names)
            found_vec = c.query(found_vec)
            found_dict = c.query(found_dict_query)
            if found_vec == expected_vec and found_dict == expcted_dict:
                found = True
                print('all checks passed...')
            else:
                time.sleep(5)

        self.assertTrue(found)


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
            q = op.std.set(q, field='groupkey', var=('e',), value=[
                var('e').target_id, var('e').city, var('e').gender, var('e').age_group
            ])
            return op.std.set(q, field='value', value=1)

        agg1.store(client=c)

        @rex.aggregate(
            name='user_creator_avg_watchtixme_by_2hour_windows',
            aggregate_type='average', action_types=['view'], config={'durations': [30 * 24 * 3600]},
        )
        def agg2(actions):
            q = op.std.filter(actions, var='a', where=var('a').action_type == 'view')
            q = op.std.profile(q, var='e', field='creator_id', otype='video', oid=var('e').target_id, key='creatorId')
            q = op.std.set(q, var='e', field='time_bucket', value=var('e').timestamp % (24 * 3600) // (2 * 3600))
            q = op.std.set(q, field='groupkey', var='e',
                           value=[var('e').actor_id, var('e').creator_id, var('e').time_bucket])
            return op.std.set(q, field='value', var='e', value=var('e').metadata.watch_time)

        agg2.store(client=c)


if __name__ == '__main__':
    unittest.main()