import random
import time
import unittest

from rexerclient.rql import var, op
import rexerclient as rex
from rexerclient import client
from rexerclient.models import action, profile


_URL = "http://k8s-t106-aest106e-8954308bfc-65423d0e968f5435.elb.us-west-2.amazonaws.com/data"


class TestEndToEnd(unittest.TestCase):
    def test_feature_logging(self):
        c = client.Client(_URL)
        
        # Create few profiles
        profiles = []
        for _ in range(1000):
            profiles.append(profile.Profile(otype="user", oid=random.randint(0, 1000000000), key="location_id", value=random.randint(0, 1000)))

        # profiles for posts are not required
        c.set_profiles(profiles)

        # Feature 1 - `notif_open_in_user_location_1d`
        @rex.aggregate(
            name='notif_open_in_user_location_1d',
            aggregate_type='sum', action_types=['notif_open'], config={'durations': [1 * 24 * 3600]},
        )
        def notif_open_in_user_location_1d(actions):
            q = op.std.profile(actions, field='location_id', otype='user', key='location_id', var='e', oid=var('e').actor_id)
            q = op.std.set(q, var='e', field='groupkey', value=var('e').location_id)
            return op.std.set(q, field='value', value=1)
        notif_open_in_user_location_1d.store(client=c)

        # Feature 2 - `user_notif_open_1d`
        @rex.aggregate(
            name='user_notif_open_1d',
            aggregate_type='sum', action_types=['notif_open'], config={'durations': [1 * 24 * 3600]},
        )
        def user_notif_open_1d(actions):
            q = op.std.set(actions, var='e', field='groupkey', value=var('e').actor_id)
            return op.std.set(q, field='value', value=1)
        user_notif_open_1d.store(client=c)

        # Feature 3 - `post_view_time_1d`
        @rex.aggregate(
            name='post_view_time_1d',
            aggregate_type='sum', action_types=['view'], config={'durations': [1 * 24 * 3600]},
        )
        def post_view_time_1d(actions):
            q = op.std.set(actions, var='e', field='groupkey', value=var('e').target_id)
            return op.std.set(q, field='value', var='e', value=var('e').metadata.watch_time)
        post_view_time_1d.store(client=c)

        # Log actions
        actions = []
        action_pair = [{'user_id': 100, 'post_id': 123, 'request_id': 234, 'timestamp': 12344}]
        for _ in range(10000):
            user_id = profiles[random.randint(0, 999)].oid
            post_id = random.randint(0, 100000000)
            request_id = random.randint(0, 1000000)
            timestamp = int(time.time())
            action_type = "notif_open"
            x = random.randint(0, 1)
            if x == 0:
                action_type = "view"
            actions.append(action.Action(
                actor_id=user_id, actor_type="user", target_id=post_id, target_type="post", action_type=action_type,
                request_id=request_id, timestamp=timestamp, metadata={'watch_time': random.randint(0, 100)}))
            action_pair.append({'user_id': user_id, 'post_id': post_id, 'request_id': request_id, 'timestamp': timestamp})

        c.log_multi(actions)

        time.sleep(100.0)

        # sleep for a while before features are logged
        print('sleeping for ~100 seconds - this is fine (users will have actions flowing and will have some feature values)!')

        with_location = op.std.profile(action_pair, field='location_id', otype='user', key='location_id', var='e', oid=var('e').user_id)
        with_feature1 = op.std.aggregate(with_location, name='notif_open_in_user_location_1d', field='f1', var='e', groupkey=var('e').location_id, kwargs={'duration': 1 * 24 * 3600})
        with_feature2 = op.std.aggregate(with_feature1, name='user_notif_open_1d', field='f2', var='e', groupkey=var('e').user_id, kwargs={'duration': 1 * 24 * 3600})
        with_feature3 = op.std.aggregate(with_feature2, name='post_view_time_1d', field='f3', var='e', groupkey=var('e').post_id, kwargs={'duration': 1 * 24 * 3600})
        c.query(op.feature.log(with_feature3,
            var='e',
            context_otype="user",
            context_oid=var('e').user_id,
            candidate_otype="post",
            candidate_oid=var('e').post_id,
            workflow="click_prediction",
            request_id=var('e').request_id,
            timestamp=var('e').timestamp,
            features = {
                'f1': var('e').f1,
                'f2': var('e').f2,
                'f3': var('e').f3
            }
        ))
        print('========== logged, waiting for 10 seconds to finish\n')
        time.sleep(10.0)
