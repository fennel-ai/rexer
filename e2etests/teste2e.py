import contextlib
import string
import sys

import functools

import os
import random
import subprocess
import time
import unittest

from rexerclient.rql import Var, Int, String, it, Ops, List
from rexerclient import client, models, value


ROOT = os.getenv("FENNEL_ROOT")
GODIR = os.path.join(ROOT, 'go/fennel')
URL = 'http://localhost:2425'


class TestTier(object):
    def __init__(self, tier_id):
        self.tier_id = tier_id

    def __enter__(self):
        cmd = 'TIER_ID=%d bash -c "go run --tags dynamic,integration fennel/test/cmds/tiergod --mode create"' % self.tier_id
        subprocess.Popen(cmd, shell=True, cwd=GODIR).wait()

    def __exit__(self, exc_type, exc_val, exc_tb):
        cmd = 'TIER_ID=%d bash -c "go run --tags dynamic,integration fennel/test/cmds/tiergod --mode destroy"' % self.tier_id
        subprocess.Popen(cmd, shell=True, cwd=GODIR).wait()


@contextlib.contextmanager
def gorun(path, tags, env):
    dir = ''.join(random.choice(string.ascii_lowercase) for _ in range(8))
    binary = '/tmp/%s/%s' % (dir, path)
    b = subprocess.Popen(['go', 'build', '--tags', tags, '-o', binary, path], cwd=GODIR)
    b.wait()
    p = subprocess.Popen([binary], stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL, env=env)
    # p = subprocess.Popen([binary], stderr=sys.stderr, stdout=sys.stdout, env=env)
    yield
    try:
        p.kill()
    finally:
        p.wait()
    os.remove(binary)


def tiered(wrapped):
    @functools.wraps(wrapped)
    def fn(*args, **kwargs):
        tier_id = random.randint(0, 1e8)
        with TestTier(tier_id):
            env = os.environ.copy()
            env['TIER_ID'] = str(tier_id)
            with gorun('fennel/service/http', 'dynamic,integration', env):
                with gorun('fennel/service/countaggr', 'dynamic,integration', env):
                    return wrapped(*args, **kwargs)
    return fn


class TestEndToEnd(unittest.TestCase):
    @tiered
    def test_end_to_end(self):
        c = client.Client(URL)
        uid = 12312
        video_id = 456
        city = value.String('delhi')
        gender = value.Int(1)
        age_group = value.Int(3)

        # for entity which is of type "user" and user_id 12312, set "age" to be 31
        c.set_profile("user", uid, "city", city)
        c.set_profile("user", uid, "gender", gender)
        c.set_profile("user", uid, "age_group", age_group)

        self.assertEqual(city, c.get_profile("user", uid, "city"))
        self.assertEqual(gender, c.get_profile("user", uid, "gender"))
        self.assertEqual(age_group, c.get_profile("user", uid, "age_group"))

        # Total views gained by a Trail on last 2 days for given city+gender+age_group
        q = Var('args').actions.apply(
          Ops.std.filter(where=(it.action_type == 'view') & (it.target_type == 'video')),
          Ops.std.addProfileColumn(name='city', otype='user', oid=it.actor_id, key='city'),
          Ops.std.addProfileColumn(name='gender', otype='user', oid=it.actor_id, key='gender'),
          Ops.std.addProfileColumn(name='age_group', otype='user', oid=it.actor_id, key='age_group'),
          Ops.std.addColumn(name='key', value=List(it.target_id, it.city, it.gender, it.age_group)),
        )

        options = {'duration': 3600*24*2, 'aggregate_type': 'rolling_counter', }
        c.store_aggregate('trail_view_by_city_gender_agegroup_2days', q, options)

        c.log(actor_type='user', actor_id=uid, target_type='video', target_id=video_id, action_type='view',
              request_id=1, timestamp=int(time.time()), metadata=value.Dict(device_type=value.String('android')),
        )
        time.sleep(60)

        found = c.aggregate_value(
            'trail_view_by_city_gender_agegroup_2days',
            value.List(value.Int(video_id), city, gender, age_group),
        )
        self.assertEqual(value.Int(1), found)


@unittest.skip
class TestLoad(unittest.TestCase):
    @tiered
    def test_load(self):
        c = client.Client(URL)
        self.set_aggregates(c)
        with gorun('fennel/test/cmds/loadtest', 'dynamic', os.environ.copy()):
            pass

    def set_aggregates(self, c: client.Client):
        # Total views gained by a Trail on last 2 days for given city+gender+age_group
        q = Var('args').actions.apply(
            Ops.std.filter(where=(it.action_type == 'view') & (it.target_type == 'video')),
            Ops.std.addProfileColumn(name='city', otype='user', oid=it.actor_id, key='city'),
            Ops.std.addProfileColumn(name='gender', otype='user', oid=it.actor_id, key='gender'),
            Ops.std.addProfileColumn(name='age_group', otype='user', oid=it.actor_id, key='age_group'),
            Ops.std.addColumn(name='key', value=List(it.target_id, it.city, it.gender, it.age_group)),
        )
        options = {'duration': 3600*24*2, 'aggregate_type': 'rolling_counter', }
        c.store_aggregate('trail_view_by_city_gender_agegroup_2days', q, options)

        # Avg-watchtime of a  video for given country+OS+city_sate+mobile_brand+gender in 30 days
        q = Var('args').actions.apply(
            Ops.std.filter(where=it.action_type == 'view'),
            Ops.std.addProfileColumn(name='country', otype='user', oid=it.actor_id, key='country'),
            Ops.std.addProfileColumn(name='os', otype='user', oid=it.actor_id, key='os'),
            Ops.std.addProfileColumn(name='city', otype='user', oid=it.actor_id, key='city'),
            Ops.std.addProfileColumn(name='mobile_brand', otype='user',oid=it.actor_id, key='mobile_brand'),
            Ops.std.addProfileColumn(name='gender', otype='user', oid=it.actor_id, key='gender'),
            # Ops.std.addColumn(name=String('day_of_week'), value=Ops.time.dayOfWeek(timestamp=it.timestamp)),
            # Ops.std.addColumn(name=String('time_bucket'), value=Ops.time.hourOfDay(timestamp=it.timestamp), size=Int(3600)),
            Ops.std.addColumn(name='amount', value=it.metadata.watch_time),
            Ops.std.addColumn(name='key', value=[it.target_id, it.country, it.os, it.city, it.mobile_brand, it.gender]),
        )
        options = {'aggregate_type': 'rolling_average', 'duration': 3600*24*30}
        c.store_aggregate('video_avg_watchtime_by_country_os_citystate_mobile_gender_30days', q, options)

        # Avg-watchtime of a user id  for creatorId in 2-hour window averaged over 30 days
        q = Var('args').actions.apply(
            Ops.std.filter(where=(it.action_type == 'view') & (it.target_type == 'video')),
            Ops.std.addProfileColumn(name='creator_id', otype='user', oid=it.actor_id, key='creatorId'),
            # Ops.std.addColumn(name=String('time_bucket'), value=Ops.time.hourOfDay(timestamp=it.timestamp), size=Int(3600)),
            Ops.std.addColumn(name='amount', value=it.metadata.watch_time),
            Ops.std.addColumn(name='key', value=[it.actor_id, it.creator_id]),
        )
        options = {'aggregate_type': 'rolling_average', 'duration': 3600*24*30}
        c.store_aggregate('user_creator_avg_watchtime_by_2hour_windows_30days', q, options)


if __name__ == '__main__':
    unittest.main()
