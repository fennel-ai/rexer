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


class Servers(object):
    def __init__(self, tier_id):
        self.tier_id = tier_id

    def __enter__(self):
        env = os.environ.copy()
        env['TIER_ID'] = str(self.tier_id)
        self.p2 = subprocess.Popen(['go', 'run', '--tags', 'dynamic,integration', 'fennel/service/countaggr'],
                              cwd=GODIR, stderr=subprocess.PIPE, stdout=sys.stdout, text=True, env=env)
        self.p1 = subprocess.Popen(['go', 'run', '--tags', 'dynamic,integration', 'fennel/service/http'],
                                   cwd=GODIR, stderr=subprocess.PIPE, stdout=sys.stdout, text=True, env=env)
        # wait/iterate on input long enough for servers to come up
        with self.p1.stderr as f:
            for line in f:
                if 'server is ready' in line:
                    break
        with self.p2.stderr as f:
            for line in f:
                if 'server is ready' in line:
                    break

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.p1.terminate()
        finally:
            self.p1.wait()
        try:
            self.p2.terminate()
        finally:
            self.p2.wait()


def tiered(wrapped):
    @functools.wraps(wrapped)
    def fn(*args, **kwargs):
        tid = random.randint(0, 1e8)
        with TestTier(tid):
            with Servers(tid):
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
          Ops.std.filter(where=it.action_type == String('view')),
          Ops.std.addProfileColumn(name=String('city'), otype=String('user'), oid=it.actor_id, key=String('city')),
          Ops.std.addProfileColumn(name=String('gender'), otype=String('user'), oid=it.actor_id, key=String('gender')),
          Ops.std.addProfileColumn(name=String('age_group'), otype=String('user'), oid=it.actor_id, key=String('age_group')),
          Ops.std.addColumn(name=String('key'), value=List(it.target_id, it.city, it.gender, it.age_group)),
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


if __name__ == '__main__':
    unittest.main()
