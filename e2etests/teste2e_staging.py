from enum import Enum
import os
import time
import unittest

import lib

import rexerclient as rex
from rexerclient import client
from rexerclient.models import model
from rexerclient.rql import op


# URL to the staging tier LB
_URL = "http://k8s-t106-aest106e-8954308bfc-65423d0e968f5435.elb.us-west-2.amazonaws.com/data"
_AGGREGATE_NAME = "views_per_user_aggr"
_USER_ID = 245771976
_NUM_ACTIONS = 20
# Load test log these many actions in a second for a minute
_NEW_ACTIONS = 1200
_NUM_PROCS = 5

_FLAGS = ['--url', _URL, '--num_uids', "1", '--uid', f'{_USER_ID}', '--qps', f'{_NUM_ACTIONS}', '--num_procs', f'{_NUM_PROCS}']


class _Status(Enum):
    LAG = 1
    VALIDATING = 2
    DONE = 3


class TestStagingEndToEnd(unittest.TestCase):
    @unittest.skip
    def test_viewtime(self):
        c = rex.Client(_URL)  

        @rex.aggregate(
            name=_AGGREGATE_NAME, aggregate_type='sum',
            action_types=['e2etest_view'], config={'durations':[3600*24]},
        )
        def agg(actions):
            view_events = rex.op.std.filter(actions, var='a', where=rex.var('a').action_type == 'e2etest_view')
            with_key = rex.op.std.set(view_events, var='e', field='groupkey', value=rex.var('e').actor_id)
            return rex.op.std.set(with_key, field='value', value=1)
        # # Store aggregate, if this store already exists (with the same options), this is a no-op
        agg.store(client=c)
        # c.store_aggregate(_AGGREGATE_NAME, with_val, options)
        c.set_profile("user", _USER_ID, "age", 24)

        # Query for this aggregates value
        init_val = c.aggregate_value(_AGGREGATE_NAME, _USER_ID, {'duration': 24*3600})
        print('Initial value of the aggregate: ', init_val)

        # Generate traffic
        with lib.gorun(path='fennel/test/cmds/loadtest', tags='dynamic', env=os.environ.copy(), wait=True, flags=_FLAGS):
            # Query for the value as the loadtest runs simulateneously. We query for 120 seconds after which
            # we abort if the value is not reached.
            status = _Status.LAG
            validating_cnt = 0
            val_now = init_val
            while status != _Status.DONE:
                val_now = c.aggregate_value(_AGGREGATE_NAME, _USER_ID, {'duration': 24*3600})
                print('val_now: ', val_now)
                if val_now > init_val + _NEW_ACTIONS:
                    # There were more actions than expected, fail.
                    break
                # We have found all the actions, but wait for a while to validate that there aren't anymore.
                if val_now == init_val + _NEW_ACTIONS:
                    if status == _Status.VALIDATING:
                        validating_cnt += 1
                    status = _Status.VALIDATING
                    if validating_cnt >= 5:
                        break
                    print('Found expected actions, validating to see if the actual actions are more than expected.')
                time.sleep(5.0)

            self.assertEqual(val_now, init_val + _NEW_ACTIONS)


    # TODO(mohit): Allow running this test once CI is setup with necessary permissions and keys
    @unittest.skip
    def test_model_upload_predict(self):
        c = rex.Client(_URL)
        dir_path = os.path.dirname(os.path.realpath(__file__))
        m = model.XGBoostModel(os.path.join(dir_path, 'model'), '1.3-1')

        uploaded = False
        for _ in range(20):
            try:
                c.upload_model('name', 'v2', m)
                uploaded = True
            except client.RetryError as err:
                print(f"Retrying in 60s due to error: {err}")
                time.sleep(60)
                continue
            break
        self.assertTrue(uploaded)

        found = None
        for _ in range(20):
            try:
                q = op.std.predict([1], features=[
                    0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 1, 0, 0, 1, 0,
                    0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0,
                    0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 1, 0, 0,
                    1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0,
                    0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0,
                    0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 1,
                    0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0,
                ], model_name='name', model_version='v2')
                found = c.query(q)
                self.assertEqual(1, len(found))
            except client.HTTPError:
                print("Retrying to score the model in 60s")
                time.sleep(60)
                continue
            break
        self.assertEqual(1, len(found))

        # TODO(mohit): Consider adding back model deletion.
        # Currently upload and deleting a model takes ~10-15 mins for creating sagemaker endpoint
        # for the e2e testing purpose, we just upload the model (which is an idempotent operation)
        # and run predictions against it 
