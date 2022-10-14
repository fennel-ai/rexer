#!/usr/bin/env python

import dataclasses
import functools
import os
import random
import string
import unittest
from typing import Any
import time
from datetime import datetime, timezone, timedelta
import lib
import signal
import sys
from threading import Event

# Url of the local server
URL = 'http://localhost:2425'
class LocalTier():
    def __init__(self, is_dev_tier=False, tier_id=None):
        self.grpc_process = lib.Process(None, None, None, None)
        self.env = os.environ.copy()
        self.is_dev_tier = is_dev_tier
        self.tier_id = tier_id
        signal.signal(signal.SIGINT, self.kill_process)

    def run_local_server(self):
        if self.is_dev_tier:
            tier_id = self.tier_id
        else:
            tier_id = random.randint(0, 1e8)
        print("Starting local server with tier_id: {}".format(tier_id))
        self.env['TIER_ID'] = str(tier_id)
        with lib.gorun('fennel/featurestore/test/cmds/tiergod', 'dynamic,integration', self.env, flags=['--mode', 'create'], wait=True):
            pass
        print("Tier id", tier_id)
        self.env['METRICS_PORT'] = str(2436)
        self.env['PPROF_PORT'] = str(2437)
        time.sleep(30)
        self.grpc_process = lib.run('fennel/service/grpc', 'dynamic,integration', self.env)
        # Wait for the services to be up.
        time.sleep(10)
        print("Server is up")
        Event().wait()

    def kill_process(self, signum, frame):
        print('You pressed Ctrl+C!')
        self.grpc_process.kill()
        if not self.is_dev_tier:
            with lib.gorun('fennel/featurestore/test/cmds/tiergod', 'dynamic,integration', self.env, flags=['--mode', 'destroy'],
                        wait=True):
                pass

        sys.exit(0)


if __name__ == '__main__':
    if len(sys.argv) == 1 or sys.argv[1] == 'local_test':
        local_tier = LocalTier(is_dev_tier=False)
        local_tier.run_local_server()
    elif sys.argv[1] == 'dev':
        if len(sys.argv) == 3:
            tier_id = int(sys.argv[2])
        else:
            tier_id = 106
        local_tier = LocalTier(is_dev_tier=True, tier_id=tier_id)
        local_tier.run_local_server()
    else:
        print("Unknown argument")
