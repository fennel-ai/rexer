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

URL = 'http://localhost:2425'

# env = os.environ.copy()

# def signal_handler(sig, frame):
#     print('You pressed Ctrl+C!')
#     lib.gorun('fennel/test/cmds/tiergod', 'dynamic,integration', env, flags=['--mode', 'destroy'],
#                        wait=True)
#     sys.exit(0)




class LocalTier():
    def __init__(self, flags=None):
        self.http_process = lib.Process(None, None, None, None)
        self.countaggr_process = lib.Process(None, None, None, None)
        self.env = os.environ.copy()
        signal.signal(signal.SIGINT, self.kill_process)

    def run_local_server(self):
        tier_id = random.randint(0, 1e8)
        self.env['TIER_ID'] = str(tier_id)
        with lib.gorun('fennel/test/cmds/tiergod', 'dynamic,integration', self.env, flags=['--mode', 'create'], wait=True):
            pass
        print("Tier id", tier_id)
        self.env['METRICS_PORT'] = str(2436)
        self.env['PPROF_PORT'] = str(2437)
        self.env['BADGER_DIR'] = '/tmp/badger/' + lib.randname(5)
        self.http_process = lib.run('fennel/service/http', 'dynamic,integration', self.env)
        self.env['METRICS_PORT'] = str(2446)
        self.env['PPROF_PORT'] = str(2467)
        self.env['BADGER_DIR'] = '/tmp/badger/' + lib.randname(5)
        self.countaggr_process = lib.run('fennel/service/countaggr', 'dynamic,integration', self.env)  
        # Wait for the services to be up.
        time.sleep(10)
        print("Server is up")
        Event().wait()

    def kill_process(self, signum, frame):
        print('You pressed Ctrl+C!')
        self.countaggr_process.kill()
        self.http_process.kill()
        with lib.gorun('fennel/test/cmds/tiergod', 'dynamic,integration', self.env, flags=['--mode', 'destroy'],
                       wait=True):
            pass

        sys.exit(0)


if __name__ == '__main__':
    local_tier = LocalTier()
    local_tier.run_local_server()

    #local_tier.run_local_server()
