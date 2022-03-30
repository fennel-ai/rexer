from typing import Optional

import contextlib
import os
import random
import string
import subprocess
import sys
import time


ROOT = os.getenv("FENNEL_ROOT")
GODIR = os.path.join(ROOT, 'go/fennel')


@contextlib.contextmanager
def gorun(path, tags, env, flags=None, wait=False, sleep=0, cwd: str = GODIR):
    if flags is None:
        flags = []

    dir = ''.join(random.choice(string.ascii_lowercase) for _ in range(8))
    binary = '/tmp/%s/%s' % (dir, path)
    print('going to build:', path)
    b = subprocess.Popen(['go', 'build', '--tags', tags, '-o', binary, path], cwd=cwd)
    b.wait()
    print('build: ', 'success' if b.returncode == 0 else 'fail')
    print('going to run:', path, ' '.join(flags))
    p = subprocess.Popen([binary] + flags, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL, env=env)
    #p = subprocess.Popen([binary] + flags, stderr=sys.stderr, stdout=sys.stdout, env=env)
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
