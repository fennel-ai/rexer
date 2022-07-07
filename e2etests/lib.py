from typing import Optional

import contextlib
import os
import random
import string
import subprocess
import sys
import time
import dataclasses
from typing import Any, List


ROOT = os.getenv("FENNEL_ROOT")
GODIR = os.path.join(ROOT, 'go/fennel')

class Process():
    process: Any
    flags: Any
    path: str
    binary: Any

    def __init__(self, process, path, flags, binary):
        self.path = path
        self.flags = flags
        self.binary = binary
        self.process = process


    def kill(self):
        print('going to kill:', self.path, ' '.join(self.flags))
        self.process.kill()
        self.process.wait()
        os.remove(self.binary)
        print('done killing:', self.path, ' '.join(self.flags))


def run(path, tags, env, flags=None, wait=False, sleep=0, cwd: str = GODIR) -> Process:
    if flags is None:
        flags = []

    dir = randname(8)
    binary = '/tmp/%s/%s' % (dir, path)
    print('going to build:', path)
    b = subprocess.Popen(['go', 'build', '--tags', tags, '-o', binary, path], cwd=cwd)
    b.wait()
    print('build: ', 'success' if b.returncode == 0 else 'fail')
    print('going to run:', path, ' '.join(flags))
    p = subprocess.Popen([binary] + flags, env=env, cwd=cwd) 
    return Process(p, path, flags, binary)


@contextlib.contextmanager
def gorun(path, tags, env, flags=None, wait=False, sleep=0, cwd: str = GODIR):
    if flags is None:
        flags = []

    dir = randname(8)
    binary = '/tmp/%s/%s' % (dir, path)
    print('going to build:', path)
    b = subprocess.Popen(['go', 'build', '--tags', tags, '-o', binary, path], cwd=cwd)
    b.wait()
    print('build: ', 'success' if b.returncode == 0 else 'fail')
    print('going to run:', path, ' '.join(flags))
    #p = subprocess.Popen([binary] + flags, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL, env=env)
    p = subprocess.Popen([binary] + flags, stderr=sys.stderr, stdout=sys.stdout, env=env)
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


def randname(n: int = 8) -> str:
    return ''.join(random.choice(string.ascii_lowercase) for _ in range(n))
