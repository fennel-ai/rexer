#!/usr/bin/env python
import os
import subprocess
import sys

root = os.getcwd()
godir = os.path.join(root, 'go/fennel')
print('Running go tests...')
print('-' * 50)
p1 = subprocess.Popen(['go test -tags dynamic -p 1 ./...'], shell=True, cwd=godir)
p1.wait()

pydir = os.path.join(root, 'pyclient')
print('Running python tests...')
print('-' * 50)
p2 = subprocess.Popen(['poetry run python -m unittest'], shell=True, cwd=pydir)
p2.wait()

print('=' * 50)
print('Summary:')
print('=' * 50)
print('Go tests %s' % ('PASS' if p1.returncode == 0 else 'FAIL'))
print('Python tests %s' % ('PASS' if p2.returncode == 0 else 'FAIL'))
