#!/usr/bin/env python
import argparse
import os
import subprocess
import platform


root = os.getcwd()
godir = os.path.join(root, 'go/fennel')
print('Running go tests...')
print('-' * 50)
if platform.processor() in [ 'arm', 'arm64']:
    p1 = subprocess.Popen(['go test -tags dynamic ./...'], shell=True, cwd=godir)
else:
    p1 = subprocess.Popen(['go test ./...'], shell=True, cwd=godir)
p1.wait()

pydir = os.path.join(root, 'pyconsole')
print('Running python tests in pyconsole...')
print('-' * 50)
p2 = subprocess.Popen(['poetry run python -m unittest'], shell=True, cwd=pydir)
p2.wait()

pyclient = os.path.join(root, '../rexer-pyclient-alpha/rexerclient')
print('Running python tests in pyclient...')
print('-' * 50)
p3 = subprocess.Popen(['poetry run python -m unittest'], shell=True, cwd=pyclient)
p3.wait()

print('=' * 50)
print('Summary:')
print('=' * 50)
print('Go tests %s' % ('PASS' if p1.returncode == 0 else 'FAIL'))
print('Pyconsole tests %s' % ('PASS' if p2.returncode == 0 else 'FAIL'))
print('Pyclient tests %s' % ('PASS' if p3.returncode == 0 else 'FAIL'))
