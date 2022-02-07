#!/usr/bin/env python
import argparse
import os
import subprocess
import platform

parser = argparse.ArgumentParser()
parser.add_argument("--integration", action='store_true', help="run tests in integration mode whenever possible")
args = parser.parse_args()
integration = args.integration

root = os.getcwd()
godir = os.path.join(root, 'go/fennel')

print('Running go %stests...' % ('integration ' if integration else ''))
print('-' * 50)
tags = []
if platform.processor() in ['arm', 'arm64']:
    tags.append('dynamic')
if integration:
    tags.append('integration')
if tags:
    p1 = subprocess.Popen(['go test -p 1 -tags %s ./...' % (','.join(tags))], shell=True, cwd=godir)
else:
    p1 = subprocess.Popen(['go test -p 1 ./...'], shell=True, cwd=godir)
p1.wait()

pydir = os.path.join(root, 'pyconsole')
print('Running python tests in pyconsole...')
print('-' * 50)
p2 = subprocess.Popen(['poetry run python -m unittest'], shell=True, cwd=pydir)
p2.wait()

rexerclient = os.path.join(root, '../rexer-pyclient-alpha/')
print('Running python tests in rexerclient...')
print('-' * 50)
p3 = subprocess.Popen(['poetry run python -m unittest discover rexerclient'], shell=True, cwd=rexerclient)
p3.wait()

print('=' * 50)
print('Summary:')
print('=' * 50)
print('Go tests %s' % ('PASS' if p1.returncode == 0 else 'FAIL'))
print('Pyconsole tests %s' % ('PASS' if p2.returncode == 0 else 'FAIL'))
print('Pyclient tests %s' % ('PASS' if p3.returncode == 0 else 'FAIL'))
