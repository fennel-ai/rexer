#!/usr/bin/env python
import argparse
import os
import subprocess
import platform

parser = argparse.ArgumentParser()
parser.add_argument("--integration", action='store_true', help="run tests in integration mode whenever possible")
parser.add_argument("--py-only", action='store_true', help="only run python tests")
parser.add_argument("--no-e2e", action="store_true", help="do not run e2e tests")
parser.add_argument("--run-e2e-staging", action="store_true", help="do not run e2e staging tests")
args = parser.parse_args()
integration = args.integration
py_only = args.py_only
no_e2e = args.no_e2e
run_e2e_staging = args.run_e2e_staging

root = os.getcwd()
godir = os.path.join(root, 'go/fennel')

if not py_only:
    print('Running go %stests...' % ('integration ' if integration else ''))
    print('-' * 50)
    tags = []
    if platform.processor() in ['arm', 'arm64']:
        tags.append('dynamic')
    if integration:
        tags.append('integration')
    if tags:
        p1 = subprocess.Popen(['go test -p 5 -tags %s ./...' % (','.join(tags))], shell=True, cwd=godir)
    else:
        p1 = subprocess.Popen(['go test -p 5 ./...'], shell=True, cwd=godir)
    p1.wait()

rexerclient = os.path.join(root, '../rexer-pyclient-alpha/')
print('Running python tests in rexerclient...')
print('-' * 50)
p3 = subprocess.Popen(['poetry install && poetry run python -m unittest discover rexerclient'], shell=True, cwd=rexerclient)
p3.wait()

if not no_e2e:
    print('Running e2e integration tests with python client...')
    print('-' * 50)
    p4 = subprocess.Popen(['poetry install && poetry run python -m unittest discover ../rexer/e2etests/ -p "teste2e.py"'], shell=True, cwd=rexerclient)
    p4.wait()

if run_e2e_staging:
    print('Running e2e test against staging tier with python client...')
    print('-' * 50)
    p5 = subprocess.Popen(['poetry install && poetry run python -m unittest discover ../rexer/e2etests/ -p "teste2e_staging.py"'], shell=True, cwd=rexerclient)
    p5.wait()

print('=' * 50)
print('Summary:')
print('=' * 50)
if not py_only:
    print('Go tests %s' % ('PASS' if p1.returncode == 0 else 'FAIL'))
print('Pyconsole tests %s' % ('PASS' if p2.returncode == 0 else 'FAIL'))
print('Pyclient tests %s' % ('PASS' if p3.returncode == 0 else 'FAIL'))
if not no_e2e:
    print('Pye2e tests %s' % ('PASS' if p4.returncode == 0 else 'FAIL'))
if run_e2e_staging:
    print('Pye2eStaging tests %s' % ('PASS' if p5.returncode == 0 else 'FAIL'))
