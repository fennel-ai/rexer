#!/usr/bin/env python
"""Runs teste2e.py with local server side changes against different client versions.

NOTE: Any uncommitted changes in the client are removed

NOTE: Current client versions are merely commits i.e. we reset the HEAD to a commit and run the tests using it

This should be used before making releases to the servers to ensure the client is not broken
"""
import argparse
import os
import subprocess

parser = argparse.ArgumentParser()
parser.add_argument("--commits", help="number of commits to rollback to, to test client v/s server versioning compatibility")
parser.add_argument("--commit_hash", help="Commit hash of the client to run the e2e test against")
args = parser.parse_args()
commits = args.commits
commit_hash = args.commit_hash

root = os.getenv("FENNEL_ROOT")
godir = os.path.join(root, 'go/fennel')


def run_test(commit: str):
    print('moving client version to: ')
    p = subprocess.Popen([f'git reset --hard {commit} && git rev-parse HEAD'], shell=True, cwd=rexerclient)
    p.wait()
    
    print('Running e2e integration tests with python client...')
    print('-' * 50)
    p = subprocess.Popen(['poetry install && poetry run python -m unittest discover ../rexer/e2etests/ -p "teste2e.py"'], shell=True, cwd=rexerclient)
    p.wait()
    print('Pye2e tests %s' % ('PASS' if p.returncode == 0 else 'FAIL'))

    # reset the HEAD -> do a git pull
    p = subprocess.Popen(['git pull'], shell=True, cwd=rexerclient)
    p.wait()


rexerclient = os.path.join(root, '../rexer-pyclient-alpha/')
if commit_hash:
    run_test(commit=commit_hash)
else:
    for i in range(int(commits)):
        run_test(commit=f'HEAD~{i}')
