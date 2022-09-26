#!/bin/bash

export MOTHERSHIP_ID=42
export MOTHERSHIP_MYSQL_ADDRESS=127.0.0.1
export MOTHERSHIP_MYSQL_DBNAME=controldb_dev
export MOTHERSHIP_MYSQL_USERNAME=dev
export MOTHERSHIP_MYSQL_PASSWORD=jumpstartml
export BRIDGE_SESSION_KEY=secret
export SENDGRID_API_KEY="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"
export BRIDGE_ENV=dev

# lokal api url
export API_URL=http://k8s-t107-aest107e-c969e2b35d-e2cc681d58e1e1ca.elb.ap-south-1.amazonaws.com/data 

pushd ./go/fennel >/dev/null
go run --tags=dynamic fennel/backfill/mothership/setup_tier_dev
popd >/dev/null
