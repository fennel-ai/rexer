#!/bin/bash

export MOTHERSHIP_ID=42
export MOTHERSHIP_MYSQL_ADDRESS=127.0.0.1
export MOTHERSHIP_MYSQL_DBNAME=controldb_dev
export MOTHERSHIP_MYSQL_USERNAME=dev
export MOTHERSHIP_MYSQL_PASSWORD=jumpstartml
export MOTHERSHIP_ENDPOINT=http://localhost:8080
export BRIDGE_SESSION_KEY=secret
export SENDGRID_API_KEY="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"
export GIN_MODE=debug

pushd ./go/fennel >/dev/null
go run --tags=dynamic fennel/service/console/
popd >/dev/null
