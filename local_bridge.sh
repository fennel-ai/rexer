#!/bin/bash

MOTHERSHIP_ID=42
MOTHERSHIP_MYSQL_ADDRESS=127.0.0.1
MOTHERSHIP_MYSQL_DBNAME=controldb_dev
MOTHERSHIP_MYSQL_USERNAME=dev
MOTHERSHIP_MYSQL_PASSWORD=jumpstartml

pushd ./go/fennel >/dev/null
go run --tags=dynamic fennel/service/bridge/
popd >/dev/null
