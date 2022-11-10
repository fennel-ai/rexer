#!/bin/bash

source local_bridge.rc
pushd ./go/fennel >/dev/null
go run --tags=dynamic fennel/service/bridge/
popd >/dev/null
