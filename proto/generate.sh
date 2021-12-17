#!/bin/bash

# Generate python bindings
protoc --proto_path=./ --python_out=../pyclient/gen *.proto

# Generate go bindings
protoc -I=. --go_out=../ ./*.proto
