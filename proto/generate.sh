#!/bin/bash

# Generate python bindings
protoc --proto_path=./ --python_out=../../rexer-pyclient-alpha/gen *.proto

# Generate go bindings
protoc -I=. --go_out=../go/ ./*.proto
