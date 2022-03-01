#!/bin/bash

# Generate python bindings
protoc -I=. --python_out=../../rexer-pyclient-alpha/rexerclient/gen ftypes.proto ast.proto

# Generate go bindings
protoc -I=. --go_out=../go/ ./*.proto
