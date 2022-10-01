#!/bin/bash

function version { echo "$@" | awk -F. '{ printf("%d%03d%03d\n", $1,$2,$3); }'; }

protoc_version=`/usr/local/bin/protoc --version`
IFS=', ' read -r -a array <<< "${protoc_version}"
cwd=$(pwd)

# Installation takes 15-20min.
# If your installation fails please follow the guide at https://github.com/protocolbuffers/protobuf/tree/main/src
# to install the latest version.
if [ $(version ${array[1]}) -lt $(version "3.20.0") ]
then
    echo "Protoc version needs update"
    curl --silent --location --remote-name https://github.com/protocolbuffers/protobuf/releases/download/v21.1/protobuf-all-21.1.tar.gz
    tmp_dir=$(mktemp -d -t ci-XXX)
    tar -zxf protobuf-all-21.1.tar.gz -C $tmp_dir
    rm -rf protobuf-all-21.1.tar.gz
    cd $tmp_dir
    cd protobuf-3.21.1
    ./autogen.sh
    ./configure --prefix=/usr/local
    make -j 24
    make check -j 24
    sudo make install -j 24
    echo "installation done"
    cd $cwd
fi

# Generate python bindings
/usr/local/bin/protoc -I=. --python_out=../../rexer-pyclient-alpha/rexerclient/gen ftypes.proto ast.proto

# Generate go bindings
/usr/local/bin/protoc -I=. --go_out=../go/ ./*.proto

# Generate grpc go bindings
# TODO(mohit): Had to remove to avoid conflicts with vitess compiled code
# /usr/local/bin/protoc -I=. --go-grpc_out=../go/ ./*.proto

# Generate vitess compiled go bindings for NitrousOp
/usr/local/bin/protoc -I=. --go_out=../go/ --go-vtproto_out=../go/ --go-vtproto_opt=pool=fennel/nitrous/rpc.NitrousOp ./*.proto

prepare_libs=`cat <<EOF
import sys

swap_module = {}
for k, v in sys.modules.items():
  if k.startswith("google.protobuf"):
    swap_module[k] = v

for k in swap_module.keys():
  del sys.modules[k]

site_package_path = ""
for p in sys.path:
  if p.endswith("site-packages"):
    site_package_path = p

if site_package_path != "":
  sys.path = [site_package_path + "/rex"] + sys.path
else:
  raise Exception("site package path not found")

EOF
`

restore_libs=`cat <<EOF

del sys.path[0]

for k in list(sys.modules.keys()):
  if k.startswith("google.protobuf"):
    del sys.modules[k]

for k, v in swap_module.items():
  sys.modules[k] = v

swap_module.clear()
EOF
`

client_gen_directory="../../rexer-pyclient-alpha/rexerclient/gen/*"
for file in $client_gen_directory; do
    if [[ $file == *.py ]]
    then
      echo "${restore_libs}" >> $file
      cat <(echo "${prepare_libs}") $file > tmpfile
      mv tmpfile $file
      sed -i 's/from google.protobuf/from rex.google.protobuf/' $file
    fi
done
