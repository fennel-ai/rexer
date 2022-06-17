#!/bin/bash


# Generate python bindings
protoc -I=. --python_out=../../rexer-pyclient-alpha/rexerclient/gen ftypes.proto ast.proto

# Generate go bindings
protoc -I=. --go_out=../go/ ./*.proto

# Generate grpc go bindings
protoc -I=. --go-grpc_out=../go/ ./*.proto

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

#cat <echo $text
#echo -e $prepare_libs >
#sed -i 's/search_string/replace_string/' filename
client_gen_directory="../../rexer-pyclient-alpha/rexerclient/gen/*"
for file in $client_gen_directory; do
    if [[ $file == *.py ]]
    then
      first=${first/Suzy/$second}
      echo "${restore_libs}" >> $file
      cat <(echo "${prepare_libs}") $file > tmpfile
      mv tmpfile $file
      sed -i 's/from google.protobuf import/from rex.google.protobuf import/' $file
    fi
done
