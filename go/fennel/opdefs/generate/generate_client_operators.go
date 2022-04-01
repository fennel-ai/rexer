package main

import (
	"encoding/json"
	"fennel/engine/operators"
	_ "fennel/opdefs"

	"log"
	"os"
)

/*
This script generates the operators.txt file in the rexer-pyclient-alpha/rexerclient/gen directory.
To run this script, run the following command:
$ cd go/fennel
$ go run --tags dynamic opdefs/generate/generate_client_operators.go

If the you DO NOT run this script after updating the operators, the tests will fail.
Also when checking in the updated client, do increment the client version number in  pyproject.toml file.
*/

func main() {
	file, err := os.Create("../../../rexer-pyclient-alpha/rexerclient/gen/operators.json")
	if err != nil {
		log.Fatalf("failed creating file: %s", err)
	}
	defer file.Close()
	ops := operators.GetOperators()
	if err != nil {
		log.Fatalf("Failed to fetch the operators: %s", err)
	}
	opsIndented, err := json.MarshalIndent(ops, "", " \t")
	if err != nil {
		log.Fatalf("failed marshalling to json: %s", err)
	}
	_, err = file.Write(opsIndented)
	if err != nil {
		log.Fatalf("failed writing to file: %s", err)
	}
}
