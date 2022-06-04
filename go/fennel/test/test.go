package test

import "flag"

var testmode = flag.Bool("test", false, "true if executing in test mode, false otherwise")

func IsInTest() bool {
	return *testmode
}
