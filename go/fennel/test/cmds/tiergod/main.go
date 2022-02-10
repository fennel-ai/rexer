//go:build integration

package main

import (
	"fennel/test"
	"fennel/tier"
	"flag"
	"github.com/alexflint/go-arg"
)

func main() {
	var args tier.TierArgs
	arg.Parse(&args)
	if err := args.Valid(); err != nil {
		panic(err)
	}

	mode := flag.String("mode", "create", "'create' for creating a tier, 'destroy' for destroying")
	flag.Parse()

	switch *mode {
	case "create":
		if err := test.Setup(args); err != nil {
			panic(err)
		}
	case "destroy":
		tier, err := tier.CreateFromArgs(&args)
		if err != nil {
			panic(err)
		}
		if err = test.Teardown(tier); err != nil {
			panic(err)
		}
	default:
		panic("invalid mode: valid modes are 'create' or 'destroy'")
	}
}
