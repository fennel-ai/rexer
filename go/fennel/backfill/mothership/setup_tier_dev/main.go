package main

import (
	"fennel/mothership"
	"fennel/mothership/lib/customer"
	"fennel/mothership/lib/dataplane"
	tierL "fennel/mothership/lib/tier"
	"log"
	"os"

	"github.com/alexflint/go-arg"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type Args struct {
	mothership.MothershipArgs
	BridgeENV string `arg:"required,--bridge_env,env:BRIDGE_ENV"` // dev, prod
}

const LOCALHOST = "http://localhost:2425"

func run(args Args) error {
	m, err := mothership.CreateFromArgs(&args.MothershipArgs)
	if err != nil {
		return err
	}
	db, err := gorm.Open(mysql.New(mysql.Config{
		Conn: m.DB,
	}), &gorm.Config{})
	if err != nil {
		return err
	}

	var customer customer.Customer
	if err = db.Take(&customer, "domain = ?", "fennel.ai").Error; err != nil {
		return err
	}
	var tier tierL.Tier
	if db.Take(&tier, "customer_id = ? AND api_url = ?", customer.ID, LOCALHOST).RowsAffected > 0 {
		return nil
	}

	var dp dataplane.DataPlane
	if db.Take(&dp).RowsAffected == 0 {
		dp = dataplane.DataPlane{
			AwsRole:     "role",
			Region:      "US Midwest",
			PulumiStack: "pulumi",
			VpcID:       "vpc",
		}
		if err = db.Create(&dp).Error; err != nil {
			return err
		}
	}
	tier = tierL.Tier{
		DataPlaneID:   dp.ID,
		CustomerID:    customer.ID,
		PulumiStack:   "pulumi",
		ApiUrl:        LOCALHOST,
		K8sNamespace:  "namespace",
		RequestsLimit: 1000,
	}
	err = db.Create(&tier).Error
	return err
}

func main() {
	args := Args{}
	err := arg.Parse(&args)
	if err != nil {
		log.Fatalf("error: %s\n", err)
		os.Exit(-1)
	}
	if args.BridgeENV != "dev" {
		log.Fatalln("not dev env")
		os.Exit(-1)
	}
	if err = run(args); err != nil {
		log.Fatalf("error: %s\n", err)
		os.Exit(-1)
	}
	log.Println("Backfill ran successfully.")
}
