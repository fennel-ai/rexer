package main

import (
	"fennel/mothership"
	"fennel/mothership/lib/dataplane"
	tierL "fennel/mothership/lib/tier"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/alexflint/go-arg"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type Args struct {
	mothership.MothershipArgs
	BridgeENV string `arg:"required,--bridge_env,env:BRIDGE_ENV"` // dev, prod
	APIUrl    string `arg:"required,--api_url,env:API_URL"`       // could be localhost:2425, or prod URL for existing customers
}

const LOKAL_METRICS_SERVER_ADDRESS = "http://a535b3af4b7e7400bab17167a1f5f7a4-766178462.ap-south-1.elb.amazonaws.com/"

func generateName(prefix string) string {
	return fmt.Sprintf("%s:%v", prefix, time.Now().UnixMilli())
}

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

	var tier tierL.Tier
	if db.Take(&tier, "api_url = ?", args.APIUrl).RowsAffected > 0 {
		return nil
	}

	var dp dataplane.DataPlane
	if db.Take(&dp).RowsAffected == 0 {
		dp = dataplane.DataPlane{
			AwsRole:              "role",
			Region:               "US Midwest",
			PulumiStack:          "pulumi",
			VpcID:                "vpc",
			MetricsServerAddress: LOKAL_METRICS_SERVER_ADDRESS,
		}
		if err = db.Create(&dp).Error; err != nil {
			return err
		}
	}
	tier = tierL.Tier{
		DataPlaneID:   dp.ID,
		PulumiStack:   generateName("pulumi"),
		ApiUrl:        args.APIUrl,
		K8sNamespace:  generateName("namespace"),
		Plan:          tierL.TierPlanPersonal,
		RequestsLimit: 0,
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
