package main

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"time"

	"fennel/client"
	libaction "fennel/lib/action"
	"fennel/lib/ftypes"
	libprofile "fennel/lib/profile"
	"fennel/lib/utils"
	"fennel/lib/value"

	"github.com/alexflint/go-arg"
)

type LoadTestArg struct {
	Url         string `arg:"--url" default:"http://localhost:2425"`
	NumUids     int    `arg:"--num_uids" default:"1000"`
	NumVideos   int    `arg:"--num_videos" default:"1000"`
	NumCreators int    `arg:"--num_creators" default:"100"`
	Qps         int    `arg:"--qps" default:"10000"`
	Dryrun      bool   `arg:"--dry_run" default:"false"`
	Uid         string `arg:"--uid" default:""` // If NumUids == 1 and Uid is set, use it
	NumProcs    int    `arg:"--num_procs" default:"400"`
}

const (
	ACTOR_TYPE  = "user"
	TARGET_TYPE = "video"
	// To avoid any external traffic intervening with load/e2e tests.
	ACTION_TYPE    = "e2etest_view"
	METADATA_FIELD = "watch_time"
)

func logActions(c *client.Client, numproc, total, qps int, uids, video_ids []string, dryrun bool) error {
	errs := make(chan error, numproc)
	per_proc := total / numproc
	qps_per_proc := qps / numproc
	for i := 0; i < numproc; i++ {
		go func(procid int, uids, video_ids []string, num, qps int) {
			for num > 0 {
				start := time.Now().UnixMilli()
				for i := 0; i < qps; i++ {
					a := libaction.Action{
						ActorID:    ftypes.OidType(uids[rand.Intn(len(uids))]),
						ActorType:  ACTOR_TYPE,
						TargetID:   ftypes.OidType(video_ids[rand.Intn(len(video_ids))]),
						TargetType: TARGET_TYPE,
						ActionType: ACTION_TYPE,
						Timestamp:  ftypes.Timestamp(time.Now().Unix()),
						RequestID:  "1",
						Metadata:   value.NewDict(map[string]value.Value{METADATA_FIELD: value.Int(rand.Intn(60))}),
					}
					if dryrun {
						fmt.Printf("[%d] going to log action: %v\n", procid, a)
					} else {
						if err := c.LogAction(a, ""); err != nil {
							log.Printf("loadtest error actionlog: %v", err)
							continue
							//errs <- err
							//return
						}
					}
					num -= 1
				}
				taken := time.Now().UnixMilli() - start
				if taken < 1000 {
					time.Sleep(time.Millisecond * time.Duration(1000-taken))
				}
			}
			errs <- nil
		}(i, uids, video_ids, per_proc, qps_per_proc)
	}
	for i := 0; i < numproc; i++ {
		if err := <-errs; err != nil {
			return err
		}
	}
	return nil
}

func setProfileInner(numprocs, procid int, c *client.Client, otype ftypes.OType, oids []string, fields map[string][]value.Value, dryrun bool, errs chan error) {
	for i, oid := range oids {
		if i%numprocs != procid {
			continue
		}
		for k, values := range fields {
			v := values[rand.Intn(len(values))]
			pi := libprofile.ProfileItem{
				OType: otype,
				Oid:   oid,
				Key:   k,
				Value: v,
			}
			if dryrun {
				fmt.Printf("Set profile: (%s, %s, %s) -> %v\n", otype, oid, k, v)
			} else {
				if err := c.SetProfile(&pi); err != nil {
					log.Printf("loadtest error profile: %v", err)
					continue
					//errs <- err
					//return
				}
			}
		}
	}
	errs <- nil
}

func setProfile(c *client.Client, otype ftypes.OType, oids []string, fields map[string][]value.Value, dryrun bool) error {
	numprocs := 200
	errs := make(chan error, numprocs)
	for i := 0; i < numprocs; i++ {
		go setProfileInner(numprocs, i, c, otype, oids, fields, dryrun, errs)
	}
	for i := 0; i < numprocs; i++ {
		if err := <-errs; err != nil {
			return err
		}
	}
	return nil
}

func main() {
	log.Printf("entering load test...\n")
	var flags LoadTestArg
	arg.MustParse(&flags)
	log.Printf("flags passed: %+v\n", flags)
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	rand.Seed(time.Now().Unix()) // initialize global pseudo random generator
	total := 1 * 60 * flags.Qps
	uids := make([]string, 0, flags.NumUids)

	// if the NumUids == 1, use the Uid passed by the caller.
	if flags.NumUids == 1 && flags.Uid != "" {
		uids = append(uids, flags.Uid)
	} else {
		for i := 0; i < flags.NumUids; i++ {
			uids = append(uids, utils.RandString(64))
		}
	}
	userFields := map[string][]value.Value{
		"city":         {value.String("DEL"), value.String("HYD"), value.String("SFO"), value.String("MUM"), value.String("LAX")},
		"gender":       {value.Int(1), value.Int(2), value.Int(3)},
		"age_group":    {value.Int(1), value.Int(2), value.Int(3), value.Int(4), value.Int(5)},
		"country":      {value.String("IN"), value.String("US")},
		"os":           {value.String("android"), value.String("ios")},
		"mobile_brand": {value.String("xiaomi"), value.String("apple"), value.String("samsung")},
	}
	creatorIDs := make([]value.Value, 0, flags.NumCreators)
	for i := 0; i < flags.NumCreators; i++ {
		creatorIDs = append(creatorIDs, value.Int(rand.Uint32()))
	}
	videoIds := make([]string, 0, flags.NumVideos)
	for i := 0; i < flags.NumVideos; i++ {
		videoIds = append(videoIds, utils.RandString(64))
	}
	videoFields := map[string][]value.Value{
		"creator_id": creatorIDs,
	}
	c, err := client.NewClient(flags.Url, &http.Client{})
	if err != nil {
		panic(err)
	}
	start := time.Now()
	log.Printf("starting user profiles...\n")
	if err = setProfile(c, ACTOR_TYPE, uids, userFields, flags.Dryrun); err != nil {
		panic(err)
	}
	log.Printf("=======DONE========\n")
	log.Printf("%d user profiles took %dms\n", flags.NumUids*6, time.Since(start).Milliseconds())

	start = time.Now()
	log.Printf("starting video profiles...\n")
	if err = setProfile(c, TARGET_TYPE, videoIds, videoFields, flags.Dryrun); err != nil {
		panic(err)
	}
	log.Printf("=======DONE========\n")
	log.Printf("%d video profiles took %dms\n", flags.NumVideos, time.Since(start).Milliseconds())

	start = time.Now()
	log.Printf("starting actions...\n")
	if err = logActions(c, flags.NumProcs, total, flags.Qps, uids, videoIds, flags.Dryrun); err != nil {
		panic(err)
	}
	log.Printf("=======DONE========\n")
	log.Printf("%d actions took %dms\n", total, time.Since(start).Milliseconds())
	log.Printf("done...\n")
}
