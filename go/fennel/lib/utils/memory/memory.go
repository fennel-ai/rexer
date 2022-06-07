package memory

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/Unleash/unleash-client-go/v3"
	"github.com/raulk/go-watchdog"
)

func init() {
	// Set 90% memory utilization as the threshold for capturing heap profiles.
	watchdog.HeapProfileThreshold = 0.90
}

type watchdogConfig struct {
	Limit  uint64  `json:"limit"`
	Factor float64 `json:"factor"`
}

func (config watchdogConfig) Validate() error {
	if config.Factor <= 0 || config.Factor >= 1.0 {
		return fmt.Errorf("'factor' should be in (0.0, 1.0)")
	} else if config.Limit == 0 {
		return fmt.Errorf("'limit' should be > 0")
	}
	return nil
}

func RunMemoryWatchdog(freq time.Duration) {
	go func() {
		ticker := time.NewTicker(freq)
		var currFactor float64 = 0
		var stopFn func()
		for ; true; <-ticker.C {
			variant := unleash.GetVariant("memory_watchdog")
			if !variant.Enabled {
				if stopFn != nil {
					log.Printf("Stopping memory watchdog")
					stopFn()
					stopFn = nil
				}
				continue
			}
			var config watchdogConfig
			err := json.Unmarshal([]byte(variant.Payload.Value), &config)
			if err != nil {
				log.Printf("Error parsing watchdog config: %v", err)
				continue
			}
			if err := config.Validate(); err != nil {
				log.Printf("Invalid watchdog config [%v]: %v", config, err)
				continue
			} else if config.Factor != currFactor {
				log.Printf("Got new memory watchdog config: %v", config)
				// Stop the current watchdog if previously enabled and start a new watchdog.
				if stopFn != nil {
					stopFn()
				}
				err, stopFn = watchdog.SystemDriven(config.Limit, freq, watchdog.NewAdaptivePolicy(config.Factor))
				if err != nil {
					log.Printf("Failed to start memory watchdog: %v", err)
					continue
				}
				currFactor = config.Factor
			}
		}
	}()
}
