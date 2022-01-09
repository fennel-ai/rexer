package lib

import (
	"fmt"
	"math"
)

type OidType uint64
type Timestamp uint64
type RequestID uint64

const (
	PORT = 2425
)

type OType uint32

const (
	User  OType = 1
	Video       = 2
)

type ActionType uint32

const (
	Like  ActionType = 1
	Share            = 2
	View             = 3
)

type CounterType uint32

type Key []OidType
type Window uint8

const (
	HOUR    Window = 1
	DAY            = 2
	WEEK           = 3
	MONTH          = 4
	QUARTER        = 5
	YEAR           = 6
	FOREVER        = 7
)

func Windows() []Window {
	return []Window{HOUR, DAY, WEEK, MONTH, QUARTER, YEAR, FOREVER}
}

const Z_95 = 1.96

func Wilson(num uint64, den uint64, lower bool) float64 {
	// (p + Z_95²/2n ± Z_95√p(1 – p)/n + Z_95²/4n²) / (1 + Z_95²/n)
	if den == 0 {
		return 0
	}

	p := float64(num) / float64(den)
	n := float64(den)
	base := p + (Z_95*Z_95)/(2*n)
	plusminus := Z_95 * math.Sqrt(p*(1-p)/n+(Z_95*Z_95)/(4*n*n))
	normalize := 1 + (Z_95*Z_95)/n
	if lower {
		return (base - plusminus) / normalize
	} else {
		return (base + plusminus) / normalize
	}
}

type GetCountRequest struct {
	CounterType CounterType
	Window      Window
	Key         Key
	Timestamp   Timestamp
}

func (r GetCountRequest) Validate() error {
	if r.CounterType == 0 {
		return fmt.Errorf("counter type can not be zero")
	}
	if r.Window == 0 {
		return fmt.Errorf("counter window can not be zero")
	}
	if len(r.Key) == 0 {
		return fmt.Errorf("counter key can not be empty")
	}
	return nil
}

const (
	USER_LIKE                CounterType = 1
	USER_SHARE                           = 2
	VIDEO_LIKE                           = 3
	VIDEO_SHARE                          = 4
	USER_ACCOUNT_LIKE                    = 5
	USER_TOPIC_LIKE                      = 6
	AGE_VIDEO_LIKE                       = 7
	GENDER_AGE_VIDEO_LIKE                = 8
	ZIP_ACCOUNT_LIKE                     = 9
	AGE_ZIP_U2VCLUSTER_LIKE              = 10
	PAGE_FOLLOWER_VIDEO_LIKE             = 11
	USER_VIDEO_30SWATCH                  = 12
	USER_VIDEO_LIKE                      = 13
)
