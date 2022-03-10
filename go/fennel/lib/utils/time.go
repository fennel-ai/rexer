package utils

import (
	"fmt"

	"fennel/lib/ftypes"
)

func Duration(w ftypes.Window) (uint64, error) {
	switch w {
	case ftypes.Window_MINUTE:
		return 60, nil
	case ftypes.Window_HOUR:
		return 3600, nil
	case ftypes.Window_DAY:
		return 24 * 3600, nil
	default:
		return 0, fmt.Errorf("invalid type")
	}
}
