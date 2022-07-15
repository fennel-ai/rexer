package counter

import (
	"fmt"

	"fennel/lib/ftypes"
	"fennel/lib/value"
)

func getDouble(v value.Value) (float64, error) {
	if d, ok := v.(value.Double); ok {
		return float64(d), nil
	}

	if i, ok := v.(value.Int); ok {
		return float64(i), nil
	}
	return 0, fmt.Errorf("value [%s] is not a $$ number", v.String())
}

func start(end ftypes.Timestamp, duration uint32) ftypes.Timestamp {
	d := ftypes.Timestamp(duration)
	if end > d {
		return end - d
	}
	return 0
}
