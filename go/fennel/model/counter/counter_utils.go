package counter

import (
	"fennel/lib/value"
	"fmt"
)

func getDouble(v value.Value) (float64, error) {
	if d, ok := v.(value.Double); ok {
		return float64(d), nil
	}

	if i, ok := v.(value.Int); ok {
		return float64(i), nil
	}
	return 0, fmt.Errorf("value [%s] is not a number", v.String())
}
