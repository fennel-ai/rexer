package arena

import (
	"fennel/lib/counter"
	"fennel/lib/value"
)

// Arenas of few commonly used types are defined below which are to be shared
// across the binary. The combined memory footprint of these is upto ~2.5GB
// NOTE: if you add more arena, please update the total size above in docstrings
var (
	Bytes      = New[byte](1<<12, 100<<20)      // memory footprint <= 100MB
	Values     = New[value.Value](1<<15, 1<<24) // memory footprint <= 256MB
	DictValues = New[value.Dict](1<<15, 1<<23)  // memory footprint <= 128MB
	Strings    = New[string](1<<12, 1<<22)      // memory footprint <= 96MB
	Ints       = New[int](1<<12, 1<<22)         // memory footprint <= 16MB
	Longs      = New[int64](1<<12, 1<<22)       // memory footprint <= 32MB
	Bools      = New[bool](1<<12, 1<<22)        // memory footprint <= 4MB
	Bytes2D    = New[[]byte](1<<12, 1<<22)      // memory footprint <= 96MB
	// Bucket struct uses up 28 bytes (16 for string header, 4 each for width, index and window)
	Buckets = New[counter.Bucket](1<<15, 1<<26) // memory footprint <= 2GB
)
