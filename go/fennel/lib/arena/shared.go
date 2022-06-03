package arena

import "fennel/lib/value"

// Arenas of few commonly used types are defined below which are to be shared
// across the binary. The combined memory footprint of these is upto ~300MB

var (
	Bytes  = New[byte](1<<12, 1<<22)        // memory footprint <= 4MB
	Values = New[value.Value](1<<12, 1<<22) // memory footprint <= 64MB
	// BigValues is an arena of larger value slices
	// We have a separate pool for large values so that they don't compete
	// with smaller slices for space in arena
	BigValues = New[value.Value](1<<15, 1<<22) // memory footprint <= 128MB
	Strings   = New[string](1<<12, 1<<22)      // memory footprint <= 96MB
	Ints      = New[int](1<<12, 1<<22)         // memory footprint <= 16MB
	Longs     = New[int64](1<<12, 1<<22)       // memory footprint <= 32MB
	Bools     = New[bool](1<<12, 1<<22)        // memory footprint <= 4MB
	Bytes2D   = New[[]byte](1<<12, 1<<22)      // memory footprint <= 96MB
	// NOTE: if you add more arena, please update the total size above in docstrings
)
