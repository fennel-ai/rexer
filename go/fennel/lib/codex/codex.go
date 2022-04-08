package codex

import (
	"fmt"
)

type Codex uint8

// Writes the codex to the given buffer and returns the number of bytes written
// If the buffer is too small, error is returned
func (c Codex) Write(buf []byte) (int, error) {
	if len(buf) < 1 {
		return 0, fmt.Errorf("can not write codex: buffer too small")
	}
	buf[0] = byte(c)
	return 1, nil
}

// Read the next codex from the given buffer and returns the number of bytes read
// If the buffer doesn't contain a valid codex, an error is returned
func Read(buf []byte) (Codex, int, error) {
	if len(buf) < 1 {
		return 0, 0, fmt.Errorf("codex.read: too small buffer")
	}
	return Codex(buf[0]), 1, nil
}
