package kvstore

import "fmt"

type TabletType uint8

const (
	Profile   TabletType = 1
	Aggregate TabletType = 2
	Offset    TabletType = 3
)

func (t TabletType) String() string {
	switch t {
	case Profile:
		return "Profile"
	case Aggregate:
		return "Aggregate"
	case Offset:
		return "Offset"
	default:
		return "Unknown"
	}
}

// Write adds the table type to the byte slice and returns the number of bytes written
// If the byte slice is too small, error is thrown.
// for now, TabletType is only uint8 so we can just append the byte but in future, if
// we have to support large number of tablet types, we might need to use varint encoding
func (t TabletType) Write(buf []byte) (int, error) {
	if len(buf) < 1 {
		return 0, fmt.Errorf("can not write tablet: buffer too small")
	}
	buf[0] = byte(t)
	return 1, nil
}

// ReadTablet reads the table type from the byte slice and returns the number of bytes read
// throws an error if the byte slice doesn't have valid tablet
func ReadTablet(buf []byte) (TabletType, int, error) {
	if len(buf) < 1 {
		return 0, 0, fmt.Errorf("can not read tablet: buffer too small")
	}
	return TabletType(buf[0]), 1, nil
}
