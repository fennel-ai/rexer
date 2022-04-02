package counter

import (
	"testing"

	"fennel/lib/ftypes"
	"fennel/lib/value"
	"github.com/stretchr/testify/assert"
)

func TestStart(t *testing.T) {
	durations := []uint64{100, 200}
	badDurations := []uint64{0, 1, 50, 150, 250}
	testHistogramStart(t, average{Durations: durations}, durations, badDurations)
	testHistogramStart(t, list{Durations: durations}, durations, badDurations)
	testHistogramStart(t, rollingMax{Durations: durations}, durations, badDurations)
	testHistogramStart(t, rollingMin{Durations: durations}, durations, badDurations)
	testHistogramStart(t, rollingRate{Durations: durations}, durations, badDurations)
	testHistogramStart(t, rollingStdDev{Durations: durations}, durations, badDurations)
	testHistogramStart(t, rollingSum{Durations: durations}, durations, badDurations)
	testHistogramStart(t, topK{Durations: durations}, durations, badDurations)
}

func testHistogramStart(t *testing.T, h Histogram, durations []uint64, badDurations []uint64) {
	for _, d := range durations {
		kwargs := value.NewDict(map[string]value.Value{"duration": value.Int(d)})
		s, err := h.Start(ftypes.Timestamp(d+10), kwargs)
		assert.NoError(t, err)
		assert.Equal(t, s, ftypes.Timestamp(10))
		// Duration > end
		s, err = h.Start(ftypes.Timestamp(d-10), kwargs)
		assert.NoError(t, err)
		assert.Equal(t, s, ftypes.Timestamp(0))
	}
	for _, bd := range badDurations {
		kwargs := value.NewDict(map[string]value.Value{"duration": value.Int(bd)})
		_, err := h.Start(200, kwargs)
		assert.Error(t, err)
	}
}
