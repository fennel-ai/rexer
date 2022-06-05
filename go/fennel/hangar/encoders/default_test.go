package encoders

import "testing"

func TestDefaultEncoder(t *testing.T) {
	t.Parallel()
	encoder := Default()
	testEncodeKey(t, encoder)
	testEncodeVal(t, encoder)
}

func BenchmarkDefaultEncoder_EncodeKey(b *testing.B) {
	encoder := Default()
	b.Run("encode_keys", func(b *testing.B) {
		benchmarkEncodeKey(b, encoder)
	})
	b.Run("encode_vals_10K_100_20", func(b *testing.B) {
		benchmarkEncodeVals(b, encoder, 10_000, 100, 20, 20)
	})
	b.Run("encode_vals_10K_10_200", func(b *testing.B) {
		benchmarkEncodeVals(b, encoder, 10_000, 10, 200, 200)
	})
}
