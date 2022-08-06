package rpc

import (
	context "context"

	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NewRateLimiter returns a new unary server interceptors that performs request rate limiting.
func NewRateLimiter(maxConcurrent int) grpc.UnaryServerInterceptor {
	bucket := make(chan struct{}, maxConcurrent)
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		select {
		case bucket <- struct{}{}:
			resp, err := handler(ctx, req)
			<-bucket
			return resp, err
		default:
			// Return "Unavailable" error as a 429 response as per:
			// https://github.com/grpc/grpc/blob/master/doc/http-grpc-status-mapping.md.
			return nil, status.Error(codes.Unavailable, "Rate limit exceeded")
		}
	}
}
