// Code generated by protoc-gen-go-grpc. DO NOT EDIT.

package proto

import (
	context "context"
	proto2 "fennel/lib/featurestore/aggregate/proto"
	proto3 "fennel/lib/featurestore/feature/proto"
	proto1 "fennel/lib/featurestore/status/proto"
	proto "fennel/lib/featurestore/stream/proto"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// FennelFeatureStoreClient is the client API for FennelFeatureStore service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type FennelFeatureStoreClient interface {
	RegisterStream(ctx context.Context, in *proto.CreateStreamRequest, opts ...grpc.CallOption) (*proto1.Status, error)
	RegisterAggregate(ctx context.Context, in *proto2.CreateAggregateRequest, opts ...grpc.CallOption) (*proto1.Status, error)
	RegisterFeature(ctx context.Context, in *proto3.CreateFeatureRequest, opts ...grpc.CallOption) (*proto1.Status, error)
	ExtractFeatures(ctx context.Context, in *proto3.ExtractFeaturesRequest, opts ...grpc.CallOption) (*proto3.ExtractFeaturesResponse, error)
}

type fennelFeatureStoreClient struct {
	cc grpc.ClientConnInterface
}

func NewFennelFeatureStoreClient(cc grpc.ClientConnInterface) FennelFeatureStoreClient {
	return &fennelFeatureStoreClient{cc}
}

func (c *fennelFeatureStoreClient) RegisterStream(ctx context.Context, in *proto.CreateStreamRequest, opts ...grpc.CallOption) (*proto1.Status, error) {
	out := new(proto1.Status)
	err := c.cc.Invoke(ctx, "/fennel.proto.FennelFeatureStore/RegisterStream", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *fennelFeatureStoreClient) RegisterAggregate(ctx context.Context, in *proto2.CreateAggregateRequest, opts ...grpc.CallOption) (*proto1.Status, error) {
	out := new(proto1.Status)
	err := c.cc.Invoke(ctx, "/fennel.proto.FennelFeatureStore/RegisterAggregate", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *fennelFeatureStoreClient) RegisterFeature(ctx context.Context, in *proto3.CreateFeatureRequest, opts ...grpc.CallOption) (*proto1.Status, error) {
	out := new(proto1.Status)
	err := c.cc.Invoke(ctx, "/fennel.proto.FennelFeatureStore/RegisterFeature", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *fennelFeatureStoreClient) ExtractFeatures(ctx context.Context, in *proto3.ExtractFeaturesRequest, opts ...grpc.CallOption) (*proto3.ExtractFeaturesResponse, error) {
	out := new(proto3.ExtractFeaturesResponse)
	err := c.cc.Invoke(ctx, "/fennel.proto.FennelFeatureStore/ExtractFeatures", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// FennelFeatureStoreServer is the server API for FennelFeatureStore service.
// All implementations must embed UnimplementedFennelFeatureStoreServer
// for forward compatibility
type FennelFeatureStoreServer interface {
	RegisterStream(context.Context, *proto.CreateStreamRequest) (*proto1.Status, error)
	RegisterAggregate(context.Context, *proto2.CreateAggregateRequest) (*proto1.Status, error)
	RegisterFeature(context.Context, *proto3.CreateFeatureRequest) (*proto1.Status, error)
	ExtractFeatures(context.Context, *proto3.ExtractFeaturesRequest) (*proto3.ExtractFeaturesResponse, error)
	mustEmbedUnimplementedFennelFeatureStoreServer()
}

// UnimplementedFennelFeatureStoreServer must be embedded to have forward compatible implementations.
type UnimplementedFennelFeatureStoreServer struct {
}

func (UnimplementedFennelFeatureStoreServer) RegisterStream(context.Context, *proto.CreateStreamRequest) (*proto1.Status, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RegisterStream not implemented")
}
func (UnimplementedFennelFeatureStoreServer) RegisterAggregate(context.Context, *proto2.CreateAggregateRequest) (*proto1.Status, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RegisterAggregate not implemented")
}
func (UnimplementedFennelFeatureStoreServer) RegisterFeature(context.Context, *proto3.CreateFeatureRequest) (*proto1.Status, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RegisterFeature not implemented")
}
func (UnimplementedFennelFeatureStoreServer) ExtractFeatures(context.Context, *proto3.ExtractFeaturesRequest) (*proto3.ExtractFeaturesResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ExtractFeatures not implemented")
}
func (UnimplementedFennelFeatureStoreServer) mustEmbedUnimplementedFennelFeatureStoreServer() {}

// UnsafeFennelFeatureStoreServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to FennelFeatureStoreServer will
// result in compilation errors.
type UnsafeFennelFeatureStoreServer interface {
	mustEmbedUnimplementedFennelFeatureStoreServer()
}

func RegisterFennelFeatureStoreServer(s grpc.ServiceRegistrar, srv FennelFeatureStoreServer) {
	s.RegisterService(&FennelFeatureStore_ServiceDesc, srv)
}

func _FennelFeatureStore_RegisterStream_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(proto.CreateStreamRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(FennelFeatureStoreServer).RegisterStream(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/fennel.proto.FennelFeatureStore/RegisterStream",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(FennelFeatureStoreServer).RegisterStream(ctx, req.(*proto.CreateStreamRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _FennelFeatureStore_RegisterAggregate_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(proto2.CreateAggregateRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(FennelFeatureStoreServer).RegisterAggregate(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/fennel.proto.FennelFeatureStore/RegisterAggregate",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(FennelFeatureStoreServer).RegisterAggregate(ctx, req.(*proto2.CreateAggregateRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _FennelFeatureStore_RegisterFeature_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(proto3.CreateFeatureRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(FennelFeatureStoreServer).RegisterFeature(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/fennel.proto.FennelFeatureStore/RegisterFeature",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(FennelFeatureStoreServer).RegisterFeature(ctx, req.(*proto3.CreateFeatureRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _FennelFeatureStore_ExtractFeatures_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(proto3.ExtractFeaturesRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(FennelFeatureStoreServer).ExtractFeatures(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/fennel.proto.FennelFeatureStore/ExtractFeatures",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(FennelFeatureStoreServer).ExtractFeatures(ctx, req.(*proto3.ExtractFeaturesRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// FennelFeatureStore_ServiceDesc is the grpc.ServiceDesc for FennelFeatureStore service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var FennelFeatureStore_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "fennel.proto.FennelFeatureStore",
	HandlerType: (*FennelFeatureStoreServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "RegisterStream",
			Handler:    _FennelFeatureStore_RegisterStream_Handler,
		},
		{
			MethodName: "RegisterAggregate",
			Handler:    _FennelFeatureStore_RegisterAggregate_Handler,
		},
		{
			MethodName: "RegisterFeature",
			Handler:    _FennelFeatureStore_RegisterFeature_Handler,
		},
		{
			MethodName: "ExtractFeatures",
			Handler:    _FennelFeatureStore_ExtractFeatures_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "services.proto",
}
