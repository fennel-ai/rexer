// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v3.17.3
// source: nitrous.proto

package v2

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// NitrousClient is the client API for Nitrous service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type NitrousClient interface {
	// APIs to read data.
	GetProfiles(ctx context.Context, in *ProfilesRequest, opts ...grpc.CallOption) (*ProfilesResponse, error)
	GetAggregateValues(ctx context.Context, in *AggregateValuesRequest, opts ...grpc.CallOption) (*AggregateValuesResponse, error)
	// API to get processing lag. This is especially useful in tests.
	GetProfileLag(ctx context.Context, in *ProfileLagRequest, opts ...grpc.CallOption) (*LagResponse, error)
	GetAggregateLag(ctx context.Context, in *AggregateLagRequest, opts ...grpc.CallOption) (*LagResponse, error)
}

type nitrousClient struct {
	cc grpc.ClientConnInterface
}

func NewNitrousClient(cc grpc.ClientConnInterface) NitrousClient {
	return &nitrousClient{cc}
}

func (c *nitrousClient) GetProfiles(ctx context.Context, in *ProfilesRequest, opts ...grpc.CallOption) (*ProfilesResponse, error) {
	out := new(ProfilesResponse)
	err := c.cc.Invoke(ctx, "/nitrous.Nitrous/GetProfiles", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *nitrousClient) GetAggregateValues(ctx context.Context, in *AggregateValuesRequest, opts ...grpc.CallOption) (*AggregateValuesResponse, error) {
	out := new(AggregateValuesResponse)
	err := c.cc.Invoke(ctx, "/nitrous.Nitrous/GetAggregateValues", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *nitrousClient) GetProfileLag(ctx context.Context, in *ProfileLagRequest, opts ...grpc.CallOption) (*LagResponse, error) {
	out := new(LagResponse)
	err := c.cc.Invoke(ctx, "/nitrous.Nitrous/GetProfileLag", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *nitrousClient) GetAggregateLag(ctx context.Context, in *AggregateLagRequest, opts ...grpc.CallOption) (*LagResponse, error) {
	out := new(LagResponse)
	err := c.cc.Invoke(ctx, "/nitrous.Nitrous/GetAggregateLag", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// NitrousServer is the server API for Nitrous service.
// All implementations must embed UnimplementedNitrousServer
// for forward compatibility
type NitrousServer interface {
	// APIs to read data.
	GetProfiles(context.Context, *ProfilesRequest) (*ProfilesResponse, error)
	GetAggregateValues(context.Context, *AggregateValuesRequest) (*AggregateValuesResponse, error)
	// API to get processing lag. This is especially useful in tests.
	GetProfileLag(context.Context, *ProfileLagRequest) (*LagResponse, error)
	GetAggregateLag(context.Context, *AggregateLagRequest) (*LagResponse, error)
	mustEmbedUnimplementedNitrousServer()
}

// UnimplementedNitrousServer must be embedded to have forward compatible implementations.
type UnimplementedNitrousServer struct {
}

func (UnimplementedNitrousServer) GetProfiles(context.Context, *ProfilesRequest) (*ProfilesResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetProfiles not implemented")
}
func (UnimplementedNitrousServer) GetAggregateValues(context.Context, *AggregateValuesRequest) (*AggregateValuesResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetAggregateValues not implemented")
}
func (UnimplementedNitrousServer) GetProfileLag(context.Context, *ProfileLagRequest) (*LagResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetProfileLag not implemented")
}
func (UnimplementedNitrousServer) GetAggregateLag(context.Context, *AggregateLagRequest) (*LagResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetAggregateLag not implemented")
}
func (UnimplementedNitrousServer) mustEmbedUnimplementedNitrousServer() {}

// UnsafeNitrousServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to NitrousServer will
// result in compilation errors.
type UnsafeNitrousServer interface {
	mustEmbedUnimplementedNitrousServer()
}

func RegisterNitrousServer(s grpc.ServiceRegistrar, srv NitrousServer) {
	s.RegisterService(&Nitrous_ServiceDesc, srv)
}

func _Nitrous_GetProfiles_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ProfilesRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(NitrousServer).GetProfiles(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/nitrous.Nitrous/GetProfiles",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(NitrousServer).GetProfiles(ctx, req.(*ProfilesRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Nitrous_GetAggregateValues_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AggregateValuesRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(NitrousServer).GetAggregateValues(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/nitrous.Nitrous/GetAggregateValues",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(NitrousServer).GetAggregateValues(ctx, req.(*AggregateValuesRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Nitrous_GetProfileLag_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ProfileLagRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(NitrousServer).GetProfileLag(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/nitrous.Nitrous/GetProfileLag",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(NitrousServer).GetProfileLag(ctx, req.(*ProfileLagRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Nitrous_GetAggregateLag_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AggregateLagRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(NitrousServer).GetAggregateLag(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/nitrous.Nitrous/GetAggregateLag",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(NitrousServer).GetAggregateLag(ctx, req.(*AggregateLagRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// Nitrous_ServiceDesc is the grpc.ServiceDesc for Nitrous service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var Nitrous_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "nitrous.Nitrous",
	HandlerType: (*NitrousServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "GetProfiles",
			Handler:    _Nitrous_GetProfiles_Handler,
		},
		{
			MethodName: "GetAggregateValues",
			Handler:    _Nitrous_GetAggregateValues_Handler,
		},
		{
			MethodName: "GetProfileLag",
			Handler:    _Nitrous_GetProfileLag_Handler,
		},
		{
			MethodName: "GetAggregateLag",
			Handler:    _Nitrous_GetAggregateLag_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "nitrous.proto",
}
