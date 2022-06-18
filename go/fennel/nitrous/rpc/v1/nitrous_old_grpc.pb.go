// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v3.21.1
// source: nitrous_old.proto

package v1

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
	GetMany(ctx context.Context, in *GetManyRequest, opts ...grpc.CallOption) (*GetManyResponse, error)
	Init(ctx context.Context, in *InitReq, opts ...grpc.CallOption) (*InitResp, error)
	Lag(ctx context.Context, in *LagReq, opts ...grpc.CallOption) (*LagResp, error)
}

type nitrousClient struct {
	cc grpc.ClientConnInterface
}

func NewNitrousClient(cc grpc.ClientConnInterface) NitrousClient {
	return &nitrousClient{cc}
}

func (c *nitrousClient) GetMany(ctx context.Context, in *GetManyRequest, opts ...grpc.CallOption) (*GetManyResponse, error) {
	out := new(GetManyResponse)
	err := c.cc.Invoke(ctx, "/nitrous_old.Nitrous/GetMany", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *nitrousClient) Init(ctx context.Context, in *InitReq, opts ...grpc.CallOption) (*InitResp, error) {
	out := new(InitResp)
	err := c.cc.Invoke(ctx, "/nitrous_old.Nitrous/Init", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *nitrousClient) Lag(ctx context.Context, in *LagReq, opts ...grpc.CallOption) (*LagResp, error) {
	out := new(LagResp)
	err := c.cc.Invoke(ctx, "/nitrous_old.Nitrous/Lag", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// NitrousServer is the server API for Nitrous service.
// All implementations must embed UnimplementedNitrousServer
// for forward compatibility
type NitrousServer interface {
	GetMany(context.Context, *GetManyRequest) (*GetManyResponse, error)
	Init(context.Context, *InitReq) (*InitResp, error)
	Lag(context.Context, *LagReq) (*LagResp, error)
	mustEmbedUnimplementedNitrousServer()
}

// UnimplementedNitrousServer must be embedded to have forward compatible implementations.
type UnimplementedNitrousServer struct {
}

func (UnimplementedNitrousServer) GetMany(context.Context, *GetManyRequest) (*GetManyResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetMany not implemented")
}
func (UnimplementedNitrousServer) Init(context.Context, *InitReq) (*InitResp, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Init not implemented")
}
func (UnimplementedNitrousServer) Lag(context.Context, *LagReq) (*LagResp, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Lag not implemented")
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

func _Nitrous_GetMany_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetManyRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(NitrousServer).GetMany(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/nitrous_old.Nitrous/GetMany",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(NitrousServer).GetMany(ctx, req.(*GetManyRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Nitrous_Init_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(InitReq)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(NitrousServer).Init(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/nitrous_old.Nitrous/Init",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(NitrousServer).Init(ctx, req.(*InitReq))
	}
	return interceptor(ctx, in, info, handler)
}

func _Nitrous_Lag_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(LagReq)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(NitrousServer).Lag(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/nitrous_old.Nitrous/Lag",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(NitrousServer).Lag(ctx, req.(*LagReq))
	}
	return interceptor(ctx, in, info, handler)
}

// Nitrous_ServiceDesc is the grpc.ServiceDesc for Nitrous service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var Nitrous_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "nitrous_old.Nitrous",
	HandlerType: (*NitrousServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "GetMany",
			Handler:    _Nitrous_GetMany_Handler,
		},
		{
			MethodName: "Init",
			Handler:    _Nitrous_Init_Handler,
		},
		{
			MethodName: "Lag",
			Handler:    _Nitrous_Lag_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "nitrous_old.proto",
}
