// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.27.1
// 	protoc        v3.19.3
// source: query.proto

package query

import (
	proto "fennel/engine/ast/proto"
	value "fennel/lib/value"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type ProtoQueryRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	QueryId      uint64 `protobuf:"varint,1,opt,name=query_id,json=queryId,proto3" json:"query_id,omitempty"`
	Custid       uint64 `protobuf:"varint,2,opt,name=custid,proto3" json:"custid,omitempty"`
	MinTimestamp uint64 `protobuf:"varint,3,opt,name=min_timestamp,json=minTimestamp,proto3" json:"min_timestamp,omitempty"`
	MaxTimestamp uint64 `protobuf:"varint,4,opt,name=max_timestamp,json=maxTimestamp,proto3" json:"max_timestamp,omitempty"`
}

func (x *ProtoQueryRequest) Reset() {
	*x = ProtoQueryRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_query_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ProtoQueryRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ProtoQueryRequest) ProtoMessage() {}

func (x *ProtoQueryRequest) ProtoReflect() protoreflect.Message {
	mi := &file_query_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ProtoQueryRequest.ProtoReflect.Descriptor instead.
func (*ProtoQueryRequest) Descriptor() ([]byte, []int) {
	return file_query_proto_rawDescGZIP(), []int{0}
}

func (x *ProtoQueryRequest) GetQueryId() uint64 {
	if x != nil {
		return x.QueryId
	}
	return 0
}

func (x *ProtoQueryRequest) GetCustid() uint64 {
	if x != nil {
		return x.Custid
	}
	return 0
}

func (x *ProtoQueryRequest) GetMinTimestamp() uint64 {
	if x != nil {
		return x.MinTimestamp
	}
	return 0
}

func (x *ProtoQueryRequest) GetMaxTimestamp() uint64 {
	if x != nil {
		return x.MaxTimestamp
	}
	return 0
}

type ProtoAstWithDict struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Ast  *proto.Ast    `protobuf:"bytes,1,opt,name=ast,proto3" json:"ast,omitempty"`
	Dict *value.PVDict `protobuf:"bytes,2,opt,name=dict,proto3" json:"dict,omitempty"`
}

func (x *ProtoAstWithDict) Reset() {
	*x = ProtoAstWithDict{}
	if protoimpl.UnsafeEnabled {
		mi := &file_query_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ProtoAstWithDict) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ProtoAstWithDict) ProtoMessage() {}

func (x *ProtoAstWithDict) ProtoReflect() protoreflect.Message {
	mi := &file_query_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ProtoAstWithDict.ProtoReflect.Descriptor instead.
func (*ProtoAstWithDict) Descriptor() ([]byte, []int) {
	return file_query_proto_rawDescGZIP(), []int{1}
}

func (x *ProtoAstWithDict) GetAst() *proto.Ast {
	if x != nil {
		return x.Ast
	}
	return nil
}

func (x *ProtoAstWithDict) GetDict() *value.PVDict {
	if x != nil {
		return x.Dict
	}
	return nil
}

var File_query_proto protoreflect.FileDescriptor

var file_query_proto_rawDesc = []byte{
	0x0a, 0x0b, 0x71, 0x75, 0x65, 0x72, 0x79, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x09, 0x61,
	0x73, 0x74, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x0b, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x90, 0x01, 0x0a, 0x11, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x51,
	0x75, 0x65, 0x72, 0x79, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x19, 0x0a, 0x08, 0x71,
	0x75, 0x65, 0x72, 0x79, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x04, 0x52, 0x07, 0x71,
	0x75, 0x65, 0x72, 0x79, 0x49, 0x64, 0x12, 0x16, 0x0a, 0x06, 0x63, 0x75, 0x73, 0x74, 0x69, 0x64,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x04, 0x52, 0x06, 0x63, 0x75, 0x73, 0x74, 0x69, 0x64, 0x12, 0x23,
	0x0a, 0x0d, 0x6d, 0x69, 0x6e, 0x5f, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x18,
	0x03, 0x20, 0x01, 0x28, 0x04, 0x52, 0x0c, 0x6d, 0x69, 0x6e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74,
	0x61, 0x6d, 0x70, 0x12, 0x23, 0x0a, 0x0d, 0x6d, 0x61, 0x78, 0x5f, 0x74, 0x69, 0x6d, 0x65, 0x73,
	0x74, 0x61, 0x6d, 0x70, 0x18, 0x04, 0x20, 0x01, 0x28, 0x04, 0x52, 0x0c, 0x6d, 0x61, 0x78, 0x54,
	0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x22, 0x47, 0x0a, 0x10, 0x50, 0x72, 0x6f, 0x74,
	0x6f, 0x41, 0x73, 0x74, 0x57, 0x69, 0x74, 0x68, 0x44, 0x69, 0x63, 0x74, 0x12, 0x16, 0x0a, 0x03,
	0x61, 0x73, 0x74, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x04, 0x2e, 0x41, 0x73, 0x74, 0x52,
	0x03, 0x61, 0x73, 0x74, 0x12, 0x1b, 0x0a, 0x04, 0x64, 0x69, 0x63, 0x74, 0x18, 0x02, 0x20, 0x01,
	0x28, 0x0b, 0x32, 0x07, 0x2e, 0x50, 0x56, 0x44, 0x69, 0x63, 0x74, 0x52, 0x04, 0x64, 0x69, 0x63,
	0x74, 0x42, 0x12, 0x5a, 0x10, 0x66, 0x65, 0x6e, 0x6e, 0x65, 0x6c, 0x2f, 0x6c, 0x69, 0x62, 0x2f,
	0x71, 0x75, 0x65, 0x72, 0x79, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_query_proto_rawDescOnce sync.Once
	file_query_proto_rawDescData = file_query_proto_rawDesc
)

func file_query_proto_rawDescGZIP() []byte {
	file_query_proto_rawDescOnce.Do(func() {
		file_query_proto_rawDescData = protoimpl.X.CompressGZIP(file_query_proto_rawDescData)
	})
	return file_query_proto_rawDescData
}

var file_query_proto_msgTypes = make([]protoimpl.MessageInfo, 2)
var file_query_proto_goTypes = []interface{}{
	(*ProtoQueryRequest)(nil), // 0: ProtoQueryRequest
	(*ProtoAstWithDict)(nil),  // 1: ProtoAstWithDict
	(*proto.Ast)(nil),         // 2: Ast
	(*value.PVDict)(nil),      // 3: PVDict
}
var file_query_proto_depIdxs = []int32{
	2, // 0: ProtoAstWithDict.ast:type_name -> Ast
	3, // 1: ProtoAstWithDict.dict:type_name -> PVDict
	2, // [2:2] is the sub-list for method output_type
	2, // [2:2] is the sub-list for method input_type
	2, // [2:2] is the sub-list for extension type_name
	2, // [2:2] is the sub-list for extension extendee
	0, // [0:2] is the sub-list for field type_name
}

func init() { file_query_proto_init() }
func file_query_proto_init() {
	if File_query_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_query_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ProtoQueryRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_query_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ProtoAstWithDict); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_query_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   2,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_query_proto_goTypes,
		DependencyIndexes: file_query_proto_depIdxs,
		MessageInfos:      file_query_proto_msgTypes,
	}.Build()
	File_query_proto = out.File
	file_query_proto_rawDesc = nil
	file_query_proto_goTypes = nil
	file_query_proto_depIdxs = nil
}
