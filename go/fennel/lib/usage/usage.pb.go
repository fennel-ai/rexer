// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.0
// 	protoc        v3.21.1
// source: usage.proto

package usage

import (
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

type UsageCountersProto struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Number of queries.
	Queries uint64 `protobuf:"varint,1,opt,name=queries,proto3" json:"queries,omitempty"`
	// Number of actions.
	Actions uint64 `protobuf:"varint,2,opt,name=actions,proto3" json:"actions,omitempty"`
	// Time at which the counters are to be reported.
	Timestamp uint64 `protobuf:"varint,3,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
}

func (x *UsageCountersProto) Reset() {
	*x = UsageCountersProto{}
	if protoimpl.UnsafeEnabled {
		mi := &file_usage_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *UsageCountersProto) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*UsageCountersProto) ProtoMessage() {}

func (x *UsageCountersProto) ProtoReflect() protoreflect.Message {
	mi := &file_usage_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use UsageCountersProto.ProtoReflect.Descriptor instead.
func (*UsageCountersProto) Descriptor() ([]byte, []int) {
	return file_usage_proto_rawDescGZIP(), []int{0}
}

func (x *UsageCountersProto) GetQueries() uint64 {
	if x != nil {
		return x.Queries
	}
	return 0
}

func (x *UsageCountersProto) GetActions() uint64 {
	if x != nil {
		return x.Actions
	}
	return 0
}

func (x *UsageCountersProto) GetTimestamp() uint64 {
	if x != nil {
		return x.Timestamp
	}
	return 0
}

var File_usage_proto protoreflect.FileDescriptor

var file_usage_proto_rawDesc = []byte{
	0x0a, 0x0b, 0x75, 0x73, 0x61, 0x67, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x66, 0x0a,
	0x12, 0x55, 0x73, 0x61, 0x67, 0x65, 0x43, 0x6f, 0x75, 0x6e, 0x74, 0x65, 0x72, 0x73, 0x50, 0x72,
	0x6f, 0x74, 0x6f, 0x12, 0x18, 0x0a, 0x07, 0x71, 0x75, 0x65, 0x72, 0x69, 0x65, 0x73, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x04, 0x52, 0x07, 0x71, 0x75, 0x65, 0x72, 0x69, 0x65, 0x73, 0x12, 0x18, 0x0a,
	0x07, 0x61, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x18, 0x02, 0x20, 0x01, 0x28, 0x04, 0x52, 0x07,
	0x61, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x12, 0x1c, 0x0a, 0x09, 0x74, 0x69, 0x6d, 0x65, 0x73,
	0x74, 0x61, 0x6d, 0x70, 0x18, 0x03, 0x20, 0x01, 0x28, 0x04, 0x52, 0x09, 0x74, 0x69, 0x6d, 0x65,
	0x73, 0x74, 0x61, 0x6d, 0x70, 0x42, 0x12, 0x5a, 0x10, 0x66, 0x65, 0x6e, 0x6e, 0x65, 0x6c, 0x2f,
	0x6c, 0x69, 0x62, 0x2f, 0x75, 0x73, 0x61, 0x67, 0x65, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x33,
}

var (
	file_usage_proto_rawDescOnce sync.Once
	file_usage_proto_rawDescData = file_usage_proto_rawDesc
)

func file_usage_proto_rawDescGZIP() []byte {
	file_usage_proto_rawDescOnce.Do(func() {
		file_usage_proto_rawDescData = protoimpl.X.CompressGZIP(file_usage_proto_rawDescData)
	})
	return file_usage_proto_rawDescData
}

var file_usage_proto_msgTypes = make([]protoimpl.MessageInfo, 1)
var file_usage_proto_goTypes = []interface{}{
	(*UsageCountersProto)(nil), // 0: UsageCountersProto
}
var file_usage_proto_depIdxs = []int32{
	0, // [0:0] is the sub-list for method output_type
	0, // [0:0] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_usage_proto_init() }
func file_usage_proto_init() {
	if File_usage_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_usage_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*UsageCountersProto); i {
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
			RawDescriptor: file_usage_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   1,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_usage_proto_goTypes,
		DependencyIndexes: file_usage_proto_depIdxs,
		MessageInfos:      file_usage_proto_msgTypes,
	}.Build()
	File_usage_proto = out.File
	file_usage_proto_rawDesc = nil
	file_usage_proto_goTypes = nil
	file_usage_proto_depIdxs = nil
}
