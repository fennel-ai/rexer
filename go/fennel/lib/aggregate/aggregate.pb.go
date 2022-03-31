// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.0
// 	protoc        v3.17.3
// source: aggregate.proto

package aggregate

import (
	proto "fennel/engine/ast/proto"
	ftypes "fennel/lib/ftypes"
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

type ProtoAggregate struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	AggName   string      `protobuf:"bytes,1,opt,name=agg_name,json=aggName,proto3" json:"agg_name,omitempty"`
	Query     *proto.Ast  `protobuf:"bytes,2,opt,name=query,proto3" json:"query,omitempty"`
	Timestamp uint64      `protobuf:"varint,3,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
	Options   *AggOptions `protobuf:"bytes,4,opt,name=options,proto3" json:"options,omitempty"`
}

func (x *ProtoAggregate) Reset() {
	*x = ProtoAggregate{}
	if protoimpl.UnsafeEnabled {
		mi := &file_aggregate_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ProtoAggregate) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ProtoAggregate) ProtoMessage() {}

func (x *ProtoAggregate) ProtoReflect() protoreflect.Message {
	mi := &file_aggregate_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ProtoAggregate.ProtoReflect.Descriptor instead.
func (*ProtoAggregate) Descriptor() ([]byte, []int) {
	return file_aggregate_proto_rawDescGZIP(), []int{0}
}

func (x *ProtoAggregate) GetAggName() string {
	if x != nil {
		return x.AggName
	}
	return ""
}

func (x *ProtoAggregate) GetQuery() *proto.Ast {
	if x != nil {
		return x.Query
	}
	return nil
}

func (x *ProtoAggregate) GetTimestamp() uint64 {
	if x != nil {
		return x.Timestamp
	}
	return 0
}

func (x *ProtoAggregate) GetOptions() *AggOptions {
	if x != nil {
		return x.Options
	}
	return nil
}

type AggOptions struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	AggType   string        `protobuf:"bytes,1,opt,name=agg_type,json=aggType,proto3" json:"agg_type,omitempty"`
	Durations []uint64      `protobuf:"varint,2,rep,packed,name=durations,proto3" json:"durations,omitempty"`
	Window    ftypes.Window `protobuf:"varint,3,opt,name=window,proto3,enum=Window" json:"window,omitempty"`
	Limit     uint64        `protobuf:"varint,4,opt,name=limit,proto3" json:"limit,omitempty"`
	Normalize bool          `protobuf:"varint,5,opt,name=normalize,proto3" json:"normalize,omitempty"`
}

func (x *AggOptions) Reset() {
	*x = AggOptions{}
	if protoimpl.UnsafeEnabled {
		mi := &file_aggregate_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *AggOptions) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AggOptions) ProtoMessage() {}

func (x *AggOptions) ProtoReflect() protoreflect.Message {
	mi := &file_aggregate_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use AggOptions.ProtoReflect.Descriptor instead.
func (*AggOptions) Descriptor() ([]byte, []int) {
	return file_aggregate_proto_rawDescGZIP(), []int{1}
}

func (x *AggOptions) GetAggType() string {
	if x != nil {
		return x.AggType
	}
	return ""
}

func (x *AggOptions) GetDurations() []uint64 {
	if x != nil {
		return x.Durations
	}
	return nil
}

func (x *AggOptions) GetWindow() ftypes.Window {
	if x != nil {
		return x.Window
	}
	return ftypes.Window(0)
}

func (x *AggOptions) GetLimit() uint64 {
	if x != nil {
		return x.Limit
	}
	return 0
}

func (x *AggOptions) GetNormalize() bool {
	if x != nil {
		return x.Normalize
	}
	return false
}

type AggRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	AggName string `protobuf:"bytes,1,opt,name=agg_name,json=aggName,proto3" json:"agg_name,omitempty"`
}

func (x *AggRequest) Reset() {
	*x = AggRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_aggregate_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *AggRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AggRequest) ProtoMessage() {}

func (x *AggRequest) ProtoReflect() protoreflect.Message {
	mi := &file_aggregate_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use AggRequest.ProtoReflect.Descriptor instead.
func (*AggRequest) Descriptor() ([]byte, []int) {
	return file_aggregate_proto_rawDescGZIP(), []int{2}
}

func (x *AggRequest) GetAggName() string {
	if x != nil {
		return x.AggName
	}
	return ""
}

type ProtoGetAggValueRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	AggName string        `protobuf:"bytes,1,opt,name=agg_name,json=aggName,proto3" json:"agg_name,omitempty"`
	Key     *value.PValue `protobuf:"bytes,2,opt,name=key,proto3" json:"key,omitempty"`
}

func (x *ProtoGetAggValueRequest) Reset() {
	*x = ProtoGetAggValueRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_aggregate_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ProtoGetAggValueRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ProtoGetAggValueRequest) ProtoMessage() {}

func (x *ProtoGetAggValueRequest) ProtoReflect() protoreflect.Message {
	mi := &file_aggregate_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ProtoGetAggValueRequest.ProtoReflect.Descriptor instead.
func (*ProtoGetAggValueRequest) Descriptor() ([]byte, []int) {
	return file_aggregate_proto_rawDescGZIP(), []int{3}
}

func (x *ProtoGetAggValueRequest) GetAggName() string {
	if x != nil {
		return x.AggName
	}
	return ""
}

func (x *ProtoGetAggValueRequest) GetKey() *value.PValue {
	if x != nil {
		return x.Key
	}
	return nil
}

var File_aggregate_proto protoreflect.FileDescriptor

var file_aggregate_proto_rawDesc = []byte{
	0x0a, 0x0f, 0x61, 0x67, 0x67, 0x72, 0x65, 0x67, 0x61, 0x74, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x1a, 0x09, 0x61, 0x73, 0x74, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x0c, 0x66, 0x74,
	0x79, 0x70, 0x65, 0x73, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x0b, 0x76, 0x61, 0x6c, 0x75,
	0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x8c, 0x01, 0x0a, 0x0e, 0x50, 0x72, 0x6f, 0x74,
	0x6f, 0x41, 0x67, 0x67, 0x72, 0x65, 0x67, 0x61, 0x74, 0x65, 0x12, 0x19, 0x0a, 0x08, 0x61, 0x67,
	0x67, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x61, 0x67,
	0x67, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x1a, 0x0a, 0x05, 0x71, 0x75, 0x65, 0x72, 0x79, 0x18, 0x02,
	0x20, 0x01, 0x28, 0x0b, 0x32, 0x04, 0x2e, 0x41, 0x73, 0x74, 0x52, 0x05, 0x71, 0x75, 0x65, 0x72,
	0x79, 0x12, 0x1c, 0x0a, 0x09, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x18, 0x03,
	0x20, 0x01, 0x28, 0x04, 0x52, 0x09, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x12,
	0x25, 0x0a, 0x07, 0x6f, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0b,
	0x32, 0x0b, 0x2e, 0x41, 0x67, 0x67, 0x4f, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x52, 0x07, 0x6f,
	0x70, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x22, 0x9a, 0x01, 0x0a, 0x0a, 0x41, 0x67, 0x67, 0x4f, 0x70,
	0x74, 0x69, 0x6f, 0x6e, 0x73, 0x12, 0x19, 0x0a, 0x08, 0x61, 0x67, 0x67, 0x5f, 0x74, 0x79, 0x70,
	0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x61, 0x67, 0x67, 0x54, 0x79, 0x70, 0x65,
	0x12, 0x1c, 0x0a, 0x09, 0x64, 0x75, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x18, 0x02, 0x20,
	0x03, 0x28, 0x04, 0x52, 0x09, 0x64, 0x75, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x12, 0x1f,
	0x0a, 0x06, 0x77, 0x69, 0x6e, 0x64, 0x6f, 0x77, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x07,
	0x2e, 0x57, 0x69, 0x6e, 0x64, 0x6f, 0x77, 0x52, 0x06, 0x77, 0x69, 0x6e, 0x64, 0x6f, 0x77, 0x12,
	0x14, 0x0a, 0x05, 0x6c, 0x69, 0x6d, 0x69, 0x74, 0x18, 0x04, 0x20, 0x01, 0x28, 0x04, 0x52, 0x05,
	0x6c, 0x69, 0x6d, 0x69, 0x74, 0x12, 0x1c, 0x0a, 0x09, 0x6e, 0x6f, 0x72, 0x6d, 0x61, 0x6c, 0x69,
	0x7a, 0x65, 0x18, 0x05, 0x20, 0x01, 0x28, 0x08, 0x52, 0x09, 0x6e, 0x6f, 0x72, 0x6d, 0x61, 0x6c,
	0x69, 0x7a, 0x65, 0x22, 0x27, 0x0a, 0x0a, 0x41, 0x67, 0x67, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73,
	0x74, 0x12, 0x19, 0x0a, 0x08, 0x61, 0x67, 0x67, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x07, 0x61, 0x67, 0x67, 0x4e, 0x61, 0x6d, 0x65, 0x22, 0x4f, 0x0a, 0x17,
	0x50, 0x72, 0x6f, 0x74, 0x6f, 0x47, 0x65, 0x74, 0x41, 0x67, 0x67, 0x56, 0x61, 0x6c, 0x75, 0x65,
	0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x19, 0x0a, 0x08, 0x61, 0x67, 0x67, 0x5f, 0x6e,
	0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x61, 0x67, 0x67, 0x4e, 0x61,
	0x6d, 0x65, 0x12, 0x19, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32,
	0x07, 0x2e, 0x50, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x42, 0x16, 0x5a,
	0x14, 0x66, 0x65, 0x6e, 0x6e, 0x65, 0x6c, 0x2f, 0x6c, 0x69, 0x62, 0x2f, 0x61, 0x67, 0x67, 0x72,
	0x65, 0x67, 0x61, 0x74, 0x65, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_aggregate_proto_rawDescOnce sync.Once
	file_aggregate_proto_rawDescData = file_aggregate_proto_rawDesc
)

func file_aggregate_proto_rawDescGZIP() []byte {
	file_aggregate_proto_rawDescOnce.Do(func() {
		file_aggregate_proto_rawDescData = protoimpl.X.CompressGZIP(file_aggregate_proto_rawDescData)
	})
	return file_aggregate_proto_rawDescData
}

var file_aggregate_proto_msgTypes = make([]protoimpl.MessageInfo, 4)
var file_aggregate_proto_goTypes = []interface{}{
	(*ProtoAggregate)(nil),          // 0: ProtoAggregate
	(*AggOptions)(nil),              // 1: AggOptions
	(*AggRequest)(nil),              // 2: AggRequest
	(*ProtoGetAggValueRequest)(nil), // 3: ProtoGetAggValueRequest
	(*proto.Ast)(nil),               // 4: Ast
	(ftypes.Window)(0),              // 5: Window
	(*value.PValue)(nil),            // 6: PValue
}
var file_aggregate_proto_depIdxs = []int32{
	4, // 0: ProtoAggregate.query:type_name -> Ast
	1, // 1: ProtoAggregate.options:type_name -> AggOptions
	5, // 2: AggOptions.window:type_name -> Window
	6, // 3: ProtoGetAggValueRequest.key:type_name -> PValue
	4, // [4:4] is the sub-list for method output_type
	4, // [4:4] is the sub-list for method input_type
	4, // [4:4] is the sub-list for extension type_name
	4, // [4:4] is the sub-list for extension extendee
	0, // [0:4] is the sub-list for field type_name
}

func init() { file_aggregate_proto_init() }
func file_aggregate_proto_init() {
	if File_aggregate_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_aggregate_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ProtoAggregate); i {
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
		file_aggregate_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*AggOptions); i {
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
		file_aggregate_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*AggRequest); i {
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
		file_aggregate_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ProtoGetAggValueRequest); i {
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
			RawDescriptor: file_aggregate_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   4,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_aggregate_proto_goTypes,
		DependencyIndexes: file_aggregate_proto_depIdxs,
		MessageInfos:      file_aggregate_proto_msgTypes,
	}.Build()
	File_aggregate_proto = out.File
	file_aggregate_proto_rawDesc = nil
	file_aggregate_proto_goTypes = nil
	file_aggregate_proto_depIdxs = nil
}
