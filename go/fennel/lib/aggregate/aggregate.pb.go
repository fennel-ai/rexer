// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.27.1
// 	protoc        v3.19.3
// source: aggregate.proto

package aggregate

import (
	proto "fennel/engine/ast/proto"
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

type WindowType int32

const (
	WindowType_NULL       WindowType = 0
	WindowType_LAST       WindowType = 1
	WindowType_TIMESERIES WindowType = 2
)

// Enum value maps for WindowType.
var (
	WindowType_name = map[int32]string{
		0: "NULL",
		1: "LAST",
		2: "TIMESERIES",
	}
	WindowType_value = map[string]int32{
		"NULL":       0,
		"LAST":       1,
		"TIMESERIES": 2,
	}
)

func (x WindowType) Enum() *WindowType {
	p := new(WindowType)
	*p = x
	return p
}

func (x WindowType) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (WindowType) Descriptor() protoreflect.EnumDescriptor {
	return file_aggregate_proto_enumTypes[0].Descriptor()
}

func (WindowType) Type() protoreflect.EnumType {
	return &file_aggregate_proto_enumTypes[0]
}

func (x WindowType) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use WindowType.Descriptor instead.
func (WindowType) EnumDescriptor() ([]byte, []int) {
	return file_aggregate_proto_rawDescGZIP(), []int{0}
}

type ProtoAggregate struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	CustId    uint64      `protobuf:"varint,1,opt,name=cust_id,json=custId,proto3" json:"cust_id,omitempty"`
	AggType   string      `protobuf:"bytes,2,opt,name=agg_type,json=aggType,proto3" json:"agg_type,omitempty"`
	AggName   string      `protobuf:"bytes,3,opt,name=agg_name,json=aggName,proto3" json:"agg_name,omitempty"`
	Query     *proto.Ast  `protobuf:"bytes,4,opt,name=query,proto3" json:"query,omitempty"`
	Timestamp uint64      `protobuf:"varint,5,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
	Options   *AggOptions `protobuf:"bytes,6,opt,name=options,proto3" json:"options,omitempty"`
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

func (x *ProtoAggregate) GetCustId() uint64 {
	if x != nil {
		return x.CustId
	}
	return 0
}

func (x *ProtoAggregate) GetAggType() string {
	if x != nil {
		return x.AggType
	}
	return ""
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

	WindowType WindowType `protobuf:"varint,1,opt,name=window_type,json=windowType,proto3,enum=WindowType" json:"window_type,omitempty"`
	Duration   uint64     `protobuf:"varint,2,opt,name=duration,proto3" json:"duration,omitempty"`
	Retention  uint64     `protobuf:"varint,3,opt,name=retention,proto3" json:"retention,omitempty"`
	Limit      uint64     `protobuf:"varint,4,opt,name=limit,proto3" json:"limit,omitempty"`
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

func (x *AggOptions) GetWindowType() WindowType {
	if x != nil {
		return x.WindowType
	}
	return WindowType_NULL
}

func (x *AggOptions) GetDuration() uint64 {
	if x != nil {
		return x.Duration
	}
	return 0
}

func (x *AggOptions) GetRetention() uint64 {
	if x != nil {
		return x.Retention
	}
	return 0
}

func (x *AggOptions) GetLimit() uint64 {
	if x != nil {
		return x.Limit
	}
	return 0
}

type AggRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	AggType string `protobuf:"bytes,1,opt,name=agg_type,json=aggType,proto3" json:"agg_type,omitempty"`
	AggName string `protobuf:"bytes,2,opt,name=agg_name,json=aggName,proto3" json:"agg_name,omitempty"`
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

func (x *AggRequest) GetAggType() string {
	if x != nil {
		return x.AggType
	}
	return ""
}

func (x *AggRequest) GetAggName() string {
	if x != nil {
		return x.AggName
	}
	return ""
}

var File_aggregate_proto protoreflect.FileDescriptor

var file_aggregate_proto_rawDesc = []byte{
	0x0a, 0x0f, 0x61, 0x67, 0x67, 0x72, 0x65, 0x67, 0x61, 0x74, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x1a, 0x09, 0x61, 0x73, 0x74, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0xc0, 0x01, 0x0a,
	0x0e, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x41, 0x67, 0x67, 0x72, 0x65, 0x67, 0x61, 0x74, 0x65, 0x12,
	0x17, 0x0a, 0x07, 0x63, 0x75, 0x73, 0x74, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x04,
	0x52, 0x06, 0x63, 0x75, 0x73, 0x74, 0x49, 0x64, 0x12, 0x19, 0x0a, 0x08, 0x61, 0x67, 0x67, 0x5f,
	0x74, 0x79, 0x70, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x61, 0x67, 0x67, 0x54,
	0x79, 0x70, 0x65, 0x12, 0x19, 0x0a, 0x08, 0x61, 0x67, 0x67, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18,
	0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x61, 0x67, 0x67, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x1a,
	0x0a, 0x05, 0x71, 0x75, 0x65, 0x72, 0x79, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x04, 0x2e,
	0x41, 0x73, 0x74, 0x52, 0x05, 0x71, 0x75, 0x65, 0x72, 0x79, 0x12, 0x1c, 0x0a, 0x09, 0x74, 0x69,
	0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x18, 0x05, 0x20, 0x01, 0x28, 0x04, 0x52, 0x09, 0x74,
	0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x12, 0x25, 0x0a, 0x07, 0x6f, 0x70, 0x74, 0x69,
	0x6f, 0x6e, 0x73, 0x18, 0x06, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x0b, 0x2e, 0x41, 0x67, 0x67, 0x4f,
	0x70, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x52, 0x07, 0x6f, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x22,
	0x8a, 0x01, 0x0a, 0x0a, 0x41, 0x67, 0x67, 0x4f, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x12, 0x2c,
	0x0a, 0x0b, 0x77, 0x69, 0x6e, 0x64, 0x6f, 0x77, 0x5f, 0x74, 0x79, 0x70, 0x65, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x0e, 0x32, 0x0b, 0x2e, 0x57, 0x69, 0x6e, 0x64, 0x6f, 0x77, 0x54, 0x79, 0x70, 0x65,
	0x52, 0x0a, 0x77, 0x69, 0x6e, 0x64, 0x6f, 0x77, 0x54, 0x79, 0x70, 0x65, 0x12, 0x1a, 0x0a, 0x08,
	0x64, 0x75, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x18, 0x02, 0x20, 0x01, 0x28, 0x04, 0x52, 0x08,
	0x64, 0x75, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x1c, 0x0a, 0x09, 0x72, 0x65, 0x74, 0x65,
	0x6e, 0x74, 0x69, 0x6f, 0x6e, 0x18, 0x03, 0x20, 0x01, 0x28, 0x04, 0x52, 0x09, 0x72, 0x65, 0x74,
	0x65, 0x6e, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x14, 0x0a, 0x05, 0x6c, 0x69, 0x6d, 0x69, 0x74, 0x18,
	0x04, 0x20, 0x01, 0x28, 0x04, 0x52, 0x05, 0x6c, 0x69, 0x6d, 0x69, 0x74, 0x22, 0x42, 0x0a, 0x0a,
	0x41, 0x67, 0x67, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x19, 0x0a, 0x08, 0x61, 0x67,
	0x67, 0x5f, 0x74, 0x79, 0x70, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x61, 0x67,
	0x67, 0x54, 0x79, 0x70, 0x65, 0x12, 0x19, 0x0a, 0x08, 0x61, 0x67, 0x67, 0x5f, 0x6e, 0x61, 0x6d,
	0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x61, 0x67, 0x67, 0x4e, 0x61, 0x6d, 0x65,
	0x2a, 0x30, 0x0a, 0x0a, 0x57, 0x69, 0x6e, 0x64, 0x6f, 0x77, 0x54, 0x79, 0x70, 0x65, 0x12, 0x08,
	0x0a, 0x04, 0x4e, 0x55, 0x4c, 0x4c, 0x10, 0x00, 0x12, 0x08, 0x0a, 0x04, 0x4c, 0x41, 0x53, 0x54,
	0x10, 0x01, 0x12, 0x0e, 0x0a, 0x0a, 0x54, 0x49, 0x4d, 0x45, 0x53, 0x45, 0x52, 0x49, 0x45, 0x53,
	0x10, 0x02, 0x42, 0x16, 0x5a, 0x14, 0x66, 0x65, 0x6e, 0x6e, 0x65, 0x6c, 0x2f, 0x6c, 0x69, 0x62,
	0x2f, 0x61, 0x67, 0x67, 0x72, 0x65, 0x67, 0x61, 0x74, 0x65, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x33,
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

var file_aggregate_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_aggregate_proto_msgTypes = make([]protoimpl.MessageInfo, 3)
var file_aggregate_proto_goTypes = []interface{}{
	(WindowType)(0),        // 0: WindowType
	(*ProtoAggregate)(nil), // 1: ProtoAggregate
	(*AggOptions)(nil),     // 2: AggOptions
	(*AggRequest)(nil),     // 3: AggRequest
	(*proto.Ast)(nil),      // 4: Ast
}
var file_aggregate_proto_depIdxs = []int32{
	4, // 0: ProtoAggregate.query:type_name -> Ast
	2, // 1: ProtoAggregate.options:type_name -> AggOptions
	0, // 2: AggOptions.window_type:type_name -> WindowType
	3, // [3:3] is the sub-list for method output_type
	3, // [3:3] is the sub-list for method input_type
	3, // [3:3] is the sub-list for extension type_name
	3, // [3:3] is the sub-list for extension extendee
	0, // [0:3] is the sub-list for field type_name
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
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_aggregate_proto_rawDesc,
			NumEnums:      1,
			NumMessages:   3,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_aggregate_proto_goTypes,
		DependencyIndexes: file_aggregate_proto_depIdxs,
		EnumInfos:         file_aggregate_proto_enumTypes,
		MessageInfos:      file_aggregate_proto_msgTypes,
	}.Build()
	File_aggregate_proto = out.File
	file_aggregate_proto_rawDesc = nil
	file_aggregate_proto_goTypes = nil
	file_aggregate_proto_depIdxs = nil
}
