// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.27.1
// 	protoc        v3.19.3
// source: counter.proto

package counter

import (
	ftypes "fennel/lib/ftypes"
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

type CounterType int32

const (
	CounterType_NULL_COUNTER             CounterType = 0
	CounterType_USER_LIKE                CounterType = 1
	CounterType_USER_SHARE               CounterType = 2
	CounterType_VIDEO_LIKE               CounterType = 3
	CounterType_VIDEO_SHARE              CounterType = 4
	CounterType_USER_ACCOUNT_LIKE        CounterType = 5
	CounterType_USER_TOPIC_LIKE          CounterType = 6
	CounterType_AGE_VIDEO_LIKE           CounterType = 7
	CounterType_GENDER_AGE_VIDEO_LIKE    CounterType = 8
	CounterType_ZIP_ACCOUNT_LIKE         CounterType = 9
	CounterType_AGE_ZIP_U2VCLUSTER_LIKE  CounterType = 10
	CounterType_PAGE_FOLLOWER_VIDEO_LIKE CounterType = 11
	CounterType_USER_VIDEO_30SWATCH      CounterType = 12
	CounterType_USER_VIDEO_LIKE          CounterType = 13
)

// Enum value maps for CounterType.
var (
	CounterType_name = map[int32]string{
		0:  "NULL_COUNTER",
		1:  "USER_LIKE",
		2:  "USER_SHARE",
		3:  "VIDEO_LIKE",
		4:  "VIDEO_SHARE",
		5:  "USER_ACCOUNT_LIKE",
		6:  "USER_TOPIC_LIKE",
		7:  "AGE_VIDEO_LIKE",
		8:  "GENDER_AGE_VIDEO_LIKE",
		9:  "ZIP_ACCOUNT_LIKE",
		10: "AGE_ZIP_U2VCLUSTER_LIKE",
		11: "PAGE_FOLLOWER_VIDEO_LIKE",
		12: "USER_VIDEO_30SWATCH",
		13: "USER_VIDEO_LIKE",
	}
	CounterType_value = map[string]int32{
		"NULL_COUNTER":             0,
		"USER_LIKE":                1,
		"USER_SHARE":               2,
		"VIDEO_LIKE":               3,
		"VIDEO_SHARE":              4,
		"USER_ACCOUNT_LIKE":        5,
		"USER_TOPIC_LIKE":          6,
		"AGE_VIDEO_LIKE":           7,
		"GENDER_AGE_VIDEO_LIKE":    8,
		"ZIP_ACCOUNT_LIKE":         9,
		"AGE_ZIP_U2VCLUSTER_LIKE":  10,
		"PAGE_FOLLOWER_VIDEO_LIKE": 11,
		"USER_VIDEO_30SWATCH":      12,
		"USER_VIDEO_LIKE":          13,
	}
)

func (x CounterType) Enum() *CounterType {
	p := new(CounterType)
	*p = x
	return p
}

func (x CounterType) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (CounterType) Descriptor() protoreflect.EnumDescriptor {
	return file_counter_proto_enumTypes[0].Descriptor()
}

func (CounterType) Type() protoreflect.EnumType {
	return &file_counter_proto_enumTypes[0]
}

func (x CounterType) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use CounterType.Descriptor instead.
func (CounterType) EnumDescriptor() ([]byte, []int) {
	return file_counter_proto_rawDescGZIP(), []int{0}
}

type ProtoGetCountRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	CounterType CounterType   `protobuf:"varint,1,opt,name=CounterType,proto3,enum=CounterType" json:"CounterType,omitempty"`
	Window      ftypes.Window `protobuf:"varint,2,opt,name=Window,proto3,enum=Window" json:"Window,omitempty"`
	Key         []uint64      `protobuf:"varint,3,rep,packed,name=Key,proto3" json:"Key,omitempty"`
	Timestamp   uint64        `protobuf:"varint,4,opt,name=Timestamp,proto3" json:"Timestamp,omitempty"`
}

func (x *ProtoGetCountRequest) Reset() {
	*x = ProtoGetCountRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_counter_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ProtoGetCountRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ProtoGetCountRequest) ProtoMessage() {}

func (x *ProtoGetCountRequest) ProtoReflect() protoreflect.Message {
	mi := &file_counter_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ProtoGetCountRequest.ProtoReflect.Descriptor instead.
func (*ProtoGetCountRequest) Descriptor() ([]byte, []int) {
	return file_counter_proto_rawDescGZIP(), []int{0}
}

func (x *ProtoGetCountRequest) GetCounterType() CounterType {
	if x != nil {
		return x.CounterType
	}
	return CounterType_NULL_COUNTER
}

func (x *ProtoGetCountRequest) GetWindow() ftypes.Window {
	if x != nil {
		return x.Window
	}
	return ftypes.Window(0)
}

func (x *ProtoGetCountRequest) GetKey() []uint64 {
	if x != nil {
		return x.Key
	}
	return nil
}

func (x *ProtoGetCountRequest) GetTimestamp() uint64 {
	if x != nil {
		return x.Timestamp
	}
	return 0
}

type ProtoGetRateRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	NumCounterType CounterType   `protobuf:"varint,1,opt,name=NumCounterType,proto3,enum=CounterType" json:"NumCounterType,omitempty"`
	DenCounterType CounterType   `protobuf:"varint,2,opt,name=DenCounterType,proto3,enum=CounterType" json:"DenCounterType,omitempty"`
	NumKey         []uint64      `protobuf:"varint,3,rep,packed,name=NumKey,proto3" json:"NumKey,omitempty"`
	DenKey         []uint64      `protobuf:"varint,4,rep,packed,name=DenKey,proto3" json:"DenKey,omitempty"`
	Window         ftypes.Window `protobuf:"varint,5,opt,name=Window,proto3,enum=Window" json:"Window,omitempty"`
	Timestamp      uint64        `protobuf:"varint,6,opt,name=Timestamp,proto3" json:"Timestamp,omitempty"`
	LowerBound     bool          `protobuf:"varint,7,opt,name=LowerBound,proto3" json:"LowerBound,omitempty"`
}

func (x *ProtoGetRateRequest) Reset() {
	*x = ProtoGetRateRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_counter_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ProtoGetRateRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ProtoGetRateRequest) ProtoMessage() {}

func (x *ProtoGetRateRequest) ProtoReflect() protoreflect.Message {
	mi := &file_counter_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ProtoGetRateRequest.ProtoReflect.Descriptor instead.
func (*ProtoGetRateRequest) Descriptor() ([]byte, []int) {
	return file_counter_proto_rawDescGZIP(), []int{1}
}

func (x *ProtoGetRateRequest) GetNumCounterType() CounterType {
	if x != nil {
		return x.NumCounterType
	}
	return CounterType_NULL_COUNTER
}

func (x *ProtoGetRateRequest) GetDenCounterType() CounterType {
	if x != nil {
		return x.DenCounterType
	}
	return CounterType_NULL_COUNTER
}

func (x *ProtoGetRateRequest) GetNumKey() []uint64 {
	if x != nil {
		return x.NumKey
	}
	return nil
}

func (x *ProtoGetRateRequest) GetDenKey() []uint64 {
	if x != nil {
		return x.DenKey
	}
	return nil
}

func (x *ProtoGetRateRequest) GetWindow() ftypes.Window {
	if x != nil {
		return x.Window
	}
	return ftypes.Window(0)
}

func (x *ProtoGetRateRequest) GetTimestamp() uint64 {
	if x != nil {
		return x.Timestamp
	}
	return 0
}

func (x *ProtoGetRateRequest) GetLowerBound() bool {
	if x != nil {
		return x.LowerBound
	}
	return false
}

var File_counter_proto protoreflect.FileDescriptor

var file_counter_proto_rawDesc = []byte{
	0x0a, 0x0d, 0x63, 0x6f, 0x75, 0x6e, 0x74, 0x65, 0x72, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a,
	0x0c, 0x66, 0x74, 0x79, 0x70, 0x65, 0x73, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x97, 0x01,
	0x0a, 0x14, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x47, 0x65, 0x74, 0x43, 0x6f, 0x75, 0x6e, 0x74, 0x52,
	0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x2e, 0x0a, 0x0b, 0x43, 0x6f, 0x75, 0x6e, 0x74, 0x65,
	0x72, 0x54, 0x79, 0x70, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x0c, 0x2e, 0x43, 0x6f,
	0x75, 0x6e, 0x74, 0x65, 0x72, 0x54, 0x79, 0x70, 0x65, 0x52, 0x0b, 0x43, 0x6f, 0x75, 0x6e, 0x74,
	0x65, 0x72, 0x54, 0x79, 0x70, 0x65, 0x12, 0x1f, 0x0a, 0x06, 0x57, 0x69, 0x6e, 0x64, 0x6f, 0x77,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x07, 0x2e, 0x57, 0x69, 0x6e, 0x64, 0x6f, 0x77, 0x52,
	0x06, 0x57, 0x69, 0x6e, 0x64, 0x6f, 0x77, 0x12, 0x10, 0x0a, 0x03, 0x4b, 0x65, 0x79, 0x18, 0x03,
	0x20, 0x03, 0x28, 0x04, 0x52, 0x03, 0x4b, 0x65, 0x79, 0x12, 0x1c, 0x0a, 0x09, 0x54, 0x69, 0x6d,
	0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x18, 0x04, 0x20, 0x01, 0x28, 0x04, 0x52, 0x09, 0x54, 0x69,
	0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x22, 0x90, 0x02, 0x0a, 0x13, 0x50, 0x72, 0x6f, 0x74,
	0x6f, 0x47, 0x65, 0x74, 0x52, 0x61, 0x74, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12,
	0x34, 0x0a, 0x0e, 0x4e, 0x75, 0x6d, 0x43, 0x6f, 0x75, 0x6e, 0x74, 0x65, 0x72, 0x54, 0x79, 0x70,
	0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x0c, 0x2e, 0x43, 0x6f, 0x75, 0x6e, 0x74, 0x65,
	0x72, 0x54, 0x79, 0x70, 0x65, 0x52, 0x0e, 0x4e, 0x75, 0x6d, 0x43, 0x6f, 0x75, 0x6e, 0x74, 0x65,
	0x72, 0x54, 0x79, 0x70, 0x65, 0x12, 0x34, 0x0a, 0x0e, 0x44, 0x65, 0x6e, 0x43, 0x6f, 0x75, 0x6e,
	0x74, 0x65, 0x72, 0x54, 0x79, 0x70, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x0c, 0x2e,
	0x43, 0x6f, 0x75, 0x6e, 0x74, 0x65, 0x72, 0x54, 0x79, 0x70, 0x65, 0x52, 0x0e, 0x44, 0x65, 0x6e,
	0x43, 0x6f, 0x75, 0x6e, 0x74, 0x65, 0x72, 0x54, 0x79, 0x70, 0x65, 0x12, 0x16, 0x0a, 0x06, 0x4e,
	0x75, 0x6d, 0x4b, 0x65, 0x79, 0x18, 0x03, 0x20, 0x03, 0x28, 0x04, 0x52, 0x06, 0x4e, 0x75, 0x6d,
	0x4b, 0x65, 0x79, 0x12, 0x16, 0x0a, 0x06, 0x44, 0x65, 0x6e, 0x4b, 0x65, 0x79, 0x18, 0x04, 0x20,
	0x03, 0x28, 0x04, 0x52, 0x06, 0x44, 0x65, 0x6e, 0x4b, 0x65, 0x79, 0x12, 0x1f, 0x0a, 0x06, 0x57,
	0x69, 0x6e, 0x64, 0x6f, 0x77, 0x18, 0x05, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x07, 0x2e, 0x57, 0x69,
	0x6e, 0x64, 0x6f, 0x77, 0x52, 0x06, 0x57, 0x69, 0x6e, 0x64, 0x6f, 0x77, 0x12, 0x1c, 0x0a, 0x09,
	0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x18, 0x06, 0x20, 0x01, 0x28, 0x04, 0x52,
	0x09, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x12, 0x1e, 0x0a, 0x0a, 0x4c, 0x6f,
	0x77, 0x65, 0x72, 0x42, 0x6f, 0x75, 0x6e, 0x64, 0x18, 0x07, 0x20, 0x01, 0x28, 0x08, 0x52, 0x0a,
	0x4c, 0x6f, 0x77, 0x65, 0x72, 0x42, 0x6f, 0x75, 0x6e, 0x64, 0x2a, 0xb9, 0x02, 0x0a, 0x0b, 0x43,
	0x6f, 0x75, 0x6e, 0x74, 0x65, 0x72, 0x54, 0x79, 0x70, 0x65, 0x12, 0x10, 0x0a, 0x0c, 0x4e, 0x55,
	0x4c, 0x4c, 0x5f, 0x43, 0x4f, 0x55, 0x4e, 0x54, 0x45, 0x52, 0x10, 0x00, 0x12, 0x0d, 0x0a, 0x09,
	0x55, 0x53, 0x45, 0x52, 0x5f, 0x4c, 0x49, 0x4b, 0x45, 0x10, 0x01, 0x12, 0x0e, 0x0a, 0x0a, 0x55,
	0x53, 0x45, 0x52, 0x5f, 0x53, 0x48, 0x41, 0x52, 0x45, 0x10, 0x02, 0x12, 0x0e, 0x0a, 0x0a, 0x56,
	0x49, 0x44, 0x45, 0x4f, 0x5f, 0x4c, 0x49, 0x4b, 0x45, 0x10, 0x03, 0x12, 0x0f, 0x0a, 0x0b, 0x56,
	0x49, 0x44, 0x45, 0x4f, 0x5f, 0x53, 0x48, 0x41, 0x52, 0x45, 0x10, 0x04, 0x12, 0x15, 0x0a, 0x11,
	0x55, 0x53, 0x45, 0x52, 0x5f, 0x41, 0x43, 0x43, 0x4f, 0x55, 0x4e, 0x54, 0x5f, 0x4c, 0x49, 0x4b,
	0x45, 0x10, 0x05, 0x12, 0x13, 0x0a, 0x0f, 0x55, 0x53, 0x45, 0x52, 0x5f, 0x54, 0x4f, 0x50, 0x49,
	0x43, 0x5f, 0x4c, 0x49, 0x4b, 0x45, 0x10, 0x06, 0x12, 0x12, 0x0a, 0x0e, 0x41, 0x47, 0x45, 0x5f,
	0x56, 0x49, 0x44, 0x45, 0x4f, 0x5f, 0x4c, 0x49, 0x4b, 0x45, 0x10, 0x07, 0x12, 0x19, 0x0a, 0x15,
	0x47, 0x45, 0x4e, 0x44, 0x45, 0x52, 0x5f, 0x41, 0x47, 0x45, 0x5f, 0x56, 0x49, 0x44, 0x45, 0x4f,
	0x5f, 0x4c, 0x49, 0x4b, 0x45, 0x10, 0x08, 0x12, 0x14, 0x0a, 0x10, 0x5a, 0x49, 0x50, 0x5f, 0x41,
	0x43, 0x43, 0x4f, 0x55, 0x4e, 0x54, 0x5f, 0x4c, 0x49, 0x4b, 0x45, 0x10, 0x09, 0x12, 0x1b, 0x0a,
	0x17, 0x41, 0x47, 0x45, 0x5f, 0x5a, 0x49, 0x50, 0x5f, 0x55, 0x32, 0x56, 0x43, 0x4c, 0x55, 0x53,
	0x54, 0x45, 0x52, 0x5f, 0x4c, 0x49, 0x4b, 0x45, 0x10, 0x0a, 0x12, 0x1c, 0x0a, 0x18, 0x50, 0x41,
	0x47, 0x45, 0x5f, 0x46, 0x4f, 0x4c, 0x4c, 0x4f, 0x57, 0x45, 0x52, 0x5f, 0x56, 0x49, 0x44, 0x45,
	0x4f, 0x5f, 0x4c, 0x49, 0x4b, 0x45, 0x10, 0x0b, 0x12, 0x17, 0x0a, 0x13, 0x55, 0x53, 0x45, 0x52,
	0x5f, 0x56, 0x49, 0x44, 0x45, 0x4f, 0x5f, 0x33, 0x30, 0x53, 0x57, 0x41, 0x54, 0x43, 0x48, 0x10,
	0x0c, 0x12, 0x13, 0x0a, 0x0f, 0x55, 0x53, 0x45, 0x52, 0x5f, 0x56, 0x49, 0x44, 0x45, 0x4f, 0x5f,
	0x4c, 0x49, 0x4b, 0x45, 0x10, 0x0d, 0x42, 0x14, 0x5a, 0x12, 0x66, 0x65, 0x6e, 0x6e, 0x65, 0x6c,
	0x2f, 0x6c, 0x69, 0x62, 0x2f, 0x63, 0x6f, 0x75, 0x6e, 0x74, 0x65, 0x72, 0x62, 0x06, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_counter_proto_rawDescOnce sync.Once
	file_counter_proto_rawDescData = file_counter_proto_rawDesc
)

func file_counter_proto_rawDescGZIP() []byte {
	file_counter_proto_rawDescOnce.Do(func() {
		file_counter_proto_rawDescData = protoimpl.X.CompressGZIP(file_counter_proto_rawDescData)
	})
	return file_counter_proto_rawDescData
}

var file_counter_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_counter_proto_msgTypes = make([]protoimpl.MessageInfo, 2)
var file_counter_proto_goTypes = []interface{}{
	(CounterType)(0),             // 0: CounterType
	(*ProtoGetCountRequest)(nil), // 1: ProtoGetCountRequest
	(*ProtoGetRateRequest)(nil),  // 2: ProtoGetRateRequest
	(ftypes.Window)(0),           // 3: Window
}
var file_counter_proto_depIdxs = []int32{
	0, // 0: ProtoGetCountRequest.CounterType:type_name -> CounterType
	3, // 1: ProtoGetCountRequest.Window:type_name -> Window
	0, // 2: ProtoGetRateRequest.NumCounterType:type_name -> CounterType
	0, // 3: ProtoGetRateRequest.DenCounterType:type_name -> CounterType
	3, // 4: ProtoGetRateRequest.Window:type_name -> Window
	5, // [5:5] is the sub-list for method output_type
	5, // [5:5] is the sub-list for method input_type
	5, // [5:5] is the sub-list for extension type_name
	5, // [5:5] is the sub-list for extension extendee
	0, // [0:5] is the sub-list for field type_name
}

func init() { file_counter_proto_init() }
func file_counter_proto_init() {
	if File_counter_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_counter_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ProtoGetCountRequest); i {
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
		file_counter_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ProtoGetRateRequest); i {
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
			RawDescriptor: file_counter_proto_rawDesc,
			NumEnums:      1,
			NumMessages:   2,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_counter_proto_goTypes,
		DependencyIndexes: file_counter_proto_depIdxs,
		EnumInfos:         file_counter_proto_enumTypes,
		MessageInfos:      file_counter_proto_msgTypes,
	}.Build()
	File_counter_proto = out.File
	file_counter_proto_rawDesc = nil
	file_counter_proto_goTypes = nil
	file_counter_proto_depIdxs = nil
}
