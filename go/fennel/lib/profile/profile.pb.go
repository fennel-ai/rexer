// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.0
// 	protoc        v3.17.3
// source: profile.proto

package profile

import (
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

type ProtoProfileItem struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	OType   string        `protobuf:"bytes,1,opt,name=OType,proto3" json:"OType,omitempty"`
	Oid     uint64        `protobuf:"varint,2,opt,name=Oid,proto3" json:"Oid,omitempty"`
	Key     string        `protobuf:"bytes,3,opt,name=Key,proto3" json:"Key,omitempty"`
	Version uint64        `protobuf:"varint,4,opt,name=Version,proto3" json:"Version,omitempty"`
	Value   *value.PValue `protobuf:"bytes,5,opt,name=Value,proto3" json:"Value,omitempty"`
}

func (x *ProtoProfileItem) Reset() {
	*x = ProtoProfileItem{}
	if protoimpl.UnsafeEnabled {
		mi := &file_profile_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ProtoProfileItem) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ProtoProfileItem) ProtoMessage() {}

func (x *ProtoProfileItem) ProtoReflect() protoreflect.Message {
	mi := &file_profile_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ProtoProfileItem.ProtoReflect.Descriptor instead.
func (*ProtoProfileItem) Descriptor() ([]byte, []int) {
	return file_profile_proto_rawDescGZIP(), []int{0}
}

func (x *ProtoProfileItem) GetOType() string {
	if x != nil {
		return x.OType
	}
	return ""
}

func (x *ProtoProfileItem) GetOid() uint64 {
	if x != nil {
		return x.Oid
	}
	return 0
}

func (x *ProtoProfileItem) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

func (x *ProtoProfileItem) GetVersion() uint64 {
	if x != nil {
		return x.Version
	}
	return 0
}

func (x *ProtoProfileItem) GetValue() *value.PValue {
	if x != nil {
		return x.Value
	}
	return nil
}

type ProtoProfileFetchRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	OType   string `protobuf:"bytes,1,opt,name=OType,proto3" json:"OType,omitempty"`
	Oid     uint64 `protobuf:"varint,2,opt,name=Oid,proto3" json:"Oid,omitempty"`
	Key     string `protobuf:"bytes,3,opt,name=Key,proto3" json:"Key,omitempty"`
	Version uint64 `protobuf:"varint,4,opt,name=Version,proto3" json:"Version,omitempty"`
}

func (x *ProtoProfileFetchRequest) Reset() {
	*x = ProtoProfileFetchRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_profile_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ProtoProfileFetchRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ProtoProfileFetchRequest) ProtoMessage() {}

func (x *ProtoProfileFetchRequest) ProtoReflect() protoreflect.Message {
	mi := &file_profile_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ProtoProfileFetchRequest.ProtoReflect.Descriptor instead.
func (*ProtoProfileFetchRequest) Descriptor() ([]byte, []int) {
	return file_profile_proto_rawDescGZIP(), []int{1}
}

func (x *ProtoProfileFetchRequest) GetOType() string {
	if x != nil {
		return x.OType
	}
	return ""
}

func (x *ProtoProfileFetchRequest) GetOid() uint64 {
	if x != nil {
		return x.Oid
	}
	return 0
}

func (x *ProtoProfileFetchRequest) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

func (x *ProtoProfileFetchRequest) GetVersion() uint64 {
	if x != nil {
		return x.Version
	}
	return 0
}

type ProtoProfileList struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Profiles []*ProtoProfileItem `protobuf:"bytes,1,rep,name=profiles,proto3" json:"profiles,omitempty"`
}

func (x *ProtoProfileList) Reset() {
	*x = ProtoProfileList{}
	if protoimpl.UnsafeEnabled {
		mi := &file_profile_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ProtoProfileList) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ProtoProfileList) ProtoMessage() {}

func (x *ProtoProfileList) ProtoReflect() protoreflect.Message {
	mi := &file_profile_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ProtoProfileList.ProtoReflect.Descriptor instead.
func (*ProtoProfileList) Descriptor() ([]byte, []int) {
	return file_profile_proto_rawDescGZIP(), []int{2}
}

func (x *ProtoProfileList) GetProfiles() []*ProtoProfileItem {
	if x != nil {
		return x.Profiles
	}
	return nil
}

var File_profile_proto protoreflect.FileDescriptor

var file_profile_proto_rawDesc = []byte{
	0x0a, 0x0d, 0x70, 0x72, 0x6f, 0x66, 0x69, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a,
	0x0b, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x85, 0x01, 0x0a,
	0x10, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x50, 0x72, 0x6f, 0x66, 0x69, 0x6c, 0x65, 0x49, 0x74, 0x65,
	0x6d, 0x12, 0x14, 0x0a, 0x05, 0x4f, 0x54, 0x79, 0x70, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x05, 0x4f, 0x54, 0x79, 0x70, 0x65, 0x12, 0x10, 0x0a, 0x03, 0x4f, 0x69, 0x64, 0x18, 0x02,
	0x20, 0x01, 0x28, 0x04, 0x52, 0x03, 0x4f, 0x69, 0x64, 0x12, 0x10, 0x0a, 0x03, 0x4b, 0x65, 0x79,
	0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x4b, 0x65, 0x79, 0x12, 0x18, 0x0a, 0x07, 0x56,
	0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x18, 0x04, 0x20, 0x01, 0x28, 0x04, 0x52, 0x07, 0x56, 0x65,
	0x72, 0x73, 0x69, 0x6f, 0x6e, 0x12, 0x1d, 0x0a, 0x05, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x05,
	0x20, 0x01, 0x28, 0x0b, 0x32, 0x07, 0x2e, 0x50, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x52, 0x05, 0x56,
	0x61, 0x6c, 0x75, 0x65, 0x22, 0x6e, 0x0a, 0x18, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x50, 0x72, 0x6f,
	0x66, 0x69, 0x6c, 0x65, 0x46, 0x65, 0x74, 0x63, 0x68, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74,
	0x12, 0x14, 0x0a, 0x05, 0x4f, 0x54, 0x79, 0x70, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x05, 0x4f, 0x54, 0x79, 0x70, 0x65, 0x12, 0x10, 0x0a, 0x03, 0x4f, 0x69, 0x64, 0x18, 0x02, 0x20,
	0x01, 0x28, 0x04, 0x52, 0x03, 0x4f, 0x69, 0x64, 0x12, 0x10, 0x0a, 0x03, 0x4b, 0x65, 0x79, 0x18,
	0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x4b, 0x65, 0x79, 0x12, 0x18, 0x0a, 0x07, 0x56, 0x65,
	0x72, 0x73, 0x69, 0x6f, 0x6e, 0x18, 0x04, 0x20, 0x01, 0x28, 0x04, 0x52, 0x07, 0x56, 0x65, 0x72,
	0x73, 0x69, 0x6f, 0x6e, 0x22, 0x41, 0x0a, 0x10, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x50, 0x72, 0x6f,
	0x66, 0x69, 0x6c, 0x65, 0x4c, 0x69, 0x73, 0x74, 0x12, 0x2d, 0x0a, 0x08, 0x70, 0x72, 0x6f, 0x66,
	0x69, 0x6c, 0x65, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x11, 0x2e, 0x50, 0x72, 0x6f,
	0x74, 0x6f, 0x50, 0x72, 0x6f, 0x66, 0x69, 0x6c, 0x65, 0x49, 0x74, 0x65, 0x6d, 0x52, 0x08, 0x70,
	0x72, 0x6f, 0x66, 0x69, 0x6c, 0x65, 0x73, 0x42, 0x14, 0x5a, 0x12, 0x66, 0x65, 0x6e, 0x6e, 0x65,
	0x6c, 0x2f, 0x6c, 0x69, 0x62, 0x2f, 0x70, 0x72, 0x6f, 0x66, 0x69, 0x6c, 0x65, 0x62, 0x06, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_profile_proto_rawDescOnce sync.Once
	file_profile_proto_rawDescData = file_profile_proto_rawDesc
)

func file_profile_proto_rawDescGZIP() []byte {
	file_profile_proto_rawDescOnce.Do(func() {
		file_profile_proto_rawDescData = protoimpl.X.CompressGZIP(file_profile_proto_rawDescData)
	})
	return file_profile_proto_rawDescData
}

var file_profile_proto_msgTypes = make([]protoimpl.MessageInfo, 3)
var file_profile_proto_goTypes = []interface{}{
	(*ProtoProfileItem)(nil),         // 0: ProtoProfileItem
	(*ProtoProfileFetchRequest)(nil), // 1: ProtoProfileFetchRequest
	(*ProtoProfileList)(nil),         // 2: ProtoProfileList
	(*value.PValue)(nil),             // 3: PValue
}
var file_profile_proto_depIdxs = []int32{
	3, // 0: ProtoProfileItem.Value:type_name -> PValue
	0, // 1: ProtoProfileList.profiles:type_name -> ProtoProfileItem
	2, // [2:2] is the sub-list for method output_type
	2, // [2:2] is the sub-list for method input_type
	2, // [2:2] is the sub-list for extension type_name
	2, // [2:2] is the sub-list for extension extendee
	0, // [0:2] is the sub-list for field type_name
}

func init() { file_profile_proto_init() }
func file_profile_proto_init() {
	if File_profile_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_profile_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ProtoProfileItem); i {
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
		file_profile_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ProtoProfileFetchRequest); i {
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
		file_profile_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ProtoProfileList); i {
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
			RawDescriptor: file_profile_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   3,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_profile_proto_goTypes,
		DependencyIndexes: file_profile_proto_depIdxs,
		MessageInfos:      file_profile_proto_msgTypes,
	}.Build()
	File_profile_proto = out.File
	file_profile_proto_rawDesc = nil
	file_profile_proto_goTypes = nil
	file_profile_proto_depIdxs = nil
}
