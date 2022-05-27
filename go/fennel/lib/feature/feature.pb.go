// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.0
// 	protoc        v3.17.3
// source: feature.proto

package feature

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

type ProtoRow struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ContextOType    string        `protobuf:"bytes,1,opt,name=ContextOType,proto3" json:"ContextOType,omitempty"`
	ContextOid      string        `protobuf:"bytes,2,opt,name=ContextOid,proto3" json:"ContextOid,omitempty"`
	CandidateOType  string        `protobuf:"bytes,3,opt,name=CandidateOType,proto3" json:"CandidateOType,omitempty"`
	CandidateOid    string        `protobuf:"bytes,4,opt,name=CandidateOid,proto3" json:"CandidateOid,omitempty"`
	Features        *value.PValue `protobuf:"bytes,5,opt,name=Features,proto3" json:"Features,omitempty"`
	Workflow        string        `protobuf:"bytes,6,opt,name=Workflow,proto3" json:"Workflow,omitempty"`
	RequestID       string        `protobuf:"bytes,7,opt,name=RequestID,proto3" json:"RequestID,omitempty"`
	Timestamp       uint64        `protobuf:"varint,8,opt,name=Timestamp,proto3" json:"Timestamp,omitempty"`
	ModelName       string        `protobuf:"bytes,9,opt,name=ModelName,proto3" json:"ModelName,omitempty"`
	ModelVersion    string        `protobuf:"bytes,10,opt,name=ModelVersion,proto3" json:"ModelVersion,omitempty"`
	ModelPrediction float64       `protobuf:"fixed64,11,opt,name=ModelPrediction,proto3" json:"ModelPrediction,omitempty"`
}

func (x *ProtoRow) Reset() {
	*x = ProtoRow{}
	if protoimpl.UnsafeEnabled {
		mi := &file_feature_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ProtoRow) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ProtoRow) ProtoMessage() {}

func (x *ProtoRow) ProtoReflect() protoreflect.Message {
	mi := &file_feature_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ProtoRow.ProtoReflect.Descriptor instead.
func (*ProtoRow) Descriptor() ([]byte, []int) {
	return file_feature_proto_rawDescGZIP(), []int{0}
}

func (x *ProtoRow) GetContextOType() string {
	if x != nil {
		return x.ContextOType
	}
	return ""
}

func (x *ProtoRow) GetContextOid() string {
	if x != nil {
		return x.ContextOid
	}
	return ""
}

func (x *ProtoRow) GetCandidateOType() string {
	if x != nil {
		return x.CandidateOType
	}
	return ""
}

func (x *ProtoRow) GetCandidateOid() string {
	if x != nil {
		return x.CandidateOid
	}
	return ""
}

func (x *ProtoRow) GetFeatures() *value.PValue {
	if x != nil {
		return x.Features
	}
	return nil
}

func (x *ProtoRow) GetWorkflow() string {
	if x != nil {
		return x.Workflow
	}
	return ""
}

func (x *ProtoRow) GetRequestID() string {
	if x != nil {
		return x.RequestID
	}
	return ""
}

func (x *ProtoRow) GetTimestamp() uint64 {
	if x != nil {
		return x.Timestamp
	}
	return 0
}

func (x *ProtoRow) GetModelName() string {
	if x != nil {
		return x.ModelName
	}
	return ""
}

func (x *ProtoRow) GetModelVersion() string {
	if x != nil {
		return x.ModelVersion
	}
	return ""
}

func (x *ProtoRow) GetModelPrediction() float64 {
	if x != nil {
		return x.ModelPrediction
	}
	return 0
}

var File_feature_proto protoreflect.FileDescriptor

var file_feature_proto_rawDesc = []byte{
	0x0a, 0x0d, 0x66, 0x65, 0x61, 0x74, 0x75, 0x72, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a,
	0x0b, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x83, 0x03, 0x0a,
	0x08, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x52, 0x6f, 0x77, 0x12, 0x22, 0x0a, 0x0c, 0x43, 0x6f, 0x6e,
	0x74, 0x65, 0x78, 0x74, 0x4f, 0x54, 0x79, 0x70, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x0c, 0x43, 0x6f, 0x6e, 0x74, 0x65, 0x78, 0x74, 0x4f, 0x54, 0x79, 0x70, 0x65, 0x12, 0x1e, 0x0a,
	0x0a, 0x43, 0x6f, 0x6e, 0x74, 0x65, 0x78, 0x74, 0x4f, 0x69, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x0a, 0x43, 0x6f, 0x6e, 0x74, 0x65, 0x78, 0x74, 0x4f, 0x69, 0x64, 0x12, 0x26, 0x0a,
	0x0e, 0x43, 0x61, 0x6e, 0x64, 0x69, 0x64, 0x61, 0x74, 0x65, 0x4f, 0x54, 0x79, 0x70, 0x65, 0x18,
	0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0e, 0x43, 0x61, 0x6e, 0x64, 0x69, 0x64, 0x61, 0x74, 0x65,
	0x4f, 0x54, 0x79, 0x70, 0x65, 0x12, 0x22, 0x0a, 0x0c, 0x43, 0x61, 0x6e, 0x64, 0x69, 0x64, 0x61,
	0x74, 0x65, 0x4f, 0x69, 0x64, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0c, 0x43, 0x61, 0x6e,
	0x64, 0x69, 0x64, 0x61, 0x74, 0x65, 0x4f, 0x69, 0x64, 0x12, 0x23, 0x0a, 0x08, 0x46, 0x65, 0x61,
	0x74, 0x75, 0x72, 0x65, 0x73, 0x18, 0x05, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x07, 0x2e, 0x50, 0x56,
	0x61, 0x6c, 0x75, 0x65, 0x52, 0x08, 0x46, 0x65, 0x61, 0x74, 0x75, 0x72, 0x65, 0x73, 0x12, 0x1a,
	0x0a, 0x08, 0x57, 0x6f, 0x72, 0x6b, 0x66, 0x6c, 0x6f, 0x77, 0x18, 0x06, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x08, 0x57, 0x6f, 0x72, 0x6b, 0x66, 0x6c, 0x6f, 0x77, 0x12, 0x1c, 0x0a, 0x09, 0x52, 0x65,
	0x71, 0x75, 0x65, 0x73, 0x74, 0x49, 0x44, 0x18, 0x07, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x52,
	0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x49, 0x44, 0x12, 0x1c, 0x0a, 0x09, 0x54, 0x69, 0x6d, 0x65,
	0x73, 0x74, 0x61, 0x6d, 0x70, 0x18, 0x08, 0x20, 0x01, 0x28, 0x04, 0x52, 0x09, 0x54, 0x69, 0x6d,
	0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x12, 0x1c, 0x0a, 0x09, 0x4d, 0x6f, 0x64, 0x65, 0x6c, 0x4e,
	0x61, 0x6d, 0x65, 0x18, 0x09, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x4d, 0x6f, 0x64, 0x65, 0x6c,
	0x4e, 0x61, 0x6d, 0x65, 0x12, 0x22, 0x0a, 0x0c, 0x4d, 0x6f, 0x64, 0x65, 0x6c, 0x56, 0x65, 0x72,
	0x73, 0x69, 0x6f, 0x6e, 0x18, 0x0a, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0c, 0x4d, 0x6f, 0x64, 0x65,
	0x6c, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x12, 0x28, 0x0a, 0x0f, 0x4d, 0x6f, 0x64, 0x65,
	0x6c, 0x50, 0x72, 0x65, 0x64, 0x69, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x18, 0x0b, 0x20, 0x01, 0x28,
	0x01, 0x52, 0x0f, 0x4d, 0x6f, 0x64, 0x65, 0x6c, 0x50, 0x72, 0x65, 0x64, 0x69, 0x63, 0x74, 0x69,
	0x6f, 0x6e, 0x42, 0x14, 0x5a, 0x12, 0x66, 0x65, 0x6e, 0x6e, 0x65, 0x6c, 0x2f, 0x6c, 0x69, 0x62,
	0x2f, 0x66, 0x65, 0x61, 0x74, 0x75, 0x72, 0x65, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_feature_proto_rawDescOnce sync.Once
	file_feature_proto_rawDescData = file_feature_proto_rawDesc
)

func file_feature_proto_rawDescGZIP() []byte {
	file_feature_proto_rawDescOnce.Do(func() {
		file_feature_proto_rawDescData = protoimpl.X.CompressGZIP(file_feature_proto_rawDescData)
	})
	return file_feature_proto_rawDescData
}

var file_feature_proto_msgTypes = make([]protoimpl.MessageInfo, 1)
var file_feature_proto_goTypes = []interface{}{
	(*ProtoRow)(nil),     // 0: ProtoRow
	(*value.PValue)(nil), // 1: PValue
}
var file_feature_proto_depIdxs = []int32{
	1, // 0: ProtoRow.Features:type_name -> PValue
	1, // [1:1] is the sub-list for method output_type
	1, // [1:1] is the sub-list for method input_type
	1, // [1:1] is the sub-list for extension type_name
	1, // [1:1] is the sub-list for extension extendee
	0, // [0:1] is the sub-list for field type_name
}

func init() { file_feature_proto_init() }
func file_feature_proto_init() {
	if File_feature_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_feature_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ProtoRow); i {
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
			RawDescriptor: file_feature_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   1,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_feature_proto_goTypes,
		DependencyIndexes: file_feature_proto_depIdxs,
		MessageInfos:      file_feature_proto_msgTypes,
	}.Build()
	File_feature_proto = out.File
	file_feature_proto_rawDesc = nil
	file_feature_proto_goTypes = nil
	file_feature_proto_depIdxs = nil
}
