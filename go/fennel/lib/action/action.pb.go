// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.27.1
// 	protoc        v3.19.2
// source: action.proto

package action

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

type ProtoAction struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ActionID   uint64        `protobuf:"varint,1,opt,name=ActionID,json=action_id,proto3" json:"ActionID,omitempty"`
	ActorID    uint64        `protobuf:"varint,2,opt,name=ActorID,json=actor_id,proto3" json:"ActorID,omitempty"`
	ActorType  string        `protobuf:"bytes,3,opt,name=ActorType,json=actor_type,proto3" json:"ActorType,omitempty"`
	TargetID   uint64        `protobuf:"varint,4,opt,name=TargetID,json=target_id,proto3" json:"TargetID,omitempty"`
	TargetType string        `protobuf:"bytes,5,opt,name=TargetType,json=target_type,proto3" json:"TargetType,omitempty"`
	ActionType string        `protobuf:"bytes,6,opt,name=ActionType,json=action_type,proto3" json:"ActionType,omitempty"`
	Timestamp  uint64        `protobuf:"varint,7,opt,name=Timestamp,json=timestamp,proto3" json:"Timestamp,omitempty"`
	RequestID  uint64        `protobuf:"varint,8,opt,name=RequestID,json=request_id,proto3" json:"RequestID,omitempty"`
	Metadata   *value.PValue `protobuf:"bytes,9,opt,name=Metadata,json=metadata,proto3" json:"Metadata,omitempty"`
}

func (x *ProtoAction) Reset() {
	*x = ProtoAction{}
	if protoimpl.UnsafeEnabled {
		mi := &file_action_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ProtoAction) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ProtoAction) ProtoMessage() {}

func (x *ProtoAction) ProtoReflect() protoreflect.Message {
	mi := &file_action_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ProtoAction.ProtoReflect.Descriptor instead.
func (*ProtoAction) Descriptor() ([]byte, []int) {
	return file_action_proto_rawDescGZIP(), []int{0}
}

func (x *ProtoAction) GetActionID() uint64 {
	if x != nil {
		return x.ActionID
	}
	return 0
}

func (x *ProtoAction) GetActorID() uint64 {
	if x != nil {
		return x.ActorID
	}
	return 0
}

func (x *ProtoAction) GetActorType() string {
	if x != nil {
		return x.ActorType
	}
	return ""
}

func (x *ProtoAction) GetTargetID() uint64 {
	if x != nil {
		return x.TargetID
	}
	return 0
}

func (x *ProtoAction) GetTargetType() string {
	if x != nil {
		return x.TargetType
	}
	return ""
}

func (x *ProtoAction) GetActionType() string {
	if x != nil {
		return x.ActionType
	}
	return ""
}

func (x *ProtoAction) GetTimestamp() uint64 {
	if x != nil {
		return x.Timestamp
	}
	return 0
}

func (x *ProtoAction) GetRequestID() uint64 {
	if x != nil {
		return x.RequestID
	}
	return 0
}

func (x *ProtoAction) GetMetadata() *value.PValue {
	if x != nil {
		return x.Metadata
	}
	return nil
}

type ProtoActionFetchRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	MinActionID  uint64 `protobuf:"varint,1,opt,name=MinActionID,json=min_action_id,proto3" json:"MinActionID,omitempty"`
	MaxActionID  uint64 `protobuf:"varint,2,opt,name=MaxActionID,json=max_action_id,proto3" json:"MaxActionID,omitempty"`
	ActorID      uint64 `protobuf:"varint,3,opt,name=ActorID,json=actor_id,proto3" json:"ActorID,omitempty"`
	ActorType    string `protobuf:"bytes,4,opt,name=ActorType,json=actor_type,proto3" json:"ActorType,omitempty"`
	TargetID     uint64 `protobuf:"varint,5,opt,name=TargetID,json=target_id,proto3" json:"TargetID,omitempty"`
	TargetType   string `protobuf:"bytes,6,opt,name=TargetType,json=target_type,proto3" json:"TargetType,omitempty"`
	ActionType   string `protobuf:"bytes,7,opt,name=ActionType,json=action_type,proto3" json:"ActionType,omitempty"`
	MinTimestamp uint64 `protobuf:"varint,8,opt,name=MinTimestamp,json=min_timestamp,proto3" json:"MinTimestamp,omitempty"`
	MaxTimestamp uint64 `protobuf:"varint,9,opt,name=MaxTimestamp,json=max_timestamp,proto3" json:"MaxTimestamp,omitempty"`
	RequestID    uint64 `protobuf:"varint,10,opt,name=RequestID,json=request_id,proto3" json:"RequestID,omitempty"`
}

func (x *ProtoActionFetchRequest) Reset() {
	*x = ProtoActionFetchRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_action_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ProtoActionFetchRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ProtoActionFetchRequest) ProtoMessage() {}

func (x *ProtoActionFetchRequest) ProtoReflect() protoreflect.Message {
	mi := &file_action_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ProtoActionFetchRequest.ProtoReflect.Descriptor instead.
func (*ProtoActionFetchRequest) Descriptor() ([]byte, []int) {
	return file_action_proto_rawDescGZIP(), []int{1}
}

func (x *ProtoActionFetchRequest) GetMinActionID() uint64 {
	if x != nil {
		return x.MinActionID
	}
	return 0
}

func (x *ProtoActionFetchRequest) GetMaxActionID() uint64 {
	if x != nil {
		return x.MaxActionID
	}
	return 0
}

func (x *ProtoActionFetchRequest) GetActorID() uint64 {
	if x != nil {
		return x.ActorID
	}
	return 0
}

func (x *ProtoActionFetchRequest) GetActorType() string {
	if x != nil {
		return x.ActorType
	}
	return ""
}

func (x *ProtoActionFetchRequest) GetTargetID() uint64 {
	if x != nil {
		return x.TargetID
	}
	return 0
}

func (x *ProtoActionFetchRequest) GetTargetType() string {
	if x != nil {
		return x.TargetType
	}
	return ""
}

func (x *ProtoActionFetchRequest) GetActionType() string {
	if x != nil {
		return x.ActionType
	}
	return ""
}

func (x *ProtoActionFetchRequest) GetMinTimestamp() uint64 {
	if x != nil {
		return x.MinTimestamp
	}
	return 0
}

func (x *ProtoActionFetchRequest) GetMaxTimestamp() uint64 {
	if x != nil {
		return x.MaxTimestamp
	}
	return 0
}

func (x *ProtoActionFetchRequest) GetRequestID() uint64 {
	if x != nil {
		return x.RequestID
	}
	return 0
}

type ProtoActionList struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Actions []*ProtoAction `protobuf:"bytes,1,rep,name=actions,proto3" json:"actions,omitempty"`
}

func (x *ProtoActionList) Reset() {
	*x = ProtoActionList{}
	if protoimpl.UnsafeEnabled {
		mi := &file_action_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ProtoActionList) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ProtoActionList) ProtoMessage() {}

func (x *ProtoActionList) ProtoReflect() protoreflect.Message {
	mi := &file_action_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ProtoActionList.ProtoReflect.Descriptor instead.
func (*ProtoActionList) Descriptor() ([]byte, []int) {
	return file_action_proto_rawDescGZIP(), []int{2}
}

func (x *ProtoActionList) GetActions() []*ProtoAction {
	if x != nil {
		return x.Actions
	}
	return nil
}

var File_action_proto protoreflect.FileDescriptor

var file_action_proto_rawDesc = []byte{
	0x0a, 0x0c, 0x61, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x0b,
	0x76, 0x61, 0x6c, 0x75, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0xa5, 0x02, 0x0a, 0x0b,
	0x50, 0x72, 0x6f, 0x74, 0x6f, 0x41, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x1b, 0x0a, 0x08, 0x41,
	0x63, 0x74, 0x69, 0x6f, 0x6e, 0x49, 0x44, 0x18, 0x01, 0x20, 0x01, 0x28, 0x04, 0x52, 0x09, 0x61,
	0x63, 0x74, 0x69, 0x6f, 0x6e, 0x5f, 0x69, 0x64, 0x12, 0x19, 0x0a, 0x07, 0x41, 0x63, 0x74, 0x6f,
	0x72, 0x49, 0x44, 0x18, 0x02, 0x20, 0x01, 0x28, 0x04, 0x52, 0x08, 0x61, 0x63, 0x74, 0x6f, 0x72,
	0x5f, 0x69, 0x64, 0x12, 0x1d, 0x0a, 0x09, 0x41, 0x63, 0x74, 0x6f, 0x72, 0x54, 0x79, 0x70, 0x65,
	0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0a, 0x61, 0x63, 0x74, 0x6f, 0x72, 0x5f, 0x74, 0x79,
	0x70, 0x65, 0x12, 0x1b, 0x0a, 0x08, 0x54, 0x61, 0x72, 0x67, 0x65, 0x74, 0x49, 0x44, 0x18, 0x04,
	0x20, 0x01, 0x28, 0x04, 0x52, 0x09, 0x74, 0x61, 0x72, 0x67, 0x65, 0x74, 0x5f, 0x69, 0x64, 0x12,
	0x1f, 0x0a, 0x0a, 0x54, 0x61, 0x72, 0x67, 0x65, 0x74, 0x54, 0x79, 0x70, 0x65, 0x18, 0x05, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x0b, 0x74, 0x61, 0x72, 0x67, 0x65, 0x74, 0x5f, 0x74, 0x79, 0x70, 0x65,
	0x12, 0x1f, 0x0a, 0x0a, 0x41, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x54, 0x79, 0x70, 0x65, 0x18, 0x06,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x0b, 0x61, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x5f, 0x74, 0x79, 0x70,
	0x65, 0x12, 0x1c, 0x0a, 0x09, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x18, 0x07,
	0x20, 0x01, 0x28, 0x04, 0x52, 0x09, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x12,
	0x1d, 0x0a, 0x09, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x49, 0x44, 0x18, 0x08, 0x20, 0x01,
	0x28, 0x04, 0x52, 0x0a, 0x72, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x5f, 0x69, 0x64, 0x12, 0x23,
	0x0a, 0x08, 0x4d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x18, 0x09, 0x20, 0x01, 0x28, 0x0b,
	0x32, 0x07, 0x2e, 0x50, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x52, 0x08, 0x6d, 0x65, 0x74, 0x61, 0x64,
	0x61, 0x74, 0x61, 0x22, 0xe3, 0x02, 0x0a, 0x17, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x41, 0x63, 0x74,
	0x69, 0x6f, 0x6e, 0x46, 0x65, 0x74, 0x63, 0x68, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12,
	0x22, 0x0a, 0x0b, 0x4d, 0x69, 0x6e, 0x41, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x49, 0x44, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x04, 0x52, 0x0d, 0x6d, 0x69, 0x6e, 0x5f, 0x61, 0x63, 0x74, 0x69, 0x6f, 0x6e,
	0x5f, 0x69, 0x64, 0x12, 0x22, 0x0a, 0x0b, 0x4d, 0x61, 0x78, 0x41, 0x63, 0x74, 0x69, 0x6f, 0x6e,
	0x49, 0x44, 0x18, 0x02, 0x20, 0x01, 0x28, 0x04, 0x52, 0x0d, 0x6d, 0x61, 0x78, 0x5f, 0x61, 0x63,
	0x74, 0x69, 0x6f, 0x6e, 0x5f, 0x69, 0x64, 0x12, 0x19, 0x0a, 0x07, 0x41, 0x63, 0x74, 0x6f, 0x72,
	0x49, 0x44, 0x18, 0x03, 0x20, 0x01, 0x28, 0x04, 0x52, 0x08, 0x61, 0x63, 0x74, 0x6f, 0x72, 0x5f,
	0x69, 0x64, 0x12, 0x1d, 0x0a, 0x09, 0x41, 0x63, 0x74, 0x6f, 0x72, 0x54, 0x79, 0x70, 0x65, 0x18,
	0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0a, 0x61, 0x63, 0x74, 0x6f, 0x72, 0x5f, 0x74, 0x79, 0x70,
	0x65, 0x12, 0x1b, 0x0a, 0x08, 0x54, 0x61, 0x72, 0x67, 0x65, 0x74, 0x49, 0x44, 0x18, 0x05, 0x20,
	0x01, 0x28, 0x04, 0x52, 0x09, 0x74, 0x61, 0x72, 0x67, 0x65, 0x74, 0x5f, 0x69, 0x64, 0x12, 0x1f,
	0x0a, 0x0a, 0x54, 0x61, 0x72, 0x67, 0x65, 0x74, 0x54, 0x79, 0x70, 0x65, 0x18, 0x06, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x0b, 0x74, 0x61, 0x72, 0x67, 0x65, 0x74, 0x5f, 0x74, 0x79, 0x70, 0x65, 0x12,
	0x1f, 0x0a, 0x0a, 0x41, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x54, 0x79, 0x70, 0x65, 0x18, 0x07, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x0b, 0x61, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x5f, 0x74, 0x79, 0x70, 0x65,
	0x12, 0x23, 0x0a, 0x0c, 0x4d, 0x69, 0x6e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70,
	0x18, 0x08, 0x20, 0x01, 0x28, 0x04, 0x52, 0x0d, 0x6d, 0x69, 0x6e, 0x5f, 0x74, 0x69, 0x6d, 0x65,
	0x73, 0x74, 0x61, 0x6d, 0x70, 0x12, 0x23, 0x0a, 0x0c, 0x4d, 0x61, 0x78, 0x54, 0x69, 0x6d, 0x65,
	0x73, 0x74, 0x61, 0x6d, 0x70, 0x18, 0x09, 0x20, 0x01, 0x28, 0x04, 0x52, 0x0d, 0x6d, 0x61, 0x78,
	0x5f, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x12, 0x1d, 0x0a, 0x09, 0x52, 0x65,
	0x71, 0x75, 0x65, 0x73, 0x74, 0x49, 0x44, 0x18, 0x0a, 0x20, 0x01, 0x28, 0x04, 0x52, 0x0a, 0x72,
	0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x5f, 0x69, 0x64, 0x22, 0x39, 0x0a, 0x0f, 0x50, 0x72, 0x6f,
	0x74, 0x6f, 0x41, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x4c, 0x69, 0x73, 0x74, 0x12, 0x26, 0x0a, 0x07,
	0x61, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x0c, 0x2e,
	0x50, 0x72, 0x6f, 0x74, 0x6f, 0x41, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x07, 0x61, 0x63, 0x74,
	0x69, 0x6f, 0x6e, 0x73, 0x42, 0x13, 0x5a, 0x11, 0x66, 0x65, 0x6e, 0x6e, 0x65, 0x6c, 0x2f, 0x6c,
	0x69, 0x62, 0x2f, 0x61, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x33,
}

var (
	file_action_proto_rawDescOnce sync.Once
	file_action_proto_rawDescData = file_action_proto_rawDesc
)

func file_action_proto_rawDescGZIP() []byte {
	file_action_proto_rawDescOnce.Do(func() {
		file_action_proto_rawDescData = protoimpl.X.CompressGZIP(file_action_proto_rawDescData)
	})
	return file_action_proto_rawDescData
}

var file_action_proto_msgTypes = make([]protoimpl.MessageInfo, 3)
var file_action_proto_goTypes = []interface{}{
	(*ProtoAction)(nil),             // 0: ProtoAction
	(*ProtoActionFetchRequest)(nil), // 1: ProtoActionFetchRequest
	(*ProtoActionList)(nil),         // 2: ProtoActionList
	(*value.PValue)(nil),            // 3: PValue
}
var file_action_proto_depIdxs = []int32{
	3, // 0: ProtoAction.Metadata:type_name -> PValue
	0, // 1: ProtoActionList.actions:type_name -> ProtoAction
	2, // [2:2] is the sub-list for method output_type
	2, // [2:2] is the sub-list for method input_type
	2, // [2:2] is the sub-list for extension type_name
	2, // [2:2] is the sub-list for extension extendee
	0, // [0:2] is the sub-list for field type_name
}

func init() { file_action_proto_init() }
func file_action_proto_init() {
	if File_action_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_action_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ProtoAction); i {
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
		file_action_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ProtoActionFetchRequest); i {
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
		file_action_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ProtoActionList); i {
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
			RawDescriptor: file_action_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   3,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_action_proto_goTypes,
		DependencyIndexes: file_action_proto_depIdxs,
		MessageInfos:      file_action_proto_msgTypes,
	}.Build()
	File_action_proto = out.File
	file_action_proto_rawDesc = nil
	file_action_proto_goTypes = nil
	file_action_proto_depIdxs = nil
}
