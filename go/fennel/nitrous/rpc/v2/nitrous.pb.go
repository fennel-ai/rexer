// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.0
// 	protoc        v3.21.1
// source: nitrous.proto

package v2

import (
	aggregate "fennel/lib/aggregate"
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

type OpType int32

const (
	OpType_AGG_EVENT        OpType = 0
	OpType_PROFILE_UPDATE   OpType = 1
	OpType_CREATE_AGGREGATE OpType = 2
	OpType_DELETE_AGGREGATE OpType = 3
)

// Enum value maps for OpType.
var (
	OpType_name = map[int32]string{
		0: "AGG_EVENT",
		1: "PROFILE_UPDATE",
		2: "CREATE_AGGREGATE",
		3: "DELETE_AGGREGATE",
	}
	OpType_value = map[string]int32{
		"AGG_EVENT":        0,
		"PROFILE_UPDATE":   1,
		"CREATE_AGGREGATE": 2,
		"DELETE_AGGREGATE": 3,
	}
)

func (x OpType) Enum() *OpType {
	p := new(OpType)
	*p = x
	return p
}

func (x OpType) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (OpType) Descriptor() protoreflect.EnumDescriptor {
	return file_nitrous_proto_enumTypes[0].Descriptor()
}

func (OpType) Type() protoreflect.EnumType {
	return &file_nitrous_proto_enumTypes[0]
}

func (x OpType) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use OpType.Descriptor instead.
func (OpType) EnumDescriptor() ([]byte, []int) {
	return file_nitrous_proto_rawDescGZIP(), []int{0}
}

type NitrousOp struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	TierId uint32 `protobuf:"varint,1,opt,name=tier_id,json=tierId,proto3" json:"tier_id,omitempty"`
	Type   OpType `protobuf:"varint,2,opt,name=type,proto3,enum=nitrous.OpType" json:"type,omitempty"`
	// Types that are assignable to Op:
	//	*NitrousOp_CreateAggregate
	//	*NitrousOp_DeleteAggregate
	//	*NitrousOp_AggEvent
	//	*NitrousOp_Profile
	Op isNitrousOp_Op `protobuf_oneof:"op"`
}

func (x *NitrousOp) Reset() {
	*x = NitrousOp{}
	if protoimpl.UnsafeEnabled {
		mi := &file_nitrous_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *NitrousOp) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*NitrousOp) ProtoMessage() {}

func (x *NitrousOp) ProtoReflect() protoreflect.Message {
	mi := &file_nitrous_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use NitrousOp.ProtoReflect.Descriptor instead.
func (*NitrousOp) Descriptor() ([]byte, []int) {
	return file_nitrous_proto_rawDescGZIP(), []int{0}
}

func (x *NitrousOp) GetTierId() uint32 {
	if x != nil {
		return x.TierId
	}
	return 0
}

func (x *NitrousOp) GetType() OpType {
	if x != nil {
		return x.Type
	}
	return OpType_AGG_EVENT
}

func (m *NitrousOp) GetOp() isNitrousOp_Op {
	if m != nil {
		return m.Op
	}
	return nil
}

func (x *NitrousOp) GetCreateAggregate() *CreateAggregate {
	if x, ok := x.GetOp().(*NitrousOp_CreateAggregate); ok {
		return x.CreateAggregate
	}
	return nil
}

func (x *NitrousOp) GetDeleteAggregate() *DeleteAggregate {
	if x, ok := x.GetOp().(*NitrousOp_DeleteAggregate); ok {
		return x.DeleteAggregate
	}
	return nil
}

func (x *NitrousOp) GetAggEvent() *AggEvent {
	if x, ok := x.GetOp().(*NitrousOp_AggEvent); ok {
		return x.AggEvent
	}
	return nil
}

func (x *NitrousOp) GetProfile() *ProfileUpdate {
	if x, ok := x.GetOp().(*NitrousOp_Profile); ok {
		return x.Profile
	}
	return nil
}

type isNitrousOp_Op interface {
	isNitrousOp_Op()
}

type NitrousOp_CreateAggregate struct {
	// Define a new aggregate in nitrous.
	CreateAggregate *CreateAggregate `protobuf:"bytes,3,opt,name=create_aggregate,json=createAggregate,proto3,oneof"`
}

type NitrousOp_DeleteAggregate struct {
	// Delete an aggregate from nitrous.
	DeleteAggregate *DeleteAggregate `protobuf:"bytes,4,opt,name=delete_aggregate,json=deleteAggregate,proto3,oneof"`
}

type NitrousOp_AggEvent struct {
	// Log an aggregate event.
	AggEvent *AggEvent `protobuf:"bytes,5,opt,name=agg_event,json=aggEvent,proto3,oneof"`
}

type NitrousOp_Profile struct {
	// Log a profile update.
	Profile *ProfileUpdate `protobuf:"bytes,6,opt,name=profile,proto3,oneof"`
}

func (*NitrousOp_CreateAggregate) isNitrousOp_Op() {}

func (*NitrousOp_DeleteAggregate) isNitrousOp_Op() {}

func (*NitrousOp_AggEvent) isNitrousOp_Op() {}

func (*NitrousOp_Profile) isNitrousOp_Op() {}

type CreateAggregate struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	AggId   uint32                `protobuf:"varint,1,opt,name=agg_id,json=aggId,proto3" json:"agg_id,omitempty"`
	Options *aggregate.AggOptions `protobuf:"bytes,2,opt,name=options,proto3" json:"options,omitempty"`
}

func (x *CreateAggregate) Reset() {
	*x = CreateAggregate{}
	if protoimpl.UnsafeEnabled {
		mi := &file_nitrous_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *CreateAggregate) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CreateAggregate) ProtoMessage() {}

func (x *CreateAggregate) ProtoReflect() protoreflect.Message {
	mi := &file_nitrous_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CreateAggregate.ProtoReflect.Descriptor instead.
func (*CreateAggregate) Descriptor() ([]byte, []int) {
	return file_nitrous_proto_rawDescGZIP(), []int{1}
}

func (x *CreateAggregate) GetAggId() uint32 {
	if x != nil {
		return x.AggId
	}
	return 0
}

func (x *CreateAggregate) GetOptions() *aggregate.AggOptions {
	if x != nil {
		return x.Options
	}
	return nil
}

type DeleteAggregate struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	AggId uint32 `protobuf:"varint,1,opt,name=agg_id,json=aggId,proto3" json:"agg_id,omitempty"`
}

func (x *DeleteAggregate) Reset() {
	*x = DeleteAggregate{}
	if protoimpl.UnsafeEnabled {
		mi := &file_nitrous_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *DeleteAggregate) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DeleteAggregate) ProtoMessage() {}

func (x *DeleteAggregate) ProtoReflect() protoreflect.Message {
	mi := &file_nitrous_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DeleteAggregate.ProtoReflect.Descriptor instead.
func (*DeleteAggregate) Descriptor() ([]byte, []int) {
	return file_nitrous_proto_rawDescGZIP(), []int{2}
}

func (x *DeleteAggregate) GetAggId() uint32 {
	if x != nil {
		return x.AggId
	}
	return 0
}

type AggEvent struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	AggId     uint32        `protobuf:"varint,1,opt,name=agg_id,json=aggId,proto3" json:"agg_id,omitempty"`
	Groupkey  string        `protobuf:"bytes,2,opt,name=groupkey,proto3" json:"groupkey,omitempty"`
	Timestamp uint32        `protobuf:"varint,3,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
	Value     *value.PValue `protobuf:"bytes,4,opt,name=value,proto3" json:"value,omitempty"`
}

func (x *AggEvent) Reset() {
	*x = AggEvent{}
	if protoimpl.UnsafeEnabled {
		mi := &file_nitrous_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *AggEvent) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AggEvent) ProtoMessage() {}

func (x *AggEvent) ProtoReflect() protoreflect.Message {
	mi := &file_nitrous_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use AggEvent.ProtoReflect.Descriptor instead.
func (*AggEvent) Descriptor() ([]byte, []int) {
	return file_nitrous_proto_rawDescGZIP(), []int{3}
}

func (x *AggEvent) GetAggId() uint32 {
	if x != nil {
		return x.AggId
	}
	return 0
}

func (x *AggEvent) GetGroupkey() string {
	if x != nil {
		return x.Groupkey
	}
	return ""
}

func (x *AggEvent) GetTimestamp() uint32 {
	if x != nil {
		return x.Timestamp
	}
	return 0
}

func (x *AggEvent) GetValue() *value.PValue {
	if x != nil {
		return x.Value
	}
	return nil
}

type ProfileKey struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Otype string `protobuf:"bytes,1,opt,name=otype,proto3" json:"otype,omitempty"`
	Oid   string `protobuf:"bytes,2,opt,name=oid,proto3" json:"oid,omitempty"`
	Zkey  string `protobuf:"bytes,3,opt,name=zkey,proto3" json:"zkey,omitempty"`
}

func (x *ProfileKey) Reset() {
	*x = ProfileKey{}
	if protoimpl.UnsafeEnabled {
		mi := &file_nitrous_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ProfileKey) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ProfileKey) ProtoMessage() {}

func (x *ProfileKey) ProtoReflect() protoreflect.Message {
	mi := &file_nitrous_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ProfileKey.ProtoReflect.Descriptor instead.
func (*ProfileKey) Descriptor() ([]byte, []int) {
	return file_nitrous_proto_rawDescGZIP(), []int{4}
}

func (x *ProfileKey) GetOtype() string {
	if x != nil {
		return x.Otype
	}
	return ""
}

func (x *ProfileKey) GetOid() string {
	if x != nil {
		return x.Oid
	}
	return ""
}

func (x *ProfileKey) GetZkey() string {
	if x != nil {
		return x.Zkey
	}
	return ""
}

type ProfileUpdate struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Key       *ProfileKey   `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	Value     *value.PValue `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
	Timestamp uint32        `protobuf:"varint,3,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
}

func (x *ProfileUpdate) Reset() {
	*x = ProfileUpdate{}
	if protoimpl.UnsafeEnabled {
		mi := &file_nitrous_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ProfileUpdate) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ProfileUpdate) ProtoMessage() {}

func (x *ProfileUpdate) ProtoReflect() protoreflect.Message {
	mi := &file_nitrous_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ProfileUpdate.ProtoReflect.Descriptor instead.
func (*ProfileUpdate) Descriptor() ([]byte, []int) {
	return file_nitrous_proto_rawDescGZIP(), []int{5}
}

func (x *ProfileUpdate) GetKey() *ProfileKey {
	if x != nil {
		return x.Key
	}
	return nil
}

func (x *ProfileUpdate) GetValue() *value.PValue {
	if x != nil {
		return x.Value
	}
	return nil
}

func (x *ProfileUpdate) GetTimestamp() uint32 {
	if x != nil {
		return x.Timestamp
	}
	return 0
}

type LagRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *LagRequest) Reset() {
	*x = LagRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_nitrous_proto_msgTypes[6]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *LagRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*LagRequest) ProtoMessage() {}

func (x *LagRequest) ProtoReflect() protoreflect.Message {
	mi := &file_nitrous_proto_msgTypes[6]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use LagRequest.ProtoReflect.Descriptor instead.
func (*LagRequest) Descriptor() ([]byte, []int) {
	return file_nitrous_proto_rawDescGZIP(), []int{6}
}

type LagResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Lag uint64 `protobuf:"varint,1,opt,name=lag,proto3" json:"lag,omitempty"`
}

func (x *LagResponse) Reset() {
	*x = LagResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_nitrous_proto_msgTypes[7]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *LagResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*LagResponse) ProtoMessage() {}

func (x *LagResponse) ProtoReflect() protoreflect.Message {
	mi := &file_nitrous_proto_msgTypes[7]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use LagResponse.ProtoReflect.Descriptor instead.
func (*LagResponse) Descriptor() ([]byte, []int) {
	return file_nitrous_proto_rawDescGZIP(), []int{7}
}

func (x *LagResponse) GetLag() uint64 {
	if x != nil {
		return x.Lag
	}
	return 0
}

type AggregateValuesRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	TierId   uint32   `protobuf:"varint,1,opt,name=tier_id,json=tierId,proto3" json:"tier_id,omitempty"`
	AggId    uint32   `protobuf:"varint,2,opt,name=agg_id,json=aggId,proto3" json:"agg_id,omitempty"`
	Codec    uint32   `protobuf:"varint,3,opt,name=codec,proto3" json:"codec,omitempty"`
	Duration uint32   `protobuf:"varint,4,opt,name=duration,proto3" json:"duration,omitempty"`
	Groupkey []string `protobuf:"bytes,5,rep,name=groupkey,proto3" json:"groupkey,omitempty"`
}

func (x *AggregateValuesRequest) Reset() {
	*x = AggregateValuesRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_nitrous_proto_msgTypes[8]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *AggregateValuesRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AggregateValuesRequest) ProtoMessage() {}

func (x *AggregateValuesRequest) ProtoReflect() protoreflect.Message {
	mi := &file_nitrous_proto_msgTypes[8]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use AggregateValuesRequest.ProtoReflect.Descriptor instead.
func (*AggregateValuesRequest) Descriptor() ([]byte, []int) {
	return file_nitrous_proto_rawDescGZIP(), []int{8}
}

func (x *AggregateValuesRequest) GetTierId() uint32 {
	if x != nil {
		return x.TierId
	}
	return 0
}

func (x *AggregateValuesRequest) GetAggId() uint32 {
	if x != nil {
		return x.AggId
	}
	return 0
}

func (x *AggregateValuesRequest) GetCodec() uint32 {
	if x != nil {
		return x.Codec
	}
	return 0
}

func (x *AggregateValuesRequest) GetDuration() uint32 {
	if x != nil {
		return x.Duration
	}
	return 0
}

func (x *AggregateValuesRequest) GetGroupkey() []string {
	if x != nil {
		return x.Groupkey
	}
	return nil
}

type AggregateValuesResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Results []*value.PValue `protobuf:"bytes,1,rep,name=results,proto3" json:"results,omitempty"`
}

func (x *AggregateValuesResponse) Reset() {
	*x = AggregateValuesResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_nitrous_proto_msgTypes[9]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *AggregateValuesResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AggregateValuesResponse) ProtoMessage() {}

func (x *AggregateValuesResponse) ProtoReflect() protoreflect.Message {
	mi := &file_nitrous_proto_msgTypes[9]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use AggregateValuesResponse.ProtoReflect.Descriptor instead.
func (*AggregateValuesResponse) Descriptor() ([]byte, []int) {
	return file_nitrous_proto_rawDescGZIP(), []int{9}
}

func (x *AggregateValuesResponse) GetResults() []*value.PValue {
	if x != nil {
		return x.Results
	}
	return nil
}

type ProfilesRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	TierId uint32        `protobuf:"varint,1,opt,name=tier_id,json=tierId,proto3" json:"tier_id,omitempty"`
	Rows   []*ProfileKey `protobuf:"bytes,2,rep,name=rows,proto3" json:"rows,omitempty"`
}

func (x *ProfilesRequest) Reset() {
	*x = ProfilesRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_nitrous_proto_msgTypes[10]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ProfilesRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ProfilesRequest) ProtoMessage() {}

func (x *ProfilesRequest) ProtoReflect() protoreflect.Message {
	mi := &file_nitrous_proto_msgTypes[10]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ProfilesRequest.ProtoReflect.Descriptor instead.
func (*ProfilesRequest) Descriptor() ([]byte, []int) {
	return file_nitrous_proto_rawDescGZIP(), []int{10}
}

func (x *ProfilesRequest) GetTierId() uint32 {
	if x != nil {
		return x.TierId
	}
	return 0
}

func (x *ProfilesRequest) GetRows() []*ProfileKey {
	if x != nil {
		return x.Rows
	}
	return nil
}

type ProfilesResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Results []*value.PValue `protobuf:"bytes,1,rep,name=results,proto3" json:"results,omitempty"`
}

func (x *ProfilesResponse) Reset() {
	*x = ProfilesResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_nitrous_proto_msgTypes[11]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ProfilesResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ProfilesResponse) ProtoMessage() {}

func (x *ProfilesResponse) ProtoReflect() protoreflect.Message {
	mi := &file_nitrous_proto_msgTypes[11]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ProfilesResponse.ProtoReflect.Descriptor instead.
func (*ProfilesResponse) Descriptor() ([]byte, []int) {
	return file_nitrous_proto_rawDescGZIP(), []int{11}
}

func (x *ProfilesResponse) GetResults() []*value.PValue {
	if x != nil {
		return x.Results
	}
	return nil
}

var File_nitrous_proto protoreflect.FileDescriptor

var file_nitrous_proto_rawDesc = []byte{
	0x0a, 0x0d, 0x6e, 0x69, 0x74, 0x72, 0x6f, 0x75, 0x73, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12,
	0x07, 0x6e, 0x69, 0x74, 0x72, 0x6f, 0x75, 0x73, 0x1a, 0x0b, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x0f, 0x61, 0x67, 0x67, 0x72, 0x65, 0x67, 0x61, 0x74, 0x65,
	0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0xc3, 0x02, 0x0a, 0x09, 0x4e, 0x69, 0x74, 0x72, 0x6f,
	0x75, 0x73, 0x4f, 0x70, 0x12, 0x17, 0x0a, 0x07, 0x74, 0x69, 0x65, 0x72, 0x5f, 0x69, 0x64, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x06, 0x74, 0x69, 0x65, 0x72, 0x49, 0x64, 0x12, 0x23, 0x0a,
	0x04, 0x74, 0x79, 0x70, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x0f, 0x2e, 0x6e, 0x69,
	0x74, 0x72, 0x6f, 0x75, 0x73, 0x2e, 0x4f, 0x70, 0x54, 0x79, 0x70, 0x65, 0x52, 0x04, 0x74, 0x79,
	0x70, 0x65, 0x12, 0x45, 0x0a, 0x10, 0x63, 0x72, 0x65, 0x61, 0x74, 0x65, 0x5f, 0x61, 0x67, 0x67,
	0x72, 0x65, 0x67, 0x61, 0x74, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x18, 0x2e, 0x6e,
	0x69, 0x74, 0x72, 0x6f, 0x75, 0x73, 0x2e, 0x43, 0x72, 0x65, 0x61, 0x74, 0x65, 0x41, 0x67, 0x67,
	0x72, 0x65, 0x67, 0x61, 0x74, 0x65, 0x48, 0x00, 0x52, 0x0f, 0x63, 0x72, 0x65, 0x61, 0x74, 0x65,
	0x41, 0x67, 0x67, 0x72, 0x65, 0x67, 0x61, 0x74, 0x65, 0x12, 0x45, 0x0a, 0x10, 0x64, 0x65, 0x6c,
	0x65, 0x74, 0x65, 0x5f, 0x61, 0x67, 0x67, 0x72, 0x65, 0x67, 0x61, 0x74, 0x65, 0x18, 0x04, 0x20,
	0x01, 0x28, 0x0b, 0x32, 0x18, 0x2e, 0x6e, 0x69, 0x74, 0x72, 0x6f, 0x75, 0x73, 0x2e, 0x44, 0x65,
	0x6c, 0x65, 0x74, 0x65, 0x41, 0x67, 0x67, 0x72, 0x65, 0x67, 0x61, 0x74, 0x65, 0x48, 0x00, 0x52,
	0x0f, 0x64, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x41, 0x67, 0x67, 0x72, 0x65, 0x67, 0x61, 0x74, 0x65,
	0x12, 0x30, 0x0a, 0x09, 0x61, 0x67, 0x67, 0x5f, 0x65, 0x76, 0x65, 0x6e, 0x74, 0x18, 0x05, 0x20,
	0x01, 0x28, 0x0b, 0x32, 0x11, 0x2e, 0x6e, 0x69, 0x74, 0x72, 0x6f, 0x75, 0x73, 0x2e, 0x41, 0x67,
	0x67, 0x45, 0x76, 0x65, 0x6e, 0x74, 0x48, 0x00, 0x52, 0x08, 0x61, 0x67, 0x67, 0x45, 0x76, 0x65,
	0x6e, 0x74, 0x12, 0x32, 0x0a, 0x07, 0x70, 0x72, 0x6f, 0x66, 0x69, 0x6c, 0x65, 0x18, 0x06, 0x20,
	0x01, 0x28, 0x0b, 0x32, 0x16, 0x2e, 0x6e, 0x69, 0x74, 0x72, 0x6f, 0x75, 0x73, 0x2e, 0x50, 0x72,
	0x6f, 0x66, 0x69, 0x6c, 0x65, 0x55, 0x70, 0x64, 0x61, 0x74, 0x65, 0x48, 0x00, 0x52, 0x07, 0x70,
	0x72, 0x6f, 0x66, 0x69, 0x6c, 0x65, 0x42, 0x04, 0x0a, 0x02, 0x6f, 0x70, 0x22, 0x4f, 0x0a, 0x0f,
	0x43, 0x72, 0x65, 0x61, 0x74, 0x65, 0x41, 0x67, 0x67, 0x72, 0x65, 0x67, 0x61, 0x74, 0x65, 0x12,
	0x15, 0x0a, 0x06, 0x61, 0x67, 0x67, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0d, 0x52,
	0x05, 0x61, 0x67, 0x67, 0x49, 0x64, 0x12, 0x25, 0x0a, 0x07, 0x6f, 0x70, 0x74, 0x69, 0x6f, 0x6e,
	0x73, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x0b, 0x2e, 0x41, 0x67, 0x67, 0x4f, 0x70, 0x74,
	0x69, 0x6f, 0x6e, 0x73, 0x52, 0x07, 0x6f, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x22, 0x28, 0x0a,
	0x0f, 0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x41, 0x67, 0x67, 0x72, 0x65, 0x67, 0x61, 0x74, 0x65,
	0x12, 0x15, 0x0a, 0x06, 0x61, 0x67, 0x67, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0d,
	0x52, 0x05, 0x61, 0x67, 0x67, 0x49, 0x64, 0x22, 0x7a, 0x0a, 0x08, 0x41, 0x67, 0x67, 0x45, 0x76,
	0x65, 0x6e, 0x74, 0x12, 0x15, 0x0a, 0x06, 0x61, 0x67, 0x67, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x0d, 0x52, 0x05, 0x61, 0x67, 0x67, 0x49, 0x64, 0x12, 0x1a, 0x0a, 0x08, 0x67, 0x72,
	0x6f, 0x75, 0x70, 0x6b, 0x65, 0x79, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x67, 0x72,
	0x6f, 0x75, 0x70, 0x6b, 0x65, 0x79, 0x12, 0x1c, 0x0a, 0x09, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74,
	0x61, 0x6d, 0x70, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x09, 0x74, 0x69, 0x6d, 0x65, 0x73,
	0x74, 0x61, 0x6d, 0x70, 0x12, 0x1d, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x04, 0x20,
	0x01, 0x28, 0x0b, 0x32, 0x07, 0x2e, 0x50, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x52, 0x05, 0x76, 0x61,
	0x6c, 0x75, 0x65, 0x22, 0x48, 0x0a, 0x0a, 0x50, 0x72, 0x6f, 0x66, 0x69, 0x6c, 0x65, 0x4b, 0x65,
	0x79, 0x12, 0x14, 0x0a, 0x05, 0x6f, 0x74, 0x79, 0x70, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x05, 0x6f, 0x74, 0x79, 0x70, 0x65, 0x12, 0x10, 0x0a, 0x03, 0x6f, 0x69, 0x64, 0x18, 0x02,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x6f, 0x69, 0x64, 0x12, 0x12, 0x0a, 0x04, 0x7a, 0x6b, 0x65,
	0x79, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x7a, 0x6b, 0x65, 0x79, 0x22, 0x73, 0x0a,
	0x0d, 0x50, 0x72, 0x6f, 0x66, 0x69, 0x6c, 0x65, 0x55, 0x70, 0x64, 0x61, 0x74, 0x65, 0x12, 0x25,
	0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x13, 0x2e, 0x6e, 0x69,
	0x74, 0x72, 0x6f, 0x75, 0x73, 0x2e, 0x50, 0x72, 0x6f, 0x66, 0x69, 0x6c, 0x65, 0x4b, 0x65, 0x79,
	0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x1d, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02,
	0x20, 0x01, 0x28, 0x0b, 0x32, 0x07, 0x2e, 0x50, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x52, 0x05, 0x76,
	0x61, 0x6c, 0x75, 0x65, 0x12, 0x1c, 0x0a, 0x09, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d,
	0x70, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x09, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61,
	0x6d, 0x70, 0x22, 0x0c, 0x0a, 0x0a, 0x4c, 0x61, 0x67, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74,
	0x22, 0x1f, 0x0a, 0x0b, 0x4c, 0x61, 0x67, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12,
	0x10, 0x0a, 0x03, 0x6c, 0x61, 0x67, 0x18, 0x01, 0x20, 0x01, 0x28, 0x04, 0x52, 0x03, 0x6c, 0x61,
	0x67, 0x22, 0x96, 0x01, 0x0a, 0x16, 0x41, 0x67, 0x67, 0x72, 0x65, 0x67, 0x61, 0x74, 0x65, 0x56,
	0x61, 0x6c, 0x75, 0x65, 0x73, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x17, 0x0a, 0x07,
	0x74, 0x69, 0x65, 0x72, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x06, 0x74,
	0x69, 0x65, 0x72, 0x49, 0x64, 0x12, 0x15, 0x0a, 0x06, 0x61, 0x67, 0x67, 0x5f, 0x69, 0x64, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x05, 0x61, 0x67, 0x67, 0x49, 0x64, 0x12, 0x14, 0x0a, 0x05,
	0x63, 0x6f, 0x64, 0x65, 0x63, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x05, 0x63, 0x6f, 0x64,
	0x65, 0x63, 0x12, 0x1a, 0x0a, 0x08, 0x64, 0x75, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x18, 0x04,
	0x20, 0x01, 0x28, 0x0d, 0x52, 0x08, 0x64, 0x75, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x1a,
	0x0a, 0x08, 0x67, 0x72, 0x6f, 0x75, 0x70, 0x6b, 0x65, 0x79, 0x18, 0x05, 0x20, 0x03, 0x28, 0x09,
	0x52, 0x08, 0x67, 0x72, 0x6f, 0x75, 0x70, 0x6b, 0x65, 0x79, 0x22, 0x3c, 0x0a, 0x17, 0x41, 0x67,
	0x67, 0x72, 0x65, 0x67, 0x61, 0x74, 0x65, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x73, 0x52, 0x65, 0x73,
	0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x21, 0x0a, 0x07, 0x72, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x73,
	0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x07, 0x2e, 0x50, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x52,
	0x07, 0x72, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x73, 0x22, 0x53, 0x0a, 0x0f, 0x50, 0x72, 0x6f, 0x66,
	0x69, 0x6c, 0x65, 0x73, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x17, 0x0a, 0x07, 0x74,
	0x69, 0x65, 0x72, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x06, 0x74, 0x69,
	0x65, 0x72, 0x49, 0x64, 0x12, 0x27, 0x0a, 0x04, 0x72, 0x6f, 0x77, 0x73, 0x18, 0x02, 0x20, 0x03,
	0x28, 0x0b, 0x32, 0x13, 0x2e, 0x6e, 0x69, 0x74, 0x72, 0x6f, 0x75, 0x73, 0x2e, 0x50, 0x72, 0x6f,
	0x66, 0x69, 0x6c, 0x65, 0x4b, 0x65, 0x79, 0x52, 0x04, 0x72, 0x6f, 0x77, 0x73, 0x22, 0x35, 0x0a,
	0x10, 0x50, 0x72, 0x6f, 0x66, 0x69, 0x6c, 0x65, 0x73, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73,
	0x65, 0x12, 0x21, 0x0a, 0x07, 0x72, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x73, 0x18, 0x01, 0x20, 0x03,
	0x28, 0x0b, 0x32, 0x07, 0x2e, 0x50, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x52, 0x07, 0x72, 0x65, 0x73,
	0x75, 0x6c, 0x74, 0x73, 0x2a, 0x57, 0x0a, 0x06, 0x4f, 0x70, 0x54, 0x79, 0x70, 0x65, 0x12, 0x0d,
	0x0a, 0x09, 0x41, 0x47, 0x47, 0x5f, 0x45, 0x56, 0x45, 0x4e, 0x54, 0x10, 0x00, 0x12, 0x12, 0x0a,
	0x0e, 0x50, 0x52, 0x4f, 0x46, 0x49, 0x4c, 0x45, 0x5f, 0x55, 0x50, 0x44, 0x41, 0x54, 0x45, 0x10,
	0x01, 0x12, 0x14, 0x0a, 0x10, 0x43, 0x52, 0x45, 0x41, 0x54, 0x45, 0x5f, 0x41, 0x47, 0x47, 0x52,
	0x45, 0x47, 0x41, 0x54, 0x45, 0x10, 0x02, 0x12, 0x14, 0x0a, 0x10, 0x44, 0x45, 0x4c, 0x45, 0x54,
	0x45, 0x5f, 0x41, 0x47, 0x47, 0x52, 0x45, 0x47, 0x41, 0x54, 0x45, 0x10, 0x03, 0x32, 0xdb, 0x01,
	0x0a, 0x07, 0x4e, 0x69, 0x74, 0x72, 0x6f, 0x75, 0x73, 0x12, 0x42, 0x0a, 0x0b, 0x47, 0x65, 0x74,
	0x50, 0x72, 0x6f, 0x66, 0x69, 0x6c, 0x65, 0x73, 0x12, 0x18, 0x2e, 0x6e, 0x69, 0x74, 0x72, 0x6f,
	0x75, 0x73, 0x2e, 0x50, 0x72, 0x6f, 0x66, 0x69, 0x6c, 0x65, 0x73, 0x52, 0x65, 0x71, 0x75, 0x65,
	0x73, 0x74, 0x1a, 0x19, 0x2e, 0x6e, 0x69, 0x74, 0x72, 0x6f, 0x75, 0x73, 0x2e, 0x50, 0x72, 0x6f,
	0x66, 0x69, 0x6c, 0x65, 0x73, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x57, 0x0a,
	0x12, 0x47, 0x65, 0x74, 0x41, 0x67, 0x67, 0x72, 0x65, 0x67, 0x61, 0x74, 0x65, 0x56, 0x61, 0x6c,
	0x75, 0x65, 0x73, 0x12, 0x1f, 0x2e, 0x6e, 0x69, 0x74, 0x72, 0x6f, 0x75, 0x73, 0x2e, 0x41, 0x67,
	0x67, 0x72, 0x65, 0x67, 0x61, 0x74, 0x65, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x73, 0x52, 0x65, 0x71,
	0x75, 0x65, 0x73, 0x74, 0x1a, 0x20, 0x2e, 0x6e, 0x69, 0x74, 0x72, 0x6f, 0x75, 0x73, 0x2e, 0x41,
	0x67, 0x67, 0x72, 0x65, 0x67, 0x61, 0x74, 0x65, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x73, 0x52, 0x65,
	0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x33, 0x0a, 0x06, 0x47, 0x65, 0x74, 0x4c, 0x61, 0x67,
	0x12, 0x13, 0x2e, 0x6e, 0x69, 0x74, 0x72, 0x6f, 0x75, 0x73, 0x2e, 0x4c, 0x61, 0x67, 0x52, 0x65,
	0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x14, 0x2e, 0x6e, 0x69, 0x74, 0x72, 0x6f, 0x75, 0x73, 0x2e,
	0x4c, 0x61, 0x67, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x42, 0x17, 0x5a, 0x15, 0x66,
	0x65, 0x6e, 0x6e, 0x65, 0x6c, 0x2f, 0x6e, 0x69, 0x74, 0x72, 0x6f, 0x75, 0x73, 0x2f, 0x72, 0x70,
	0x63, 0x2f, 0x76, 0x32, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_nitrous_proto_rawDescOnce sync.Once
	file_nitrous_proto_rawDescData = file_nitrous_proto_rawDesc
)

func file_nitrous_proto_rawDescGZIP() []byte {
	file_nitrous_proto_rawDescOnce.Do(func() {
		file_nitrous_proto_rawDescData = protoimpl.X.CompressGZIP(file_nitrous_proto_rawDescData)
	})
	return file_nitrous_proto_rawDescData
}

var file_nitrous_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_nitrous_proto_msgTypes = make([]protoimpl.MessageInfo, 12)
var file_nitrous_proto_goTypes = []interface{}{
	(OpType)(0),                     // 0: nitrous.OpType
	(*NitrousOp)(nil),               // 1: nitrous.NitrousOp
	(*CreateAggregate)(nil),         // 2: nitrous.CreateAggregate
	(*DeleteAggregate)(nil),         // 3: nitrous.DeleteAggregate
	(*AggEvent)(nil),                // 4: nitrous.AggEvent
	(*ProfileKey)(nil),              // 5: nitrous.ProfileKey
	(*ProfileUpdate)(nil),           // 6: nitrous.ProfileUpdate
	(*LagRequest)(nil),              // 7: nitrous.LagRequest
	(*LagResponse)(nil),             // 8: nitrous.LagResponse
	(*AggregateValuesRequest)(nil),  // 9: nitrous.AggregateValuesRequest
	(*AggregateValuesResponse)(nil), // 10: nitrous.AggregateValuesResponse
	(*ProfilesRequest)(nil),         // 11: nitrous.ProfilesRequest
	(*ProfilesResponse)(nil),        // 12: nitrous.ProfilesResponse
	(*aggregate.AggOptions)(nil),    // 13: AggOptions
	(*value.PValue)(nil),            // 14: PValue
}
var file_nitrous_proto_depIdxs = []int32{
	0,  // 0: nitrous.NitrousOp.type:type_name -> nitrous.OpType
	2,  // 1: nitrous.NitrousOp.create_aggregate:type_name -> nitrous.CreateAggregate
	3,  // 2: nitrous.NitrousOp.delete_aggregate:type_name -> nitrous.DeleteAggregate
	4,  // 3: nitrous.NitrousOp.agg_event:type_name -> nitrous.AggEvent
	6,  // 4: nitrous.NitrousOp.profile:type_name -> nitrous.ProfileUpdate
	13, // 5: nitrous.CreateAggregate.options:type_name -> AggOptions
	14, // 6: nitrous.AggEvent.value:type_name -> PValue
	5,  // 7: nitrous.ProfileUpdate.key:type_name -> nitrous.ProfileKey
	14, // 8: nitrous.ProfileUpdate.value:type_name -> PValue
	14, // 9: nitrous.AggregateValuesResponse.results:type_name -> PValue
	5,  // 10: nitrous.ProfilesRequest.rows:type_name -> nitrous.ProfileKey
	14, // 11: nitrous.ProfilesResponse.results:type_name -> PValue
	11, // 12: nitrous.Nitrous.GetProfiles:input_type -> nitrous.ProfilesRequest
	9,  // 13: nitrous.Nitrous.GetAggregateValues:input_type -> nitrous.AggregateValuesRequest
	7,  // 14: nitrous.Nitrous.GetLag:input_type -> nitrous.LagRequest
	12, // 15: nitrous.Nitrous.GetProfiles:output_type -> nitrous.ProfilesResponse
	10, // 16: nitrous.Nitrous.GetAggregateValues:output_type -> nitrous.AggregateValuesResponse
	8,  // 17: nitrous.Nitrous.GetLag:output_type -> nitrous.LagResponse
	15, // [15:18] is the sub-list for method output_type
	12, // [12:15] is the sub-list for method input_type
	12, // [12:12] is the sub-list for extension type_name
	12, // [12:12] is the sub-list for extension extendee
	0,  // [0:12] is the sub-list for field type_name
}

func init() { file_nitrous_proto_init() }
func file_nitrous_proto_init() {
	if File_nitrous_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_nitrous_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*NitrousOp); i {
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
		file_nitrous_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*CreateAggregate); i {
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
		file_nitrous_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*DeleteAggregate); i {
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
		file_nitrous_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*AggEvent); i {
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
		file_nitrous_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ProfileKey); i {
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
		file_nitrous_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ProfileUpdate); i {
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
		file_nitrous_proto_msgTypes[6].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*LagRequest); i {
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
		file_nitrous_proto_msgTypes[7].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*LagResponse); i {
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
		file_nitrous_proto_msgTypes[8].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*AggregateValuesRequest); i {
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
		file_nitrous_proto_msgTypes[9].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*AggregateValuesResponse); i {
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
		file_nitrous_proto_msgTypes[10].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ProfilesRequest); i {
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
		file_nitrous_proto_msgTypes[11].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ProfilesResponse); i {
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
	file_nitrous_proto_msgTypes[0].OneofWrappers = []interface{}{
		(*NitrousOp_CreateAggregate)(nil),
		(*NitrousOp_DeleteAggregate)(nil),
		(*NitrousOp_AggEvent)(nil),
		(*NitrousOp_Profile)(nil),
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_nitrous_proto_rawDesc,
			NumEnums:      1,
			NumMessages:   12,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_nitrous_proto_goTypes,
		DependencyIndexes: file_nitrous_proto_depIdxs,
		EnumInfos:         file_nitrous_proto_enumTypes,
		MessageInfos:      file_nitrous_proto_msgTypes,
	}.Build()
	File_nitrous_proto = out.File
	file_nitrous_proto_rawDesc = nil
	file_nitrous_proto_goTypes = nil
	file_nitrous_proto_depIdxs = nil
}
