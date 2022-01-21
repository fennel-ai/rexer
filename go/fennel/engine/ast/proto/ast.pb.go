// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.27.1
// 	protoc        v3.19.3
// source: ast.proto

package proto

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

type Ast struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Types that are assignable to Node:
	//	*Ast_Atom
	//	*Ast_Binary
	//	*Ast_Statement
	//	*Ast_Query
	//	*Ast_List
	//	*Ast_Dict
	//	*Ast_Opcall
	//	*Ast_Var
	//	*Ast_Table
	Node isAst_Node `protobuf_oneof:"node"`
}

func (x *Ast) Reset() {
	*x = Ast{}
	if protoimpl.UnsafeEnabled {
		mi := &file_ast_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Ast) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Ast) ProtoMessage() {}

func (x *Ast) ProtoReflect() protoreflect.Message {
	mi := &file_ast_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Ast.ProtoReflect.Descriptor instead.
func (*Ast) Descriptor() ([]byte, []int) {
	return file_ast_proto_rawDescGZIP(), []int{0}
}

func (m *Ast) GetNode() isAst_Node {
	if m != nil {
		return m.Node
	}
	return nil
}

func (x *Ast) GetAtom() *Atom {
	if x, ok := x.GetNode().(*Ast_Atom); ok {
		return x.Atom
	}
	return nil
}

func (x *Ast) GetBinary() *Binary {
	if x, ok := x.GetNode().(*Ast_Binary); ok {
		return x.Binary
	}
	return nil
}

func (x *Ast) GetStatement() *Statement {
	if x, ok := x.GetNode().(*Ast_Statement); ok {
		return x.Statement
	}
	return nil
}

func (x *Ast) GetQuery() *Query {
	if x, ok := x.GetNode().(*Ast_Query); ok {
		return x.Query
	}
	return nil
}

func (x *Ast) GetList() *List {
	if x, ok := x.GetNode().(*Ast_List); ok {
		return x.List
	}
	return nil
}

func (x *Ast) GetDict() *Dict {
	if x, ok := x.GetNode().(*Ast_Dict); ok {
		return x.Dict
	}
	return nil
}

func (x *Ast) GetOpcall() *OpCall {
	if x, ok := x.GetNode().(*Ast_Opcall); ok {
		return x.Opcall
	}
	return nil
}

func (x *Ast) GetVar() *Var {
	if x, ok := x.GetNode().(*Ast_Var); ok {
		return x.Var
	}
	return nil
}

func (x *Ast) GetTable() *Table {
	if x, ok := x.GetNode().(*Ast_Table); ok {
		return x.Table
	}
	return nil
}

type isAst_Node interface {
	isAst_Node()
}

type Ast_Atom struct {
	Atom *Atom `protobuf:"bytes,1,opt,name=atom,proto3,oneof"`
}

type Ast_Binary struct {
	Binary *Binary `protobuf:"bytes,2,opt,name=binary,proto3,oneof"`
}

type Ast_Statement struct {
	Statement *Statement `protobuf:"bytes,3,opt,name=statement,proto3,oneof"`
}

type Ast_Query struct {
	Query *Query `protobuf:"bytes,4,opt,name=query,proto3,oneof"`
}

type Ast_List struct {
	List *List `protobuf:"bytes,5,opt,name=list,proto3,oneof"`
}

type Ast_Dict struct {
	Dict *Dict `protobuf:"bytes,6,opt,name=dict,proto3,oneof"`
}

type Ast_Opcall struct {
	Opcall *OpCall `protobuf:"bytes,7,opt,name=opcall,proto3,oneof"`
}

type Ast_Var struct {
	Var *Var `protobuf:"bytes,8,opt,name=var,proto3,oneof"`
}

type Ast_Table struct {
	Table *Table `protobuf:"bytes,9,opt,name=table,proto3,oneof"`
}

func (*Ast_Atom) isAst_Node() {}

func (*Ast_Binary) isAst_Node() {}

func (*Ast_Statement) isAst_Node() {}

func (*Ast_Query) isAst_Node() {}

func (*Ast_List) isAst_Node() {}

func (*Ast_Dict) isAst_Node() {}

func (*Ast_Opcall) isAst_Node() {}

func (*Ast_Var) isAst_Node() {}

func (*Ast_Table) isAst_Node() {}

type Binary struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Left  *Ast   `protobuf:"bytes,1,opt,name=left,proto3" json:"left,omitempty"`
	Right *Ast   `protobuf:"bytes,2,opt,name=right,proto3" json:"right,omitempty"`
	Op    string `protobuf:"bytes,3,opt,name=op,proto3" json:"op,omitempty"`
}

func (x *Binary) Reset() {
	*x = Binary{}
	if protoimpl.UnsafeEnabled {
		mi := &file_ast_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Binary) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Binary) ProtoMessage() {}

func (x *Binary) ProtoReflect() protoreflect.Message {
	mi := &file_ast_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Binary.ProtoReflect.Descriptor instead.
func (*Binary) Descriptor() ([]byte, []int) {
	return file_ast_proto_rawDescGZIP(), []int{1}
}

func (x *Binary) GetLeft() *Ast {
	if x != nil {
		return x.Left
	}
	return nil
}

func (x *Binary) GetRight() *Ast {
	if x != nil {
		return x.Right
	}
	return nil
}

func (x *Binary) GetOp() string {
	if x != nil {
		return x.Op
	}
	return ""
}

type Statement struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Name string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	Body *Ast   `protobuf:"bytes,2,opt,name=body,proto3" json:"body,omitempty"`
}

func (x *Statement) Reset() {
	*x = Statement{}
	if protoimpl.UnsafeEnabled {
		mi := &file_ast_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Statement) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Statement) ProtoMessage() {}

func (x *Statement) ProtoReflect() protoreflect.Message {
	mi := &file_ast_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Statement.ProtoReflect.Descriptor instead.
func (*Statement) Descriptor() ([]byte, []int) {
	return file_ast_proto_rawDescGZIP(), []int{2}
}

func (x *Statement) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *Statement) GetBody() *Ast {
	if x != nil {
		return x.Body
	}
	return nil
}

type Query struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Statements []*Statement `protobuf:"bytes,1,rep,name=statements,proto3" json:"statements,omitempty"`
}

func (x *Query) Reset() {
	*x = Query{}
	if protoimpl.UnsafeEnabled {
		mi := &file_ast_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Query) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Query) ProtoMessage() {}

func (x *Query) ProtoReflect() protoreflect.Message {
	mi := &file_ast_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Query.ProtoReflect.Descriptor instead.
func (*Query) Descriptor() ([]byte, []int) {
	return file_ast_proto_rawDescGZIP(), []int{3}
}

func (x *Query) GetStatements() []*Statement {
	if x != nil {
		return x.Statements
	}
	return nil
}

type Atom struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Types that are assignable to Inner:
	//	*Atom_Int
	//	*Atom_String_
	//	*Atom_Bool
	//	*Atom_Double
	Inner isAtom_Inner `protobuf_oneof:"inner"`
}

func (x *Atom) Reset() {
	*x = Atom{}
	if protoimpl.UnsafeEnabled {
		mi := &file_ast_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Atom) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Atom) ProtoMessage() {}

func (x *Atom) ProtoReflect() protoreflect.Message {
	mi := &file_ast_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Atom.ProtoReflect.Descriptor instead.
func (*Atom) Descriptor() ([]byte, []int) {
	return file_ast_proto_rawDescGZIP(), []int{4}
}

func (m *Atom) GetInner() isAtom_Inner {
	if m != nil {
		return m.Inner
	}
	return nil
}

func (x *Atom) GetInt() int64 {
	if x, ok := x.GetInner().(*Atom_Int); ok {
		return x.Int
	}
	return 0
}

func (x *Atom) GetString_() string {
	if x, ok := x.GetInner().(*Atom_String_); ok {
		return x.String_
	}
	return ""
}

func (x *Atom) GetBool() bool {
	if x, ok := x.GetInner().(*Atom_Bool); ok {
		return x.Bool
	}
	return false
}

func (x *Atom) GetDouble() float64 {
	if x, ok := x.GetInner().(*Atom_Double); ok {
		return x.Double
	}
	return 0
}

type isAtom_Inner interface {
	isAtom_Inner()
}

type Atom_Int struct {
	Int int64 `protobuf:"varint,1,opt,name=int,proto3,oneof"`
}

type Atom_String_ struct {
	String_ string `protobuf:"bytes,2,opt,name=string,proto3,oneof"`
}

type Atom_Bool struct {
	Bool bool `protobuf:"varint,3,opt,name=bool,proto3,oneof"`
}

type Atom_Double struct {
	Double float64 `protobuf:"fixed64,4,opt,name=double,proto3,oneof"`
}

func (*Atom_Int) isAtom_Inner() {}

func (*Atom_String_) isAtom_Inner() {}

func (*Atom_Bool) isAtom_Inner() {}

func (*Atom_Double) isAtom_Inner() {}

type List struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Values []*Ast `protobuf:"bytes,1,rep,name=values,proto3" json:"values,omitempty"`
}

func (x *List) Reset() {
	*x = List{}
	if protoimpl.UnsafeEnabled {
		mi := &file_ast_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *List) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*List) ProtoMessage() {}

func (x *List) ProtoReflect() protoreflect.Message {
	mi := &file_ast_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use List.ProtoReflect.Descriptor instead.
func (*List) Descriptor() ([]byte, []int) {
	return file_ast_proto_rawDescGZIP(), []int{5}
}

func (x *List) GetValues() []*Ast {
	if x != nil {
		return x.Values
	}
	return nil
}

type Dict struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Values map[string]*Ast `protobuf:"bytes,1,rep,name=values,proto3" json:"values,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
}

func (x *Dict) Reset() {
	*x = Dict{}
	if protoimpl.UnsafeEnabled {
		mi := &file_ast_proto_msgTypes[6]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Dict) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Dict) ProtoMessage() {}

func (x *Dict) ProtoReflect() protoreflect.Message {
	mi := &file_ast_proto_msgTypes[6]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Dict.ProtoReflect.Descriptor instead.
func (*Dict) Descriptor() ([]byte, []int) {
	return file_ast_proto_rawDescGZIP(), []int{6}
}

func (x *Dict) GetValues() map[string]*Ast {
	if x != nil {
		return x.Values
	}
	return nil
}

type OpCall struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Operand   *Ast   `protobuf:"bytes,1,opt,name=operand,proto3" json:"operand,omitempty"`
	Namespace string `protobuf:"bytes,2,opt,name=namespace,proto3" json:"namespace,omitempty"`
	Name      string `protobuf:"bytes,3,opt,name=name,proto3" json:"name,omitempty"`
	Kwargs    *Dict  `protobuf:"bytes,4,opt,name=kwargs,proto3" json:"kwargs,omitempty"`
}

func (x *OpCall) Reset() {
	*x = OpCall{}
	if protoimpl.UnsafeEnabled {
		mi := &file_ast_proto_msgTypes[7]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *OpCall) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*OpCall) ProtoMessage() {}

func (x *OpCall) ProtoReflect() protoreflect.Message {
	mi := &file_ast_proto_msgTypes[7]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use OpCall.ProtoReflect.Descriptor instead.
func (*OpCall) Descriptor() ([]byte, []int) {
	return file_ast_proto_rawDescGZIP(), []int{7}
}

func (x *OpCall) GetOperand() *Ast {
	if x != nil {
		return x.Operand
	}
	return nil
}

func (x *OpCall) GetNamespace() string {
	if x != nil {
		return x.Namespace
	}
	return ""
}

func (x *OpCall) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *OpCall) GetKwargs() *Dict {
	if x != nil {
		return x.Kwargs
	}
	return nil
}

type Var struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Name string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
}

func (x *Var) Reset() {
	*x = Var{}
	if protoimpl.UnsafeEnabled {
		mi := &file_ast_proto_msgTypes[8]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Var) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Var) ProtoMessage() {}

func (x *Var) ProtoReflect() protoreflect.Message {
	mi := &file_ast_proto_msgTypes[8]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Var.ProtoReflect.Descriptor instead.
func (*Var) Descriptor() ([]byte, []int) {
	return file_ast_proto_rawDescGZIP(), []int{8}
}

func (x *Var) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

type Table struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Inner *Ast `protobuf:"bytes,1,opt,name=inner,proto3" json:"inner,omitempty"`
}

func (x *Table) Reset() {
	*x = Table{}
	if protoimpl.UnsafeEnabled {
		mi := &file_ast_proto_msgTypes[9]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Table) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Table) ProtoMessage() {}

func (x *Table) ProtoReflect() protoreflect.Message {
	mi := &file_ast_proto_msgTypes[9]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Table.ProtoReflect.Descriptor instead.
func (*Table) Descriptor() ([]byte, []int) {
	return file_ast_proto_rawDescGZIP(), []int{9}
}

func (x *Table) GetInner() *Ast {
	if x != nil {
		return x.Inner
	}
	return nil
}

var File_ast_proto protoreflect.FileDescriptor

var file_ast_proto_rawDesc = []byte{
	0x0a, 0x09, 0x61, 0x73, 0x74, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0xb0, 0x02, 0x0a, 0x03,
	0x41, 0x73, 0x74, 0x12, 0x1b, 0x0a, 0x04, 0x61, 0x74, 0x6f, 0x6d, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x0b, 0x32, 0x05, 0x2e, 0x41, 0x74, 0x6f, 0x6d, 0x48, 0x00, 0x52, 0x04, 0x61, 0x74, 0x6f, 0x6d,
	0x12, 0x21, 0x0a, 0x06, 0x62, 0x69, 0x6e, 0x61, 0x72, 0x79, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b,
	0x32, 0x07, 0x2e, 0x42, 0x69, 0x6e, 0x61, 0x72, 0x79, 0x48, 0x00, 0x52, 0x06, 0x62, 0x69, 0x6e,
	0x61, 0x72, 0x79, 0x12, 0x2a, 0x0a, 0x09, 0x73, 0x74, 0x61, 0x74, 0x65, 0x6d, 0x65, 0x6e, 0x74,
	0x18, 0x03, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x0a, 0x2e, 0x53, 0x74, 0x61, 0x74, 0x65, 0x6d, 0x65,
	0x6e, 0x74, 0x48, 0x00, 0x52, 0x09, 0x73, 0x74, 0x61, 0x74, 0x65, 0x6d, 0x65, 0x6e, 0x74, 0x12,
	0x1e, 0x0a, 0x05, 0x71, 0x75, 0x65, 0x72, 0x79, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x06,
	0x2e, 0x51, 0x75, 0x65, 0x72, 0x79, 0x48, 0x00, 0x52, 0x05, 0x71, 0x75, 0x65, 0x72, 0x79, 0x12,
	0x1b, 0x0a, 0x04, 0x6c, 0x69, 0x73, 0x74, 0x18, 0x05, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x05, 0x2e,
	0x4c, 0x69, 0x73, 0x74, 0x48, 0x00, 0x52, 0x04, 0x6c, 0x69, 0x73, 0x74, 0x12, 0x1b, 0x0a, 0x04,
	0x64, 0x69, 0x63, 0x74, 0x18, 0x06, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x05, 0x2e, 0x44, 0x69, 0x63,
	0x74, 0x48, 0x00, 0x52, 0x04, 0x64, 0x69, 0x63, 0x74, 0x12, 0x21, 0x0a, 0x06, 0x6f, 0x70, 0x63,
	0x61, 0x6c, 0x6c, 0x18, 0x07, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x07, 0x2e, 0x4f, 0x70, 0x43, 0x61,
	0x6c, 0x6c, 0x48, 0x00, 0x52, 0x06, 0x6f, 0x70, 0x63, 0x61, 0x6c, 0x6c, 0x12, 0x18, 0x0a, 0x03,
	0x76, 0x61, 0x72, 0x18, 0x08, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x04, 0x2e, 0x56, 0x61, 0x72, 0x48,
	0x00, 0x52, 0x03, 0x76, 0x61, 0x72, 0x12, 0x1e, 0x0a, 0x05, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x18,
	0x09, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x06, 0x2e, 0x54, 0x61, 0x62, 0x6c, 0x65, 0x48, 0x00, 0x52,
	0x05, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x42, 0x06, 0x0a, 0x04, 0x6e, 0x6f, 0x64, 0x65, 0x22, 0x4e,
	0x0a, 0x06, 0x42, 0x69, 0x6e, 0x61, 0x72, 0x79, 0x12, 0x18, 0x0a, 0x04, 0x6c, 0x65, 0x66, 0x74,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x04, 0x2e, 0x41, 0x73, 0x74, 0x52, 0x04, 0x6c, 0x65,
	0x66, 0x74, 0x12, 0x1a, 0x0a, 0x05, 0x72, 0x69, 0x67, 0x68, 0x74, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x0b, 0x32, 0x04, 0x2e, 0x41, 0x73, 0x74, 0x52, 0x05, 0x72, 0x69, 0x67, 0x68, 0x74, 0x12, 0x0e,
	0x0a, 0x02, 0x6f, 0x70, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x02, 0x6f, 0x70, 0x22, 0x39,
	0x0a, 0x09, 0x53, 0x74, 0x61, 0x74, 0x65, 0x6d, 0x65, 0x6e, 0x74, 0x12, 0x12, 0x0a, 0x04, 0x6e,
	0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x12,
	0x18, 0x0a, 0x04, 0x62, 0x6f, 0x64, 0x79, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x04, 0x2e,
	0x41, 0x73, 0x74, 0x52, 0x04, 0x62, 0x6f, 0x64, 0x79, 0x22, 0x33, 0x0a, 0x05, 0x51, 0x75, 0x65,
	0x72, 0x79, 0x12, 0x2a, 0x0a, 0x0a, 0x73, 0x74, 0x61, 0x74, 0x65, 0x6d, 0x65, 0x6e, 0x74, 0x73,
	0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x0a, 0x2e, 0x53, 0x74, 0x61, 0x74, 0x65, 0x6d, 0x65,
	0x6e, 0x74, 0x52, 0x0a, 0x73, 0x74, 0x61, 0x74, 0x65, 0x6d, 0x65, 0x6e, 0x74, 0x73, 0x22, 0x6d,
	0x0a, 0x04, 0x41, 0x74, 0x6f, 0x6d, 0x12, 0x12, 0x0a, 0x03, 0x69, 0x6e, 0x74, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x03, 0x48, 0x00, 0x52, 0x03, 0x69, 0x6e, 0x74, 0x12, 0x18, 0x0a, 0x06, 0x73, 0x74,
	0x72, 0x69, 0x6e, 0x67, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x48, 0x00, 0x52, 0x06, 0x73, 0x74,
	0x72, 0x69, 0x6e, 0x67, 0x12, 0x14, 0x0a, 0x04, 0x62, 0x6f, 0x6f, 0x6c, 0x18, 0x03, 0x20, 0x01,
	0x28, 0x08, 0x48, 0x00, 0x52, 0x04, 0x62, 0x6f, 0x6f, 0x6c, 0x12, 0x18, 0x0a, 0x06, 0x64, 0x6f,
	0x75, 0x62, 0x6c, 0x65, 0x18, 0x04, 0x20, 0x01, 0x28, 0x01, 0x48, 0x00, 0x52, 0x06, 0x64, 0x6f,
	0x75, 0x62, 0x6c, 0x65, 0x42, 0x07, 0x0a, 0x05, 0x69, 0x6e, 0x6e, 0x65, 0x72, 0x22, 0x24, 0x0a,
	0x04, 0x4c, 0x69, 0x73, 0x74, 0x12, 0x1c, 0x0a, 0x06, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x73, 0x18,
	0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x04, 0x2e, 0x41, 0x73, 0x74, 0x52, 0x06, 0x76, 0x61, 0x6c,
	0x75, 0x65, 0x73, 0x22, 0x72, 0x0a, 0x04, 0x44, 0x69, 0x63, 0x74, 0x12, 0x29, 0x0a, 0x06, 0x76,
	0x61, 0x6c, 0x75, 0x65, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x11, 0x2e, 0x44, 0x69,
	0x63, 0x74, 0x2e, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x73, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x06,
	0x76, 0x61, 0x6c, 0x75, 0x65, 0x73, 0x1a, 0x3f, 0x0a, 0x0b, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x73,
	0x45, 0x6e, 0x74, 0x72, 0x79, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x1a, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x04, 0x2e, 0x41, 0x73, 0x74, 0x52, 0x05, 0x76, 0x61,
	0x6c, 0x75, 0x65, 0x3a, 0x02, 0x38, 0x01, 0x22, 0x79, 0x0a, 0x06, 0x4f, 0x70, 0x43, 0x61, 0x6c,
	0x6c, 0x12, 0x1e, 0x0a, 0x07, 0x6f, 0x70, 0x65, 0x72, 0x61, 0x6e, 0x64, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x0b, 0x32, 0x04, 0x2e, 0x41, 0x73, 0x74, 0x52, 0x07, 0x6f, 0x70, 0x65, 0x72, 0x61, 0x6e,
	0x64, 0x12, 0x1c, 0x0a, 0x09, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x18, 0x02,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x12,
	0x12, 0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x6e,
	0x61, 0x6d, 0x65, 0x12, 0x1d, 0x0a, 0x06, 0x6b, 0x77, 0x61, 0x72, 0x67, 0x73, 0x18, 0x04, 0x20,
	0x01, 0x28, 0x0b, 0x32, 0x05, 0x2e, 0x44, 0x69, 0x63, 0x74, 0x52, 0x06, 0x6b, 0x77, 0x61, 0x72,
	0x67, 0x73, 0x22, 0x19, 0x0a, 0x03, 0x56, 0x61, 0x72, 0x12, 0x12, 0x0a, 0x04, 0x6e, 0x61, 0x6d,
	0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x22, 0x23, 0x0a,
	0x05, 0x54, 0x61, 0x62, 0x6c, 0x65, 0x12, 0x1a, 0x0a, 0x05, 0x69, 0x6e, 0x6e, 0x65, 0x72, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x04, 0x2e, 0x41, 0x73, 0x74, 0x52, 0x05, 0x69, 0x6e, 0x6e,
	0x65, 0x72, 0x42, 0x19, 0x5a, 0x17, 0x66, 0x65, 0x6e, 0x6e, 0x65, 0x6c, 0x2f, 0x65, 0x6e, 0x67,
	0x69, 0x6e, 0x65, 0x2f, 0x61, 0x73, 0x74, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x06, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_ast_proto_rawDescOnce sync.Once
	file_ast_proto_rawDescData = file_ast_proto_rawDesc
)

func file_ast_proto_rawDescGZIP() []byte {
	file_ast_proto_rawDescOnce.Do(func() {
		file_ast_proto_rawDescData = protoimpl.X.CompressGZIP(file_ast_proto_rawDescData)
	})
	return file_ast_proto_rawDescData
}

var file_ast_proto_msgTypes = make([]protoimpl.MessageInfo, 11)
var file_ast_proto_goTypes = []interface{}{
	(*Ast)(nil),       // 0: Ast
	(*Binary)(nil),    // 1: Binary
	(*Statement)(nil), // 2: Statement
	(*Query)(nil),     // 3: Query
	(*Atom)(nil),      // 4: Atom
	(*List)(nil),      // 5: List
	(*Dict)(nil),      // 6: Dict
	(*OpCall)(nil),    // 7: OpCall
	(*Var)(nil),       // 8: Var
	(*Table)(nil),     // 9: Table
	nil,               // 10: Dict.ValuesEntry
}
var file_ast_proto_depIdxs = []int32{
	4,  // 0: Ast.atom:type_name -> Atom
	1,  // 1: Ast.binary:type_name -> Binary
	2,  // 2: Ast.statement:type_name -> Statement
	3,  // 3: Ast.query:type_name -> Query
	5,  // 4: Ast.list:type_name -> List
	6,  // 5: Ast.dict:type_name -> Dict
	7,  // 6: Ast.opcall:type_name -> OpCall
	8,  // 7: Ast.var:type_name -> Var
	9,  // 8: Ast.table:type_name -> Table
	0,  // 9: Binary.left:type_name -> Ast
	0,  // 10: Binary.right:type_name -> Ast
	0,  // 11: Statement.body:type_name -> Ast
	2,  // 12: Query.statements:type_name -> Statement
	0,  // 13: List.values:type_name -> Ast
	10, // 14: Dict.values:type_name -> Dict.ValuesEntry
	0,  // 15: OpCall.operand:type_name -> Ast
	6,  // 16: OpCall.kwargs:type_name -> Dict
	0,  // 17: Table.inner:type_name -> Ast
	0,  // 18: Dict.ValuesEntry.value:type_name -> Ast
	19, // [19:19] is the sub-list for method output_type
	19, // [19:19] is the sub-list for method input_type
	19, // [19:19] is the sub-list for extension type_name
	19, // [19:19] is the sub-list for extension extendee
	0,  // [0:19] is the sub-list for field type_name
}

func init() { file_ast_proto_init() }
func file_ast_proto_init() {
	if File_ast_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_ast_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Ast); i {
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
		file_ast_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Binary); i {
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
		file_ast_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Statement); i {
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
		file_ast_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Query); i {
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
		file_ast_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Atom); i {
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
		file_ast_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*List); i {
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
		file_ast_proto_msgTypes[6].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Dict); i {
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
		file_ast_proto_msgTypes[7].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*OpCall); i {
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
		file_ast_proto_msgTypes[8].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Var); i {
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
		file_ast_proto_msgTypes[9].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Table); i {
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
	file_ast_proto_msgTypes[0].OneofWrappers = []interface{}{
		(*Ast_Atom)(nil),
		(*Ast_Binary)(nil),
		(*Ast_Statement)(nil),
		(*Ast_Query)(nil),
		(*Ast_List)(nil),
		(*Ast_Dict)(nil),
		(*Ast_Opcall)(nil),
		(*Ast_Var)(nil),
		(*Ast_Table)(nil),
	}
	file_ast_proto_msgTypes[4].OneofWrappers = []interface{}{
		(*Atom_Int)(nil),
		(*Atom_String_)(nil),
		(*Atom_Bool)(nil),
		(*Atom_Double)(nil),
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_ast_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   11,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_ast_proto_goTypes,
		DependencyIndexes: file_ast_proto_depIdxs,
		MessageInfos:      file_ast_proto_msgTypes,
	}.Build()
	File_ast_proto = out.File
	file_ast_proto_rawDesc = nil
	file_ast_proto_goTypes = nil
	file_ast_proto_depIdxs = nil
}
