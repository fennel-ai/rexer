# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: ast.proto

import sys
_b=sys.version_info[0]<3 and (lambda x:x) or (lambda x:x.encode('latin1'))
from google.protobuf.internal import enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor.FileDescriptor(
  name='ast.proto',
  package='',
  syntax='proto3',
  serialized_options=_b('Z\nengine/ast'),
  serialized_pb=_b('\n\tast.proto\"\xf0\x01\n\x03\x41st\x12\x15\n\x04\x61tom\x18\x01 \x01(\x0b\x32\x05.AtomH\x00\x12\x19\n\x06\x62inary\x18\x02 \x01(\x0b\x32\x07.BinaryH\x00\x12\x1f\n\tstatement\x18\x03 \x01(\x0b\x32\n.StatementH\x00\x12\x17\n\x05query\x18\x04 \x01(\x0b\x32\x06.QueryH\x00\x12\x15\n\x04list\x18\x05 \x01(\x0b\x32\x05.ListH\x00\x12\x15\n\x04\x64ict\x18\x06 \x01(\x0b\x32\x05.DictH\x00\x12\x19\n\x06opcall\x18\x07 \x01(\x0b\x32\x07.OpCallH\x00\x12\x13\n\x03var\x18\x08 \x01(\x0b\x32\x04.VarH\x00\x12\x17\n\x05table\x18\t \x01(\x0b\x32\x06.TableH\x00\x42\x06\n\x04node\"=\n\x06\x42inary\x12\x12\n\x04left\x18\x01 \x01(\x0b\x32\x04.Ast\x12\x13\n\x05right\x18\x02 \x01(\x0b\x32\x04.Ast\x12\n\n\x02op\x18\x03 \x01(\t\"-\n\tStatement\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\x12\n\x04\x62ody\x18\x02 \x01(\x0b\x32\x04.Ast\"\'\n\x05Query\x12\x1e\n\nstatements\x18\x01 \x03(\x0b\x32\n.Statement\"4\n\x04\x41tom\x12\x1c\n\tatom_type\x18\x01 \x01(\x0e\x32\t.AtomType\x12\x0e\n\x06lexeme\x18\x02 \x01(\t\"\x1b\n\x04List\x12\x13\n\x05\x65lems\x18\x01 \x03(\x0b\x32\x04.Ast\"^\n\x04\x44ict\x12!\n\x06values\x18\x01 \x03(\x0b\x32\x11.Dict.ValuesEntry\x1a\x33\n\x0bValuesEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\x13\n\x05value\x18\x02 \x01(\x0b\x32\x04.Ast:\x02\x38\x01\"W\n\x06OpCall\x12\x15\n\x07operand\x18\x01 \x01(\x0b\x32\x04.Ast\x12\x11\n\tnamespace\x18\x02 \x01(\t\x12\x0c\n\x04name\x18\x03 \x01(\t\x12\x15\n\x06kwargs\x18\x04 \x01(\x0b\x32\x05.Dict\"\x13\n\x03Var\x12\x0c\n\x04name\x18\x01 \x01(\t\"\x1c\n\x05Table\x12\x13\n\x05inner\x18\x01 \x01(\x0b\x32\x04.Ast*5\n\x08\x41tomType\x12\x07\n\x03INT\x10\x00\x12\n\n\x06STRING\x10\x01\x12\x08\n\x04\x42OOL\x10\x02\x12\n\n\x06\x44OUBLE\x10\x03\x42\x0cZ\nengine/astb\x06proto3')
)

_ATOMTYPE = _descriptor.EnumDescriptor(
  name='AtomType',
  full_name='AtomType',
  filename=None,
  file=DESCRIPTOR,
  values=[
    _descriptor.EnumValueDescriptor(
      name='INT', index=0, number=0,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='STRING', index=1, number=1,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='BOOL', index=2, number=2,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='DOUBLE', index=3, number=3,
      serialized_options=None,
      type=None),
  ],
  containing_type=None,
  serialized_options=None,
  serialized_start=726,
  serialized_end=779,
)
_sym_db.RegisterEnumDescriptor(_ATOMTYPE)

AtomType = enum_type_wrapper.EnumTypeWrapper(_ATOMTYPE)
INT = 0
STRING = 1
BOOL = 2
DOUBLE = 3



_AST = _descriptor.Descriptor(
  name='Ast',
  full_name='Ast',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='atom', full_name='Ast.atom', index=0,
      number=1, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='binary', full_name='Ast.binary', index=1,
      number=2, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='statement', full_name='Ast.statement', index=2,
      number=3, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='query', full_name='Ast.query', index=3,
      number=4, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='list', full_name='Ast.list', index=4,
      number=5, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='dict', full_name='Ast.dict', index=5,
      number=6, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='opcall', full_name='Ast.opcall', index=6,
      number=7, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='var', full_name='Ast.var', index=7,
      number=8, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='table', full_name='Ast.table', index=8,
      number=9, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
    _descriptor.OneofDescriptor(
      name='node', full_name='Ast.node',
      index=0, containing_type=None, fields=[]),
  ],
  serialized_start=14,
  serialized_end=254,
)


_BINARY = _descriptor.Descriptor(
  name='Binary',
  full_name='Binary',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='left', full_name='Binary.left', index=0,
      number=1, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='right', full_name='Binary.right', index=1,
      number=2, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='op', full_name='Binary.op', index=2,
      number=3, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=256,
  serialized_end=317,
)


_STATEMENT = _descriptor.Descriptor(
  name='Statement',
  full_name='Statement',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='name', full_name='Statement.name', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='body', full_name='Statement.body', index=1,
      number=2, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=319,
  serialized_end=364,
)


_QUERY = _descriptor.Descriptor(
  name='Query',
  full_name='Query',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='statements', full_name='Query.statements', index=0,
      number=1, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=366,
  serialized_end=405,
)


_ATOM = _descriptor.Descriptor(
  name='Atom',
  full_name='Atom',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='atom_type', full_name='Atom.atom_type', index=0,
      number=1, type=14, cpp_type=8, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='lexeme', full_name='Atom.lexeme', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=407,
  serialized_end=459,
)


_LIST = _descriptor.Descriptor(
  name='List',
  full_name='List',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='elems', full_name='List.elems', index=0,
      number=1, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=461,
  serialized_end=488,
)


_DICT_VALUESENTRY = _descriptor.Descriptor(
  name='ValuesEntry',
  full_name='Dict.ValuesEntry',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='key', full_name='Dict.ValuesEntry.key', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='value', full_name='Dict.ValuesEntry.value', index=1,
      number=2, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=_b('8\001'),
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=533,
  serialized_end=584,
)

_DICT = _descriptor.Descriptor(
  name='Dict',
  full_name='Dict',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='values', full_name='Dict.values', index=0,
      number=1, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[_DICT_VALUESENTRY, ],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=490,
  serialized_end=584,
)


_OPCALL = _descriptor.Descriptor(
  name='OpCall',
  full_name='OpCall',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='operand', full_name='OpCall.operand', index=0,
      number=1, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='namespace', full_name='OpCall.namespace', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='name', full_name='OpCall.name', index=2,
      number=3, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='kwargs', full_name='OpCall.kwargs', index=3,
      number=4, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=586,
  serialized_end=673,
)


_VAR = _descriptor.Descriptor(
  name='Var',
  full_name='Var',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='name', full_name='Var.name', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=675,
  serialized_end=694,
)


_TABLE = _descriptor.Descriptor(
  name='Table',
  full_name='Table',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='inner', full_name='Table.inner', index=0,
      number=1, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=696,
  serialized_end=724,
)

_AST.fields_by_name['atom'].message_type = _ATOM
_AST.fields_by_name['binary'].message_type = _BINARY
_AST.fields_by_name['statement'].message_type = _STATEMENT
_AST.fields_by_name['query'].message_type = _QUERY
_AST.fields_by_name['list'].message_type = _LIST
_AST.fields_by_name['dict'].message_type = _DICT
_AST.fields_by_name['opcall'].message_type = _OPCALL
_AST.fields_by_name['var'].message_type = _VAR
_AST.fields_by_name['table'].message_type = _TABLE
_AST.oneofs_by_name['node'].fields.append(
  _AST.fields_by_name['atom'])
_AST.fields_by_name['atom'].containing_oneof = _AST.oneofs_by_name['node']
_AST.oneofs_by_name['node'].fields.append(
  _AST.fields_by_name['binary'])
_AST.fields_by_name['binary'].containing_oneof = _AST.oneofs_by_name['node']
_AST.oneofs_by_name['node'].fields.append(
  _AST.fields_by_name['statement'])
_AST.fields_by_name['statement'].containing_oneof = _AST.oneofs_by_name['node']
_AST.oneofs_by_name['node'].fields.append(
  _AST.fields_by_name['query'])
_AST.fields_by_name['query'].containing_oneof = _AST.oneofs_by_name['node']
_AST.oneofs_by_name['node'].fields.append(
  _AST.fields_by_name['list'])
_AST.fields_by_name['list'].containing_oneof = _AST.oneofs_by_name['node']
_AST.oneofs_by_name['node'].fields.append(
  _AST.fields_by_name['dict'])
_AST.fields_by_name['dict'].containing_oneof = _AST.oneofs_by_name['node']
_AST.oneofs_by_name['node'].fields.append(
  _AST.fields_by_name['opcall'])
_AST.fields_by_name['opcall'].containing_oneof = _AST.oneofs_by_name['node']
_AST.oneofs_by_name['node'].fields.append(
  _AST.fields_by_name['var'])
_AST.fields_by_name['var'].containing_oneof = _AST.oneofs_by_name['node']
_AST.oneofs_by_name['node'].fields.append(
  _AST.fields_by_name['table'])
_AST.fields_by_name['table'].containing_oneof = _AST.oneofs_by_name['node']
_BINARY.fields_by_name['left'].message_type = _AST
_BINARY.fields_by_name['right'].message_type = _AST
_STATEMENT.fields_by_name['body'].message_type = _AST
_QUERY.fields_by_name['statements'].message_type = _STATEMENT
_ATOM.fields_by_name['atom_type'].enum_type = _ATOMTYPE
_LIST.fields_by_name['elems'].message_type = _AST
_DICT_VALUESENTRY.fields_by_name['value'].message_type = _AST
_DICT_VALUESENTRY.containing_type = _DICT
_DICT.fields_by_name['values'].message_type = _DICT_VALUESENTRY
_OPCALL.fields_by_name['operand'].message_type = _AST
_OPCALL.fields_by_name['kwargs'].message_type = _DICT
_TABLE.fields_by_name['inner'].message_type = _AST
DESCRIPTOR.message_types_by_name['Ast'] = _AST
DESCRIPTOR.message_types_by_name['Binary'] = _BINARY
DESCRIPTOR.message_types_by_name['Statement'] = _STATEMENT
DESCRIPTOR.message_types_by_name['Query'] = _QUERY
DESCRIPTOR.message_types_by_name['Atom'] = _ATOM
DESCRIPTOR.message_types_by_name['List'] = _LIST
DESCRIPTOR.message_types_by_name['Dict'] = _DICT
DESCRIPTOR.message_types_by_name['OpCall'] = _OPCALL
DESCRIPTOR.message_types_by_name['Var'] = _VAR
DESCRIPTOR.message_types_by_name['Table'] = _TABLE
DESCRIPTOR.enum_types_by_name['AtomType'] = _ATOMTYPE
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

Ast = _reflection.GeneratedProtocolMessageType('Ast', (_message.Message,), {
  'DESCRIPTOR' : _AST,
  '__module__' : 'ast_pb2'
  # @@protoc_insertion_point(class_scope:Ast)
  })
_sym_db.RegisterMessage(Ast)

Binary = _reflection.GeneratedProtocolMessageType('Binary', (_message.Message,), {
  'DESCRIPTOR' : _BINARY,
  '__module__' : 'ast_pb2'
  # @@protoc_insertion_point(class_scope:Binary)
  })
_sym_db.RegisterMessage(Binary)

Statement = _reflection.GeneratedProtocolMessageType('Statement', (_message.Message,), {
  'DESCRIPTOR' : _STATEMENT,
  '__module__' : 'ast_pb2'
  # @@protoc_insertion_point(class_scope:Statement)
  })
_sym_db.RegisterMessage(Statement)

Query = _reflection.GeneratedProtocolMessageType('Query', (_message.Message,), {
  'DESCRIPTOR' : _QUERY,
  '__module__' : 'ast_pb2'
  # @@protoc_insertion_point(class_scope:Query)
  })
_sym_db.RegisterMessage(Query)

Atom = _reflection.GeneratedProtocolMessageType('Atom', (_message.Message,), {
  'DESCRIPTOR' : _ATOM,
  '__module__' : 'ast_pb2'
  # @@protoc_insertion_point(class_scope:Atom)
  })
_sym_db.RegisterMessage(Atom)

List = _reflection.GeneratedProtocolMessageType('List', (_message.Message,), {
  'DESCRIPTOR' : _LIST,
  '__module__' : 'ast_pb2'
  # @@protoc_insertion_point(class_scope:List)
  })
_sym_db.RegisterMessage(List)

Dict = _reflection.GeneratedProtocolMessageType('Dict', (_message.Message,), {

  'ValuesEntry' : _reflection.GeneratedProtocolMessageType('ValuesEntry', (_message.Message,), {
    'DESCRIPTOR' : _DICT_VALUESENTRY,
    '__module__' : 'ast_pb2'
    # @@protoc_insertion_point(class_scope:Dict.ValuesEntry)
    })
  ,
  'DESCRIPTOR' : _DICT,
  '__module__' : 'ast_pb2'
  # @@protoc_insertion_point(class_scope:Dict)
  })
_sym_db.RegisterMessage(Dict)
_sym_db.RegisterMessage(Dict.ValuesEntry)

OpCall = _reflection.GeneratedProtocolMessageType('OpCall', (_message.Message,), {
  'DESCRIPTOR' : _OPCALL,
  '__module__' : 'ast_pb2'
  # @@protoc_insertion_point(class_scope:OpCall)
  })
_sym_db.RegisterMessage(OpCall)

Var = _reflection.GeneratedProtocolMessageType('Var', (_message.Message,), {
  'DESCRIPTOR' : _VAR,
  '__module__' : 'ast_pb2'
  # @@protoc_insertion_point(class_scope:Var)
  })
_sym_db.RegisterMessage(Var)

Table = _reflection.GeneratedProtocolMessageType('Table', (_message.Message,), {
  'DESCRIPTOR' : _TABLE,
  '__module__' : 'ast_pb2'
  # @@protoc_insertion_point(class_scope:Table)
  })
_sym_db.RegisterMessage(Table)


DESCRIPTOR._options = None
_DICT_VALUESENTRY._options = None
# @@protoc_insertion_point(module_scope)
