# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: ast.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\tast.proto\"\xb9\x02\n\x03\x41st\x12\x15\n\x04\x61tom\x18\x01 \x01(\x0b\x32\x05.AtomH\x00\x12\x19\n\x06\x62inary\x18\x02 \x01(\x0b\x32\x07.BinaryH\x00\x12\x1f\n\tstatement\x18\x03 \x01(\x0b\x32\n.StatementH\x00\x12\x17\n\x05query\x18\x04 \x01(\x0b\x32\x06.QueryH\x00\x12\x15\n\x04list\x18\x05 \x01(\x0b\x32\x05.ListH\x00\x12\x15\n\x04\x64ict\x18\x06 \x01(\x0b\x32\x05.DictH\x00\x12\x19\n\x06opcall\x18\x07 \x01(\x0b\x32\x07.OpCallH\x00\x12\x13\n\x03var\x18\x08 \x01(\x0b\x32\x04.VarH\x00\x12\x17\n\x05table\x18\t \x01(\x0b\x32\x06.TableH\x00\x12\x11\n\x02\x61t\x18\n \x01(\x0b\x32\x03.AtH\x00\x12\x19\n\x06lookup\x18\x0b \x01(\x0b\x32\x07.LookupH\x00\x12\x19\n\x06ifelse\x18\x0c \x01(\x0b\x32\x07.IfElseH\x00\x42\x06\n\x04node\"=\n\x06\x42inary\x12\x12\n\x04left\x18\x01 \x01(\x0b\x32\x04.Ast\x12\x13\n\x05right\x18\x02 \x01(\x0b\x32\x04.Ast\x12\n\n\x02op\x18\x03 \x01(\t\"-\n\tStatement\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\x12\n\x04\x62ody\x18\x02 \x01(\x0b\x32\x04.Ast\"\'\n\x05Query\x12\x1e\n\nstatements\x18\x01 \x03(\x0b\x32\n.Statement\"R\n\x04\x41tom\x12\r\n\x03int\x18\x01 \x01(\x03H\x00\x12\x10\n\x06string\x18\x02 \x01(\tH\x00\x12\x0e\n\x04\x62ool\x18\x03 \x01(\x08H\x00\x12\x10\n\x06\x64ouble\x18\x04 \x01(\x01H\x00\x42\x07\n\x05inner\"\x1c\n\x04List\x12\x14\n\x06values\x18\x01 \x03(\x0b\x32\x04.Ast\"^\n\x04\x44ict\x12!\n\x06values\x18\x01 \x03(\x0b\x32\x11.Dict.ValuesEntry\x1a\x33\n\x0bValuesEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\x13\n\x05value\x18\x02 \x01(\x0b\x32\x04.Ast:\x02\x38\x01\"W\n\x06OpCall\x12\x15\n\x07operand\x18\x01 \x01(\x0b\x32\x04.Ast\x12\x11\n\tnamespace\x18\x02 \x01(\t\x12\x0c\n\x04name\x18\x03 \x01(\t\x12\x15\n\x06kwargs\x18\x04 \x01(\x0b\x32\x05.Dict\"\x13\n\x03Var\x12\x0c\n\x04name\x18\x01 \x01(\t\"\x1c\n\x05Table\x12\x13\n\x05inner\x18\x01 \x01(\x0b\x32\x04.Ast\"\x04\n\x02\x41t\",\n\x06Lookup\x12\x10\n\x02on\x18\x01 \x01(\x0b\x32\x04.Ast\x12\x10\n\x08property\x18\x02 \x01(\t\"O\n\x06IfElse\x12\x17\n\tcondition\x18\x01 \x01(\x0b\x32\x04.Ast\x12\x15\n\x07then_do\x18\x02 \x01(\x0b\x32\x04.Ast\x12\x15\n\x07\x65lse_do\x18\x03 \x01(\x0b\x32\x04.AstB\x19Z\x17\x66\x65nnel/engine/ast/protob\x06proto3')



_AST = DESCRIPTOR.message_types_by_name['Ast']
_BINARY = DESCRIPTOR.message_types_by_name['Binary']
_STATEMENT = DESCRIPTOR.message_types_by_name['Statement']
_QUERY = DESCRIPTOR.message_types_by_name['Query']
_ATOM = DESCRIPTOR.message_types_by_name['Atom']
_LIST = DESCRIPTOR.message_types_by_name['List']
_DICT = DESCRIPTOR.message_types_by_name['Dict']
_DICT_VALUESENTRY = _DICT.nested_types_by_name['ValuesEntry']
_OPCALL = DESCRIPTOR.message_types_by_name['OpCall']
_VAR = DESCRIPTOR.message_types_by_name['Var']
_TABLE = DESCRIPTOR.message_types_by_name['Table']
_AT = DESCRIPTOR.message_types_by_name['At']
_LOOKUP = DESCRIPTOR.message_types_by_name['Lookup']
_IFELSE = DESCRIPTOR.message_types_by_name['IfElse']
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

At = _reflection.GeneratedProtocolMessageType('At', (_message.Message,), {
  'DESCRIPTOR' : _AT,
  '__module__' : 'ast_pb2'
  # @@protoc_insertion_point(class_scope:At)
  })
_sym_db.RegisterMessage(At)

Lookup = _reflection.GeneratedProtocolMessageType('Lookup', (_message.Message,), {
  'DESCRIPTOR' : _LOOKUP,
  '__module__' : 'ast_pb2'
  # @@protoc_insertion_point(class_scope:Lookup)
  })
_sym_db.RegisterMessage(Lookup)

IfElse = _reflection.GeneratedProtocolMessageType('IfElse', (_message.Message,), {
  'DESCRIPTOR' : _IFELSE,
  '__module__' : 'ast_pb2'
  # @@protoc_insertion_point(class_scope:IfElse)
  })
_sym_db.RegisterMessage(IfElse)

if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  DESCRIPTOR._serialized_options = b'Z\027fennel/engine/ast/proto'
  _DICT_VALUESENTRY._options = None
  _DICT_VALUESENTRY._serialized_options = b'8\001'
  _AST._serialized_start=14
  _AST._serialized_end=327
  _BINARY._serialized_start=329
  _BINARY._serialized_end=390
  _STATEMENT._serialized_start=392
  _STATEMENT._serialized_end=437
  _QUERY._serialized_start=439
  _QUERY._serialized_end=478
  _ATOM._serialized_start=480
  _ATOM._serialized_end=562
  _LIST._serialized_start=564
  _LIST._serialized_end=592
  _DICT._serialized_start=594
  _DICT._serialized_end=688
  _DICT_VALUESENTRY._serialized_start=637
  _DICT_VALUESENTRY._serialized_end=688
  _OPCALL._serialized_start=690
  _OPCALL._serialized_end=777
  _VAR._serialized_start=779
  _VAR._serialized_end=798
  _TABLE._serialized_start=800
  _TABLE._serialized_end=828
  _AT._serialized_start=830
  _AT._serialized_end=834
  _LOOKUP._serialized_start=836
  _LOOKUP._serialized_end=880
  _IFELSE._serialized_start=882
  _IFELSE._serialized_end=961
# @@protoc_insertion_point(module_scope)
