# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: counter.proto

import sys
_b=sys.version_info[0]<3 and (lambda x:x) or (lambda x:x.encode('latin1'))
from google.protobuf.internal import enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


import types_pb2 as types__pb2


DESCRIPTOR = _descriptor.FileDescriptor(
  name='counter.proto',
  package='',
  syntax='proto3',
  serialized_options=_b('Z\010data/lib'),
  serialized_pb=_b('\n\rcounter.proto\x1a\x0btypes.proto\"r\n\x14ProtoGetCountRequest\x12!\n\x0b\x43ounterType\x18\x01 \x01(\x0e\x32\x0c.CounterType\x12\x17\n\x06Window\x18\x02 \x01(\x0e\x32\x07.Window\x12\x0b\n\x03Key\x18\x03 \x03(\x04\x12\x11\n\tTimestamp\x18\x04 \x01(\x04\"\xc1\x01\n\x13ProtoGetRateRequest\x12$\n\x0eNumCounterType\x18\x01 \x01(\x0e\x32\x0c.CounterType\x12$\n\x0e\x44\x65nCounterType\x18\x02 \x01(\x0e\x32\x0c.CounterType\x12\x0e\n\x06NumKey\x18\x03 \x03(\x04\x12\x0e\n\x06\x44\x65nKey\x18\x04 \x03(\x04\x12\x17\n\x06Window\x18\x05 \x01(\x0e\x32\x07.Window\x12\x11\n\tTimestamp\x18\x06 \x01(\x04\x12\x12\n\nLowerBound\x18\x07 \x01(\x08*e\n\x06Window\x12\x0f\n\x0bNULL_WINDOW\x10\x00\x12\x08\n\x04HOUR\x10\x01\x12\x07\n\x03\x44\x41Y\x10\x02\x12\x08\n\x04WEEK\x10\x03\x12\t\n\x05MONTH\x10\x04\x12\x0b\n\x07QUARTER\x10\x05\x12\x08\n\x04YEAR\x10\x06\x12\x0b\n\x07\x46OREVER\x10\x07\x42\nZ\x08\x64\x61ta/libb\x06proto3')
  ,
  dependencies=[types__pb2.DESCRIPTOR,])

_WINDOW = _descriptor.EnumDescriptor(
  name='Window',
  full_name='Window',
  filename=None,
  file=DESCRIPTOR,
  values=[
    _descriptor.EnumValueDescriptor(
      name='NULL_WINDOW', index=0, number=0,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='HOUR', index=1, number=1,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='DAY', index=2, number=2,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='WEEK', index=3, number=3,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='MONTH', index=4, number=4,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='QUARTER', index=5, number=5,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='YEAR', index=6, number=6,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='FOREVER', index=7, number=7,
      serialized_options=None,
      type=None),
  ],
  containing_type=None,
  serialized_options=None,
  serialized_start=342,
  serialized_end=443,
)
_sym_db.RegisterEnumDescriptor(_WINDOW)

Window = enum_type_wrapper.EnumTypeWrapper(_WINDOW)
NULL_WINDOW = 0
HOUR = 1
DAY = 2
WEEK = 3
MONTH = 4
QUARTER = 5
YEAR = 6
FOREVER = 7



_PROTOGETCOUNTREQUEST = _descriptor.Descriptor(
  name='ProtoGetCountRequest',
  full_name='ProtoGetCountRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='CounterType', full_name='ProtoGetCountRequest.CounterType', index=0,
      number=1, type=14, cpp_type=8, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='Window', full_name='ProtoGetCountRequest.Window', index=1,
      number=2, type=14, cpp_type=8, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='Key', full_name='ProtoGetCountRequest.Key', index=2,
      number=3, type=4, cpp_type=4, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='Timestamp', full_name='ProtoGetCountRequest.Timestamp', index=3,
      number=4, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
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
  serialized_start=30,
  serialized_end=144,
)


_PROTOGETRATEREQUEST = _descriptor.Descriptor(
  name='ProtoGetRateRequest',
  full_name='ProtoGetRateRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='NumCounterType', full_name='ProtoGetRateRequest.NumCounterType', index=0,
      number=1, type=14, cpp_type=8, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='DenCounterType', full_name='ProtoGetRateRequest.DenCounterType', index=1,
      number=2, type=14, cpp_type=8, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='NumKey', full_name='ProtoGetRateRequest.NumKey', index=2,
      number=3, type=4, cpp_type=4, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='DenKey', full_name='ProtoGetRateRequest.DenKey', index=3,
      number=4, type=4, cpp_type=4, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='Window', full_name='ProtoGetRateRequest.Window', index=4,
      number=5, type=14, cpp_type=8, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='Timestamp', full_name='ProtoGetRateRequest.Timestamp', index=5,
      number=6, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='LowerBound', full_name='ProtoGetRateRequest.LowerBound', index=6,
      number=7, type=8, cpp_type=7, label=1,
      has_default_value=False, default_value=False,
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
  serialized_start=147,
  serialized_end=340,
)

_PROTOGETCOUNTREQUEST.fields_by_name['CounterType'].enum_type = types__pb2._COUNTERTYPE
_PROTOGETCOUNTREQUEST.fields_by_name['Window'].enum_type = _WINDOW
_PROTOGETRATEREQUEST.fields_by_name['NumCounterType'].enum_type = types__pb2._COUNTERTYPE
_PROTOGETRATEREQUEST.fields_by_name['DenCounterType'].enum_type = types__pb2._COUNTERTYPE
_PROTOGETRATEREQUEST.fields_by_name['Window'].enum_type = _WINDOW
DESCRIPTOR.message_types_by_name['ProtoGetCountRequest'] = _PROTOGETCOUNTREQUEST
DESCRIPTOR.message_types_by_name['ProtoGetRateRequest'] = _PROTOGETRATEREQUEST
DESCRIPTOR.enum_types_by_name['Window'] = _WINDOW
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

ProtoGetCountRequest = _reflection.GeneratedProtocolMessageType('ProtoGetCountRequest', (_message.Message,), {
  'DESCRIPTOR' : _PROTOGETCOUNTREQUEST,
  '__module__' : 'counter_pb2'
  # @@protoc_insertion_point(class_scope:ProtoGetCountRequest)
  })
_sym_db.RegisterMessage(ProtoGetCountRequest)

ProtoGetRateRequest = _reflection.GeneratedProtocolMessageType('ProtoGetRateRequest', (_message.Message,), {
  'DESCRIPTOR' : _PROTOGETRATEREQUEST,
  '__module__' : 'counter_pb2'
  # @@protoc_insertion_point(class_scope:ProtoGetRateRequest)
  })
_sym_db.RegisterMessage(ProtoGetRateRequest)


DESCRIPTOR._options = None
# @@protoc_insertion_point(module_scope)
