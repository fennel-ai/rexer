# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: action.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x0c\x61\x63tion.proto\"\xa3\x02\n\x0bProtoAction\x12\x1b\n\x08\x41\x63tionID\x18\x01 \x01(\x04R\taction_id\x12\x19\n\x07\x41\x63torID\x18\x02 \x01(\x04R\x08\x61\x63tor_id\x12\x1d\n\tActorType\x18\x03 \x01(\rR\nactor_type\x12\x1b\n\x08TargetID\x18\x04 \x01(\x04R\ttarget_id\x12\x1f\n\nTargetType\x18\x05 \x01(\rR\x0btarget_type\x12\x1f\n\nActionType\x18\x06 \x01(\rR\x0b\x61\x63tion_type\x12!\n\x0b\x41\x63tionValue\x18\x07 \x01(\x05R\x0c\x61\x63tion_value\x12\x1c\n\tTimestamp\x18\x08 \x01(\x04R\ttimestamp\x12\x1d\n\tRequestID\x18\t \x01(\x04R\nrequest_id\"\xb7\x03\n\x17ProtoActionFetchRequest\x12\"\n\x0bMinActionID\x18\x01 \x01(\x04R\rmin_action_id\x12\"\n\x0bMaxActionID\x18\x02 \x01(\x04R\rmax_action_id\x12\x19\n\x07\x41\x63torID\x18\x03 \x01(\x04R\x08\x61\x63tor_id\x12\x1d\n\tActorType\x18\x04 \x01(\rR\nactor_type\x12\x1b\n\x08TargetID\x18\x05 \x01(\x04R\ttarget_id\x12\x1f\n\nTargetType\x18\x06 \x01(\rR\x0btarget_type\x12\x1f\n\nActionType\x18\x07 \x01(\rR\x0b\x61\x63tion_type\x12(\n\x0eMinActionValue\x18\x08 \x01(\x05R\x10min_action_value\x12(\n\x0eMaxActionValue\x18\t \x01(\x05R\x10max_action_value\x12#\n\x0cMinTimestamp\x18\n \x01(\x04R\rmin_timestamp\x12#\n\x0cMaxTimestamp\x18\x0b \x01(\x04R\rmax_timestamp\x12\x1d\n\tRequestID\x18\x0c \x01(\x04R\nrequest_id\"0\n\x0fProtoActionList\x12\x1d\n\x07\x61\x63tions\x18\x01 \x03(\x0b\x32\x0c.ProtoActionB\x11Z\x0f\x66\x65nnel/data/libb\x06proto3')



_PROTOACTION = DESCRIPTOR.message_types_by_name['ProtoAction']
_PROTOACTIONFETCHREQUEST = DESCRIPTOR.message_types_by_name['ProtoActionFetchRequest']
_PROTOACTIONLIST = DESCRIPTOR.message_types_by_name['ProtoActionList']
ProtoAction = _reflection.GeneratedProtocolMessageType('ProtoAction', (_message.Message,), {
  'DESCRIPTOR' : _PROTOACTION,
  '__module__' : 'action_pb2'
  # @@protoc_insertion_point(class_scope:ProtoAction)
  })
_sym_db.RegisterMessage(ProtoAction)

ProtoActionFetchRequest = _reflection.GeneratedProtocolMessageType('ProtoActionFetchRequest', (_message.Message,), {
  'DESCRIPTOR' : _PROTOACTIONFETCHREQUEST,
  '__module__' : 'action_pb2'
  # @@protoc_insertion_point(class_scope:ProtoActionFetchRequest)
  })
_sym_db.RegisterMessage(ProtoActionFetchRequest)

ProtoActionList = _reflection.GeneratedProtocolMessageType('ProtoActionList', (_message.Message,), {
  'DESCRIPTOR' : _PROTOACTIONLIST,
  '__module__' : 'action_pb2'
  # @@protoc_insertion_point(class_scope:ProtoActionList)
  })
_sym_db.RegisterMessage(ProtoActionList)

if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  DESCRIPTOR._serialized_options = b'Z\017fennel/data/lib'
  _PROTOACTION._serialized_start=17
  _PROTOACTION._serialized_end=308
  _PROTOACTIONFETCHREQUEST._serialized_start=311
  _PROTOACTIONFETCHREQUEST._serialized_end=750
  _PROTOACTIONLIST._serialized_start=752
  _PROTOACTIONLIST._serialized_end=800
# @@protoc_insertion_point(module_scope)
