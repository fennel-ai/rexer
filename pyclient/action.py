from gen.action_pb2 import ProtoAction as Action
from gen.action_pb2 import ProtoActionList as ActionList
from gen.action_pb2 import ProtoActionFetchRequest as ActionFetchRequest


class InvalidInput(Exception):
    pass


def validate(a: Action):
    errors = []
    if a.ActorID == 0:
        errors.append('actorID can not be zero')
    if a.ActorType == 0:
        errors.append('actor type can not be zero')
    if a.TargetID == 0:
        errors.append('target ID can not be zero')
    if a.TargetType == 0:
        errors.append('target type can not be zero')
    if a.ActionType == 0:
        errors.append('action type can not be zero')
    if a.RequestID == 0:
        errors.append('request ID can not be zero')
    return errors
