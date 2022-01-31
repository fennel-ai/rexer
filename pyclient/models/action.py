import client
from gen.action_pb2 import ProtoAction as Action
from gen.action_pb2 import ProtoActionList as ActionList
from gen.action_pb2 import ProtoActionFetchRequest as ActionFetchRequest


class InvalidInput(Exception):
    pass


def validate(a: Action):
    errors = []
    if a.ActorID == 0:
        errors.append('actorID can not be zero')
    if len(a.ActorType) == 0:
        errors.append('actor type can not be empty')
    if a.TargetID == 0:
        errors.append('target ID can not be zero')
    if len(a.TargetType) == 0:
        errors.append('target type can not be empty')
    if len(a.ActionType) == 0:
        errors.append('action type can not be empty')
    if a.RequestID == 0:
        errors.append('request ID can not be zero')
    return errors


def from_proto_action_list(al):
    """Takes a valid action list and returns a list of actions"""
    ret = []
    for a in al.actions:
        ret.append(a)
    return ret
    # return al.actions


def to_proto_action_list(actions):
    """Takes a list of actions and returns an actionlist"""
    if not isinstance(actions, list):
        raise client.InvalidInput('actions not a list but instead: %s' % str(actions))
    al = ActionList()
    for a in actions:
        if not isinstance(a, Action):
            raise client.InvalidInput('members of actions not action but instead: %s' % str(a))

        al.actions.append(a)
    return al
