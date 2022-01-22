import client
from gen.profile_pb2 import ProtoProfileItem as ProfileItem
from gen.profile_pb2 import ProtoProfileList as ProfileList
from gen.profile_pb2 import ProtoProfileFetchRequest as ProfileFetchRequest


def validate(pi: ProfileItem):
    errors = []
    if pi.Oid == 0:
        errors.append("oid can not be zero")
    if pi.OType == 0:
        errors.append("otype can not be zero")
    if len(pi.Key) == 0:
        errors.append("key can not be empty")

    return errors

def from_proto_profile_list(pl):
    """Takes a valid profile list and returns a list of profiles"""
    ret = []
    for p in pl.profiles:
        ret.append(p)
    return ret

def to_proto_profile_list(profiles):
    """Takes a list of profiles and returns a profilelist"""
    if not isinstance(profiles, list):
        raise client.InvalidInput('profiles not a list but instead: %s' % str(profiles))
    pl = ProfileList()
    for p in profiles:
        if not isinstance(p, ProfileItem):
            raise client.InvalidInput('members of profiles not profile but instead: %s' % str(p))
        
        pl.profiles.append(p)
    return pl