from gen.profile_pb2 import ProtoProfileItem as ProfileItem


def validate(pi: ProfileItem):
    errors = []
    if pi.Oid == 0:
        errors.append("oid can not be zero")
    if pi.OType == 0:
        errors.append("otype can not be zero")
    if len(pi.Key) == 0:
        errors.append("key can not be empty")

    return errors
