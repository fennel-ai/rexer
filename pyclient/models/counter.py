from gen.counter_pb2 import ProtoGetCountRequest as GetCountRequest
from gen.counter_pb2 import ProtoGetRateRequest as GetRateRequest
from gen.counter_pb2 import CounterType
from gen.ftypes_pb2 import Window


def validate_count_request(r: GetCountRequest):
    errors = []
    if r.CounterType == 0:
        errors.append("counter type can not be zero")
    if r.Window == 0:
        errors.append("counter window can not be zero")
    if len(r.Key) == 0:
        errors.append("counter key can not be empty")
    return errors


def validate_rate_request(r: GetRateRequest):
    errors = []
    if r.NumCounterType == 0:
        errors.append("num counter type can not be zero")
    if r.DenCounterType == 0:
        errors.append("den counter type can not be zero")
    if len(r.NumKey) == 0:
        errors.append("num counter key can not be empty")
    if len(r.DenKey) == 0:
        errors.append("den counter key can not be empty")
    if r.Window == 0:
        errors.append("counter window can not be zero")
    return errors
