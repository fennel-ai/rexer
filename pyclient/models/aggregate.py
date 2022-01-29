from gen.aggregate_pb2 import ProtoGetAggValueRequest as GetAggValueRequest
from models  import value

def validate(req: GetAggValueRequest):
    errors = []
    if len(req.agg_type) == 0:
        errors.append("aggregate type can not be of zero length")
    if len(req.agg_name) == 0:
        errors.append("aggregate name can not be of zero length")
    if not value.is_valid(req.key):
        errors.append("key is not a valid value")
    return errors
