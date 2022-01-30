from gen.aggregate_pb2 import ProtoGetAggValueRequest as GetAggValueRequest
from gen.aggregate_pb2 import ProtoAggregate as Aggregate
from gen.aggregate_pb2 import AggRequest, AggOptions
from models import value
from gen.ast_pb2 import Ast
from rql import Expr


def validate_value_request(req: GetAggValueRequest):
    errors = validate_type_name(req.agg_type, req.agg_name)
    if not value.is_valid(req.key):
        errors.append("key is not a valid value")
    return errors


def validate_type_name(agg_type, agg_name):
    errors = []
    if len(agg_type) == 0:
        errors.append("aggregate type can not be of zero length")
    if len(agg_name) == 0:
        errors.append("aggregate name can not be of zero length")
    return errors


def validate(agg_type: str, agg_name: str, query: Ast, options: AggOptions):
    errors = validate_type_name(agg_type, agg_name)
    if not isinstance(query, Expr):
        errors.append(
            "query expected to be an RQL expression but got: '%s' instead" % query
        )
    if not options.IsInitialized():
        errors.append("aggregate options not initialized")
    return errors
