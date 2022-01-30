from models import action, value, profile, aggregate
from gen.ast_pb2 import Ast
import requests
from rql import Expr, Serializer

PORT = 2425
# TODO: how does client find out the URL?
URL = 'http://localhost'


class InvalidInput(Exception):
    pass


class Client(object):
    def __init__(self, url=URL, port=PORT):
        self.url = str(url)
        self.port = str(port)

    def query(self, query: Expr):
        if not isinstance(query, Expr):
            raise InvalidInput("query expected to be an RQL Expr but got '%s' instead" % query)
        ast = Serializer().serialize(query)
        ser = ast.SerializeToString()
        response = requests.post(self._query_url(), data=ser)
        # if response isn't 200, raise the exception
        if response.status_code != requests.codes.OK:
            response.raise_for_status()

        # now try to read the response and parse it into a value
        v = value.Value()
        v.ParseFromString(response.content)
        return v

    def set_profile(self, item: profile.ProfileItem):
        if not isinstance(item, profile.ProfileItem):
            raise InvalidInput('arg of set profile should be ProfileItem but got: %s' % item)
        errors = profile.validate(item)
        if len(errors) > 0:
            raise InvalidInput('invalid profile item: %s' % ', '.join(errors))

        ser = item.SerializeToString()
        response = requests.post(self._set_url(), data=ser)
        response.raise_for_status()

    def get_profile(self, item: profile.ProfileItem):
        if not isinstance(item, profile.ProfileItem):
            raise InvalidInput('arg of get profile should be ProfileItem but got: %s' % item)
        errors = profile.validate(item)
        if len(errors) > 0:
            raise InvalidInput('invalid profile item: %s' % ', '.join(errors))

        ser = item.SerializeToString()
        response = requests.post(self._get_url(), data=ser)
        if response.status_code != requests.codes.OK:
            response.raise_for_status()
        v = value.Value()
        v.ParseFromString(response.content)
        return v

    def get_profiles(self, pfr: profile.ProfileFetchRequest):
        if not isinstance(pfr, profile.ProfileFetchRequest):
            raise InvalidInput('fetch arg not a ProfileFetchRequest object: %s' % str(pfr))
        ser = pfr.SerializeToString()
        response = requests.post(self._get_profiles_url(), data=ser)
        if response.status_code != requests.codes.OK:
            response.raise_for_status()

        pl = profile.ProfileList()
        # TODO: this could raise proto.DecodeError?
        # TODO copied from function fetch(afr) in this code

        pl.ParseFromString(response.content)
        return profile.from_proto_profile_list(pl)

    def log(self, a: action.Action):
        if not isinstance(a, action.Action):
            raise InvalidInput('log arg not an action: %s' % str(a))
        errors = action.validate(a)
        if len(errors) > 0:
            raise InvalidInput('invalid action: %s' % ','.join(errors))

        ser = a.SerializeToString()
        response = requests.post(self._log_url(), data=ser)
        # if response isn't 200, raise the exception
        response.raise_for_status()

    def fetch(self, afr: action.ActionFetchRequest):
        if not isinstance(afr, action.ActionFetchRequest):
            raise InvalidInput('fetch arg not an ActionFetchRequest object: %s' % str(afr))
        ser = afr.SerializeToString()
        response = requests.post(self._fetch_url(), data=ser)
        # if response isn't 200, raise the exception
        if response.status_code != requests.codes.OK:
            response.raise_for_status()

        # now try to read the response and parse it into list of actions
        al = action.ActionList()
        # TODo: this could raise proto.DecodeError? How to handle it?
        al.ParseFromString(response.content)
        return action.from_proto_action_list(al)

    def aggregate_value(self, request: aggregate.GetAggValueRequest):
        if not isinstance(request, aggregate.GetAggValueRequest):
            raise InvalidInput('arg to aggregate_value must be GetAggValueRequest but instead got: %s' % str(request))
        errors = aggregate.validate_value_request(request)
        if len(errors) > 0:
            raise InvalidInput('invalid input: %s' % ', '.join(errors))
        ser = request.SerializeToString()
        response = requests.post(self._get_aggregate_value_url(), data=ser)
        # if response isn't 200, raise the exception
        if response.status_code != requests.codes.OK:
            response.raise_for_status()

        # now try to read the response and parse it into a single value
        v = value.Value()
        v.ParseFromString(response.content)
        return v

    def store_aggregate(self, agg_type: str, agg_name: str, query: Expr, options: aggregate.AggOptions):
        errors = aggregate.validate(agg_type, agg_name, query, options)
        if len(errors) > 0:
            raise InvalidInput('invalid input: %s' % ', '.join(errors))
        agg = aggregate.Aggregate()
        agg.agg_type = agg_type
        agg.agg_name = agg_name
        q = Serializer().serialize(query)
        agg.query.CopyFrom(q)
        agg.options.CopyFrom(options)
        ser = agg.SerializeToString()
        response = requests.post(self._store_aggregate_url(), data=ser)
        # if response isn't 200, raise the exception
        response.raise_for_status()

    def retrieve_aggregate(self, agg_type: str, agg_name: str) -> aggregate.Aggregate:
        errors = aggregate.validate_type_name(agg_type, agg_name)
        if len(errors) > 0:
            raise InvalidInput('invalid input: %s' % ', '.join(errors))
        req = aggregate.AggRequest()
        req.agg_type = agg_type
        req.agg_name = agg_name
        ser = req.SerializeToString()
        response = requests.post(self._retrieve_aggregate_url(), data=ser)
        # if response isn't 200, raise the exception
        if response.status_code != requests.codes.OK:
            response.raise_for_status()

        # parse aggregate from the response content
        ret = aggregate.Aggregate()
        ret.ParseFromString(response.content)
        return ret

    def _base_url(self):
        return self.url + ':' + self.port

    def _log_url(self):
        return self._base_url() + '/log'

    def _fetch_url(self):
        return self._base_url() + '/fetch'

    def _get_url(self):
        return self._base_url() + '/get'

    def _set_url(self):
        return self._base_url() + '/set'

    def _query_url(self):
        return self._base_url() + '/query'

    def _get_profiles_url(self):
        return self._base_url() + '/get_profiles'

    def _get_aggregate_value_url(self):
        return self._base_url() + '/aggregate_value'

    def _store_aggregate_url(self):
        return self._base_url() + '/store_aggregate'

    def _retrieve_aggregate_url(self):
        return self._base_url() + '/retrieve_aggregate'
