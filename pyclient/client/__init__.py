from models import action, counter, value, profile, aggregate
from gen.ast_pb2 import Ast
import requests

PORT = 2425
# TODO: how does client find out the URL?
URL = 'http://localhost'


class InvalidInput(Exception):
    pass


class Client(object):
    def __init__(self, url=URL, port=PORT):
        self.url = str(url)
        self.port = str(port)

    def query(self, query: Ast):
        if not isinstance(query, Ast):
            raise InvalidInput('query is not a query Ast, did you forget to use query.query before calling this?')
        ser = query.SerializeToString()
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

    def count(self, request):
        if not isinstance(request, counter.GetCountRequest):
            raise InvalidInput('arg to count must be GetCountRequest but instead got: %s' % str(request))
        errors = counter.validate_count_request(request)
        if len(errors) > 0:
            raise InvalidInput('invalid input: %s' % ', '.join(errors))
        ser = request.SerializeToString()
        response = requests.post(self._count_url(), data=ser)
        # if response isn't 200, raise the exception
        if response.status_code != requests.codes.OK:
            response.raise_for_status()

        # now try to read the response and parse it into a single int
        # TODo: this could raise parseError, do we need special handling?
        count = int(response.content)
        return count

    def rate(self, request: counter.GetRateRequest):
        if not isinstance(request, counter.GetRateRequest):
            raise InvalidInput('arg to rate must be GetRateRequest but instead got: %s' % str(request))
        errors = counter.validate_rate_request(request)
        if len(errors) > 0:
            raise InvalidInput('invalid input: %s' % ', '.join(errors))
        ser = request.SerializeToString()
        response = requests.post(self._rate_url(), data=ser)
        # if response isn't 200, raise the exception
        if response.status_code != requests.codes.OK:
            response.raise_for_status()

        # now try to read the response and parse it into a single int
        # TODo: this could raise parseError, do we need special handling?
        rate = float(response.content)
        return rate

    def aggregate_value(self, request: aggregate.GetAggValueRequest):
        if not isinstance(request, aggregate.GetAggValueRequest):
            raise InvalidInput('arg to aggregate_value must be GetAggValueRequest but instead got: %s' % str(request))
        errors = aggregate.validate(request)
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

    def _base_url(self):
        return self.url + ':' + self.port

    def _log_url(self):
        return self._base_url() + '/log'

    def _count_url(self):
        return self._base_url() + '/count'

    def _rate_url(self):
        return self._base_url() + '/rate'

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
