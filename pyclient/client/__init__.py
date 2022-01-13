import action
import requests

import counter
import profile
import value

PORT = 2425
# TODO: how does client find out the URL?
URL = 'http://localhost'


class InvalidInput(Exception):
    pass


class Client(object):
    def __init__(self, url=URL, port=PORT):
        self.url = str(url)
        self.port = str(port)

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

    def query(self, query):
        pass
