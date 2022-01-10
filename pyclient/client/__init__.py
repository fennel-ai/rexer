import action
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

    def fetch(self, ar: action.ActionFetchRequest):
        pass

    def count(self, request):
        pass

    def _base_url(self):
        return self.url + ':' + self.port

    def _log_url(self):
        return self._base_url() + '/log'

    def rate(self, request):
        pass

    def set_profile(self, target_type, target_id, key, value, version):
        pass

    def get_profile(self, target_type, target_id, key, version=None):
        pass

    def query(self, query):
        pass
