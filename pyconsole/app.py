import requests
from absl import flags
from flask import Flask, request, jsonify
from google.protobuf import json_format

from rexerclient import client, value
from rexerclient.models import action, profile

app = Flask('console')

# Flags:
endpoint_flag = flags.DEFINE_string("endpoint", "http://localhost:2425", "URL for the data-plane API end-point")

def build_app(**kwargs):
    global c, go_url
    if endpoint_flag.name in kwargs:
        go_url = kwargs.get(endpoint_flag.name)
        c = client.Client(kwargs.get(endpoint_flag.name))
    else:
        go_url = endpoint_flag.default
        c = client.Client(endpoint_flag.default)
    return app


def is_uint(s, size=32):
    try:
        n = int(s)
        return 0 <= n < (2 ** size)
    except:
        return False

def is_int(s, size=32):
    try:
        n = int(s)
        return -(2 ** (size - 1)) <= n < (2 ** (size - 1))
    except:
        return False

def is_str(s):
    if isinstance(s, str):
        return True


def _validate_profile_get(otype, oid, key, version):
    errors = []

    if otype is None:
        errors.append('otype is not specified')
    elif not is_str(otype):
        errors.append('otype is not a valid string')
    elif len(otype) == 0:
        errors.append('otype is not a non-empty string')
    elif len(otype) > 256:
        errors.append('otype is longer than 256 characters')

    if oid is None:
        errors.append('oid is not specified')
    elif not is_uint(oid, 64):
        errors.append('oid is not a valid 64-bit unsigned integer')

    if key is not None:
        if not is_str(key):
            errors.append('key is provided but is not a valid string')
        elif len(key) == 0:
            errors.append('key is provided but is not a non-empty string')
        elif len(key) > 256:
            errors.append('key is provided but is longer than 256 characters')
    if version is not None and not is_uint(version, 64):
        errors.append('version is provided but is not a valid 64-bit unsigned integer')

    return errors


def _to_int(s, default=0):
    return int(s) if s is not None else default

def _to_str(s, default=''):
    return str(s) if s is not None else default


def _to_profile_item(otype, oid, key, version):
    ret = profile.ProfileItem()
    ret.OType = str(otype)
    ret.Oid = int(oid)
    ret.Key = key if key is not None else ""
    ret.Version = _to_int(version)
    ret.Value.CopyFrom(value.Nil())
    return ret


@app.route('/profile/', methods=['GET'])
def profile_handler():
    args = request.args
    oid = args.get('oid', None)
    otype = args.get('otype', None)
    key = args.get('key', None)
    version = args.get('version', None)
    errors = _validate_profile_get(otype, oid, key, version)
    if len(errors) > 0:
        app.logger.error(request, errors)
        return jsonify({'errors': errors}), 400
    req = _to_profile_item(otype, oid, key, version)
    # TODO: client's get_profile returns a single value but
    # we need a list of all relevant profile rows here
    ser = req.SerializeToString()
    response = requests.post(go_url+'/get', data=ser)
    if response.status_code != requests.codes.OK:
        response.raise_for_status()
    v = value.Value()
    v.ParseFromString(response.content)
    return json_format.MessageToJson(v)


def _validate_action_get(actor_id, actor_type, target_id, target_type, action_type,
                         min_timestamp, max_timestamp, min_action_id, max_action_id, request_id):
    errors = []
    if (actor_id is not None) and (not is_uint(actor_id, 64)):
        errors.append('actor_id is provided but is not a valid 64-bit unsigned integer')
    if (target_id is not None) and (not is_uint(target_id, 64)):
        errors.append('target_id is provided but is not a valid 64-bit unsigned integer')
    if (min_action_id is not None) and (not is_uint(min_action_id, 64)):
        errors.append('min_action_id is provided but is not a valid 64-bit unsigned integer')
    if (max_action_id is not None) and (not is_uint(max_action_id, 64)):
        errors.append('max_action_id is provided but is not a valid 64-bit unsigned integer')
    if (min_timestamp is not None) and (not is_uint(min_timestamp, 64)):
        errors.append('min_timestamp is provided but is not a valid 64-bit unsigned integer')
    if (max_timestamp is not None) and (not is_uint(max_timestamp, 64)):
        errors.append('max_timestamp is provided but is not a valid 64-bit unsigned integer')
    if (request_id is not None) and (not is_uint(request_id, 64)):
        errors.append('request_id is provided but is not a valid 64-bit unsigned integer')
    if (actor_type is not None):
        if not is_str(actor_type):
            errors.append('actor_type is provided but is not a valid non-empty string')
        elif len(actor_type) > 256:
            errors.append('actor_type is provided but is longer than 256 chars')
    if (target_type is not None):
        if not is_str(target_type):
            errors.append('target_type is provided but is not a valid non-empty string')
        elif len(target_type) > 256:
            errors.append('target_type is provided but is longer than 256 chars')
    if (action_type is not None):
        if not is_str(action_type):
            errors.append('action_type is provided but is not a valid non-empty string')
        elif len(action_type) > 256:
            errors.append('action_type is provided but is longer than 256 chars')

    return errors


def _to_action_fetch_request(actor_id, actor_type, target_id, target_type, action_type,
                             min_timestamp, max_timestamp, min_action_id, max_action_id, request_id):
    ret = action.ActionFetchRequest()
    ret.ActorID = _to_int(actor_id)
    ret.ActorType = _to_str(actor_type)
    ret.TargetID = _to_int(target_id)
    ret.TargetType = _to_str(target_type)
    ret.ActionType = _to_str(action_type)
    ret.MinTimestamp = _to_int(min_timestamp)
    ret.MaxTimestamp = _to_int(max_timestamp)
    ret.MinActionID = _to_int(min_action_id)
    ret.MaxActionID = _to_int(max_action_id)
    ret.RequestID = _to_int(request_id)
    return ret


@app.route('/actions/', methods=['GET'])
def action_handler():
    args = request.args
    actor_id = args.get('actor_id', None)
    target_id = args.get('target_id', None)
    actor_type = args.get('actor_type', None)
    target_type = args.get('target_type', None)
    action_type = args.get('action_type', None)
    request_id = args.get('request_id', None)
    min_action_id = args.get('min_action_id', None)
    max_action_id = args.get('max_action_id', None)
    min_timestamp = args.get('min_timestamp', None)
    max_timestamp = args.get('max_timestamp', None)
    errors = _validate_action_get(actor_id, actor_type, target_id, target_type, action_type,
                                  min_timestamp, max_timestamp, min_action_id, max_action_id, request_id)
    if len(errors) > 0:
        return jsonify({'errors': errors}), 400
    
    req = _to_action_fetch_request(actor_id, actor_type, target_id, target_type, action_type,
                                   min_timestamp, max_timestamp, min_action_id, max_action_id, request_id)
    ser = req.SerializeToString()
    response = requests.post(go_url+'/fetch', data=ser)
    if response.status_code != requests.codes.OK:
        response.raise_for_status()
    al = action.ActionList()
    al.ParseFromString(response.content)
    actions = action.from_proto_action_list(al)
    strs = []
    for a in actions:
        strs.append(json_format.MessageToJson(a, including_default_value_fields=True))
    return '[' + ', '.join(strs) + ']'

def _validate_profile_get_multi(otype, oid, key, version):
    errors = []
    if (otype is not None) and (not is_str(otype)):
        errors.append('otype is provided but is not a valid string')
    if (oid is not None) and (not is_uint(oid, 64)):
        errors.append('oid is provided but is not a valid 64-bit unsigned integer')
    if (key is not None) and (not is_str(key)):
        errors.append('key is provided but is not a valid string')
    if (version is not None) and (not is_uint(version, 64)):
        errors.append('version is provided but is not a valid 64-bit unsigned integer')
    
    return errors

def _to_profile_fetch_request(otype, oid, key, version):
    ret = profile.ProfileFetchRequest()
    ret.OType = _to_str(otype)
    ret.Oid = _to_int(oid)
    ret.Key = _to_str(key)
    ret.Version = _to_int(version)
    return ret

@app.route('/profile_multi/', methods=['GET'])
def profile_multi_handler():
    args = request.args
    otype = args.get('otype', None)
    oid = args.get('oid', None)
    key = args.get('key', None)
    version = args.get('version', None)
    errors = _validate_profile_get_multi(otype, oid, key, version)
    if len(errors) > 0:
        return jsonify({'errors': errors}), 400
    req = _to_profile_fetch_request(otype, oid, key, version)
    ser = req.SerializeToString()
    response = requests.post(go_url+'/get_multi', data=ser)
    if response.status_code != requests.codes.OK:
        response.raise_for_status()
    pl = profile.ProfileList()
    pl.ParseFromString(response.content)
    profiles = profile.from_proto_profile_list(pl)
    strs = []
    for p in profiles:
        strs.append(json_format.MessageToJson(p, including_default_value_fields=True))
    return '[' + ', '.join(strs) + ']'


go_url = endpoint_flag.default
c = client.Client(endpoint_flag.default)
app.run(host="localhost", port="2475")
