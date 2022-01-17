from flask import Flask, request, jsonify
from google.protobuf import json_format

import client
from models import action, value, profile

HOST = 'localhost'
PORT = 2475

c = client.Client()
app = Flask('console')


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


def _validate_profile_get(otype, oid, key, version):
    errors = []
    if otype is None:
        errors.append('otype is not specified')
    elif not is_uint(otype, 32):
        errors.append('otype is not a valid 32-bit unsigned integer')

    if oid is None:
        errors.append('oid is not specified')
    elif not is_uint(oid, 64):
        errors.append('oid is not a valid 64-bit unsigned integer')

    if key is not None and len(key) == 0:
        errors.append('key is provided but is not a valid non-empty string')
    if version is not None and not is_uint(version, 64):
        errors.append('version is provided but is not a valid 64-bit unsigned integer')

    return errors


def _to_int(s, default=0):
    return int(s) if s is not None else default


def _to_profile_item(otype, oid, key, version):
    ret = profile.ProfileItem()
    ret.OType = int(otype)
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
    v = c.get_profile(req)
    return json_format.MessageToJson(v)


def _validate_action_get(actor_id, actor_type, target_id, target_type, action_type,
                         min_action_value, max_action_value, min_timestamp, max_timestamp,
                         min_action_id, max_action_id, request_id):
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
    if (actor_type is not None) and (not is_uint(actor_type, 32)):
        errors.append('actor_type is provided but is not a valid 32-bit unsigned integer')
    if (target_type is not None) and (not is_uint(target_type, 32)):
        errors.append('target_type is provided but is not a valid 32-bit unsigned integer')
    if (action_type is not None) and (not is_uint(action_type, 32)):
        errors.append('action_type is provided but is not a valid 32-bit unsigned integer')
    if (min_action_value is not None) and (not is_int(min_action_value, 32)):
        errors.append('min_action_value is provided but is not a valid 32-bit signed integer')
    if (max_action_value is not None) and (not is_int(max_action_value, 32)):
        errors.append('max_action_value is provided but is not a valid 32-bit signed integer')

    return errors


def _to_action_fetch_request(actor_id, actor_type, target_id, target_type, action_type,
                             min_action_value, max_action_value, min_timestamp, max_timestamp,
                             min_action_id, max_action_id, request_id):
    ret = action.ActionFetchRequest()
    ret.ActorID = _to_int(actor_id)
    ret.ActorType = _to_int(actor_type)
    ret.TargetID = _to_int(target_id)
    ret.TargetType = _to_int(target_type)
    ret.ActionType = _to_int(action_type)
    ret.MinActionValue = _to_int(min_action_value)
    ret.MaxActionValue = _to_int(max_action_value)
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
    min_action_value = args.get('min_action_value', None)
    max_action_value = args.get('max_action_value', None)
    min_timestamp = args.get('min_timestamp', None)
    max_timestamp = args.get('max_timestamp', None)
    errors = _validate_action_get(actor_id, actor_type, target_id, target_type, action_type,
                                  min_action_value, max_action_value, min_timestamp, max_timestamp,
                                  min_action_id, max_action_id, request_id)
    if len(errors) > 0:
        return jsonify({'errors': errors}), 400
    req = _to_action_fetch_request(actor_id, actor_type, target_id, target_type, action_type,
                                   min_action_value, max_action_value, min_timestamp, max_timestamp,
                                   min_action_id, max_action_id, request_id)
    actions = c.fetch(req)
    strs = []
    for a in actions:
        strs.append(json_format.MessageToJson(a, including_default_value_fields=True))
    return '[' + ', '.join(strs) + ']'


if __name__ == '__main__':
    app.run(host=HOST, port=PORT)
