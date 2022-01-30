import unittest
import httpretty
import requests

import query
from models import action, counter, value, profile, aggregate
import client


class Testclient(unittest.TestCase):
    @httpretty.activate(verbose=True, allow_net_connect=False)
    def test_get_set_profile(self):
        c = client.Client()

        # invalid requests throw exception for both set & get
        with self.assertRaises(client.InvalidInput):
            c.get_profile(1)
        with self.assertRaises(client.InvalidInput):
            c.set_profile(1)
        with self.assertRaises(client.InvalidInput):
            c.get_profile('hi')
        with self.assertRaises(client.InvalidInput):
            c.set_profile('hi')
        with self.assertRaises(client.InvalidInput):
            c.get_profile(profile.ProfileItem())
        with self.assertRaises(client.InvalidInput):
            c.set_profile(profile.ProfileItem())
        with self.assertRaises(client.InvalidInput):
            c.get_profiles(1)
        with self.assertRaises(client.InvalidInput):
            c.get_profiles('hi')
        with self.assertRaises(client.InvalidInput):
            c.get_profiles(profile.ProfileItem)

        # but valid requests don't throw exceptions

        # Test set for valid profiles
        httpretty.register_uri(httpretty.POST, 'http://localhost:2425/set')

        p1 = profile.ProfileItem()
        p1.CustID = 1
        p1.Oid, p1.OType = 1, '2'
        p1.Key = 'key'
        v = value.Int(5)
        p1.Value.CopyFrom(v)
        c.set_profile(p1)
        self.assertEqual(p1.SerializeToString(), httpretty.last_request().body)

        p2 = profile.ProfileItem()
        p2.CustID = 1
        p2.Oid, p2.OType = 2, '1'
        p2.Key = 'key2'
        v = value.Int(7)
        p2.Value.CopyFrom(v)
        c.set_profile(p2)
        self.assertEqual(p2.SerializeToString(), httpretty.last_request().body)

        # Test get for valid profiles

        expected1 = value.Int(5)
        response1 = httpretty.Response(expected1.SerializeToString())
        expected2 = value.Int(7)
        response2 = httpretty.Response(expected2.SerializeToString())

        httpretty.register_uri(httpretty.POST, 'http://localhost:2425/get', responses=[response1, response2])
        ret = c.get_profile(p1)
        self.assertEqual(value.Int(5), ret)
        ret = c.get_profile(p2)
        self.assertEqual(value.Int(7), ret)

        # Test multi-get profiles
        pl = profile.to_proto_profile_list([p1, p2])
        response1 = httpretty.Response(pl.SerializeToString())

        pl = profile.to_proto_profile_list([p2])
        response2 = httpretty.Response(pl.SerializeToString())

        httpretty.register_uri(httpretty.POST, 'http://localhost:2425/get_profiles', responses=[response1, response2])

        pfr = profile.ProfileFetchRequest()
        ret = c.get_profiles(pfr)
        self.assertEqual(pfr.SerializeToString(), httpretty.last_request().body)
        self.assertListEqual([p1, p2], ret)
        pfr = profile.ProfileFetchRequest()
        pfr.OType = '1'
        ret = c.get_profiles(pfr)
        self.assertEqual(pfr.SerializeToString(), httpretty.last_request().body)
        self.assertListEqual([p2], ret)

    @httpretty.activate(verbose=True, allow_net_connect=False)
    def test_log(self):
        a1 = make_action(1)
        a2 = make_action(2)
        httpretty.register_uri(httpretty.POST, 'http://localhost:2425/log')
        c = client.Client()

        c.log(a1)
        self.assertEqual(a1.SerializeToString(), httpretty.last_request().body)
        c.log(a2)
        self.assertEqual(a2.SerializeToString(), httpretty.last_request().body)

        # but logging an invalid exception throws an error
        with self.assertRaises(client.InvalidInput):
            c.log(1)
        with self.assertRaises(client.InvalidInput):
            c.log('hi')
        with self.assertRaises(client.InvalidInput):
            c.log(action.Action())
        a1.TargetType = ''
        with self.assertRaises(client.InvalidInput):
            c.log(a1)

    @httpretty.activate(verbose=True, allow_net_connect=False)
    def test_fetch(self):
        a1 = make_action(1)
        a2 = make_action(2)
        al = action.to_proto_action_list([a1, a2])
        response1 = httpretty.Response(al.SerializeToString())

        al = action.to_proto_action_list([a2])
        response2 = httpretty.Response(al.SerializeToString())
        httpretty.register_uri(httpretty.POST, 'http://localhost:2425/fetch', responses=[response1, response2])

        # invalid requests throw exception
        c = client.Client()
        with self.assertRaises(client.InvalidInput):
            c.fetch(1)
        with self.assertRaises(client.InvalidInput):
            c.fetch('hi')
        with self.assertRaises(client.InvalidInput):
            c.fetch(action.Action())

        # but not with valid actionfetchrequest
        afr = action.ActionFetchRequest()
        ret = c.fetch(afr)
        self.assertEqual(afr.SerializeToString(), httpretty.last_request().body)
        # self.assertEqual([a1, a2], ret)
        self.assertListEqual([a1, a2], ret)
        afr = action.ActionFetchRequest()
        afr.ActorID = 2
        ret = c.fetch(afr)
        self.assertEqual(afr.SerializeToString(), httpretty.last_request().body)
        # technically we should get a different result but our mock server doesn't know that
        self.assertEqual([a2], ret)

    @httpretty.activate(verbose=True, allow_net_connect=False)
    def test_count(self):
        count = 7
        response = httpretty.Response(str(count))
        httpretty.register_uri(httpretty.POST, 'http://localhost:2425/count', responses=[response])

        # invalid requests throw exception
        c = client.Client()
        with self.assertRaises(client.InvalidInput):
            c.count(1)
        with self.assertRaises(client.InvalidInput):
            c.count('hi')
        with self.assertRaises(client.InvalidInput):
            c.count(counter.GetCountRequest())

        # but not with valid GetCountRequest
        req = counter.GetCountRequest()
        req.CounterType = counter.CounterType.USER_LIKE
        req.Window = counter.Window.HOUR
        req.Key.append(1)
        req.Timestamp = 123
        ret = c.count(req)

        self.assertEqual(req.SerializeToString(), httpretty.last_request().body)
        self.assertEqual(count, ret)

    @httpretty.activate(verbose=True, allow_net_connect=False)
    def test_rate(self):
        rate = 0.123
        response = httpretty.Response(str(rate))
        httpretty.register_uri(httpretty.POST, 'http://localhost:2425/rate', responses=[response])

        # invalid requests throw exception
        c = client.Client()
        with self.assertRaises(client.InvalidInput):
            c.count(1)
        with self.assertRaises(client.InvalidInput):
            c.count('hi')
        with self.assertRaises(client.InvalidInput):
            c.count(counter.GetCountRequest())
        with self.assertRaises(client.InvalidInput):
            c.count(counter.GetRateRequest())

        # but not with valid GetCountRequest
        req = counter.GetRateRequest()
        req.NumCounterType = counter.CounterType.USER_LIKE
        req.DenCounterType = counter.CounterType.VIDEO_LIKE
        req.Window = counter.Window.HOUR
        req.NumKey.append(1)
        req.DenKey.append(2)
        req.Timestamp = 123
        ret = c.rate(req)

        self.assertEqual(req.SerializeToString(), httpretty.last_request().body)
        self.assertEqual(rate, ret)

    @httpretty.activate(verbose=True, allow_net_connect=False)
    def test_query(self):
        d1 = query.Dict(x=query.Int(1), y=query.Int(2))
        d2 = query.Dict(x=query.Int(3), y=query.Int(5))
        d3 = query.Dict(x=query.Int(1), y=query.Int(0))
        t = query.Table(query.List(d1, d2, d3))
        e = query.Transform(t).using(query.Ops.std.filter(where=query.at.x > query.at.y + query.Double(0.5)))
        req = query.query(e)

        v = value.Int(5)
        response = httpretty.Response(v.SerializeToString())
        httpretty.register_uri(httpretty.POST, 'http://localhost:2425/query', responses=[response])

        # invalid requests throw exception
        c = client.Client()
        with self.assertRaises(client.InvalidInput):
            c.query(1)
        with self.assertRaises(client.InvalidInput):
            c.query('hi')
        with self.assertRaises(client.InvalidInput):
            c.query(counter.GetCountRequest())
        with self.assertRaises(client.InvalidInput):
            c.query(e)

        # but not with valid query request
        ret = c.query(req)

        self.assertEqual(req.SerializeToString(), httpretty.last_request().body)
        self.assertEqual(v, ret)

    @httpretty.activate(verbose=True, allow_net_connect=False)
    def test_store_aggregate(self):
        # invalid requests throw exception
        c = client.Client()
        with self.assertRaises(client.InvalidInput):
            c.store_aggregate("", "", query.Int(1), aggregate.AggOptions())
        with self.assertRaises(client.InvalidInput):
            c.store_aggregate("aggtype", "", query.Int(1), aggregate.AggOptions())
        with self.assertRaises(client.InvalidInput):
            c.store_aggregate("aggtype", "aggname", query.Int(1), aggregate.AggOptions())

        # but valid ones don't
        httpretty.register_uri(httpretty.POST, 'http://localhost:2425/store_aggregate')
        options = aggregate.AggOptions()
        options.duration = 6*3600
        ret = c.store_aggregate("aggtype", "aggname", query.query(query.Int(1)), options)
        self.assertIs(None, ret)

        # and if server gave a non-200 response, we raise an error
        def request_callback(request, uri, response_headers):
            return [401, response_headers, 'some error message']
        httpretty.register_uri(httpretty.POST, 'http://localhost:2425/store_aggregate', body=request_callback)
        with self.assertRaises(requests.RequestException):
            c.store_aggregate("aggtype", "aggname", query.query(query.Int(1)), aggregate.AggOptions())

    @httpretty.activate(verbose=True, allow_net_connect=False)
    def test_retrieve_aggregate(self):
        c = client.Client()
        with self.assertRaises(client.InvalidInput):
            c.retrieve_aggregate("", "")
        with self.assertRaises(client.InvalidInput):
            c.retrieve_aggregate("aggtype", "")

        # but valid ones don't
        expected = aggregate.Aggregate()
        expected.agg_type = "some type"
        expected.agg_name = "some name"
        expected.query.CopyFrom(query.query(query.Int(1)))
        options = aggregate.AggOptions()
        options.duration = 6*3600
        expected.options.CopyFrom(options)
        response = httpretty.Response(expected.SerializeToString())
        httpretty.register_uri(httpretty.POST, 'http://localhost:2425/retrieve_aggregate', responses=[response])
        ret = c.retrieve_aggregate("some type", "some name")
        self.assertEqual(expected, ret)

        # and if server gave a non-200 response, we raise an error
        def request_callback(request, uri, response_headers):
            return [401, response_headers, 'some error message']

        httpretty.register_uri(httpretty.POST, 'http://localhost:2425/retrieve_aggregate', body=request_callback)
        with self.assertRaises(requests.RequestException):
            c.retrieve_aggregate("aggtype", "aggname")

    @httpretty.activate(verbose=True, allow_net_connect=False)
    def test_aggregate_value(self):
        # invalid requests throw exception
        c = client.Client()
        with self.assertRaises(client.InvalidInput):
            c.aggregate_value(1)
        with self.assertRaises(client.InvalidInput):
            c.aggregate_value('hi')
        with self.assertRaises(client.InvalidInput):
            c.aggregate_value(counter.GetCountRequest())
        bad_request = aggregate.GetAggValueRequest()
        bad_request.agg_type = "sometype"
        bad_request.agg_name = "" # this is bad because empty names aren't allowed
        bad_request.key.CopyFrom(value.Int(1))
        with self.assertRaises(client.InvalidInput):
            c.aggregate_value(bad_request)
        bad_request.agg_type = "sometype"
        bad_request.agg_name = "somename"
        bad_request.key.CopyFrom(value.Value()) # this time, this is an ill-formed value
        with self.assertRaises(client.InvalidInput):
            c.aggregate_value(bad_request)

        # but not with valid query request
        v = value.Int(5)
        response = httpretty.Response(v.SerializeToString())
        httpretty.register_uri(httpretty.POST, 'http://localhost:2425/aggregate_value', responses=[response])
        request = aggregate.GetAggValueRequest()
        request.agg_type = "sometype"
        request.agg_name = "somename"
        request.key.CopyFrom(value.Int(1))
        ret = c.aggregate_value(request)

        self.assertEqual(request.SerializeToString(), httpretty.last_request().body)
        self.assertEqual(v, ret)


def make_action(k):
    k = k * 10
    a = action.Action()
    a.ActorID = k
    a.ActorType = str(k + 1)
    a.TargetID = k + 2
    a.TargetType = str(k + 3)
    a.ActionType = str(k + 4)
    a.RequestID = k + 5
    a.CustID = k + 6
    return a


def make_profile(k):
    k = k * 9
    p = profile.ProfileItem()
    p.CustID = 1
    p.Oid = k+7
    p.Otype = str(k%2)
    p.Key = str(k)
    return p


if __name__ == '__main__':
    unittest.main()
