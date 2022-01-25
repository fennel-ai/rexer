import unittest
import httpretty

from models import action, counter, value, profile
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
            c.get(profile.ProfileItem)

        # but valid requests don't throw exceptions
        httpretty.register_uri(httpretty.POST, 'http://localhost:2425/set')

        p1 = profile.ProfileItem()
        p1.CustID = 1
        p1.Oid, p1.OType = 1, 2
        p1.Key = 'key'
        v = value.Int(5)
        p1.Value.CopyFrom(v)
        c.set_profile(p1)
        self.assertEqual(p1.SerializeToString(), httpretty.last_request().body)

        p2 = profile.ProfileItem()
        p2.CustID = 1
        p2.Oid, p2.OType = 2, 1
        p2.Key = 'key2'
        v = value.Int(7)
        p2.Value.CopyFrom(v)
        c.set_profile(p2)
        self.assertEqual(p2.SerializeToString(), httpretty.last_request().body)

        expected = value.Int(5)
        response = httpretty.Response(expected.SerializeToString())
        httpretty.register_uri(httpretty.POST, 'http://localhost:2425/get', responses=[response])
        ret = c.get_profile(req)
        self.assertEqual(value.Int(5), ret)

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
        pfr.OType = 1
        ret = c.get_profiles(pfr)
        self.assertEqual(pfr.SerializeToString(), httpretty.lasts_request().body)
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
        a1.TargetType = 0
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


def make_action(k):
    k = k * 10
    a = action.Action()
    a.ActorID = k
    a.ActorType = k + 1
    a.TargetID = k + 2
    a.TargetType = k + 3
    a.ActionType = k + 4
    a.RequestID = k + 5
    a.CustID = k + 6
    return a

def make_profile(k):
    k = k * 9
    p = profile.ProfileItem()
    p.CustID = 1
    p.Oid = k+7
    p.Otype = k%2
    p.Key = str(k)
    return p

if __name__ == '__main__':
    unittest.main()
