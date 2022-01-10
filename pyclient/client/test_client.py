import unittest
import httpretty

import action
import client


class Testclient(unittest.TestCase):
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
    def test_log_fetch(self):
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


def make_action(k):
    k = k * 10
    a = action.Action()
    a.ActorID = k
    a.ActorType = k + 1
    a.TargetID = k + 2
    a.TargetType = k + 3
    a.ActionType = k + 4
    a.RequestID = k + 5
    return a


if __name__ == '__main__':
    unittest.main()
