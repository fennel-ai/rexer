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
