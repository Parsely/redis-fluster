from __future__ import absolute_import, print_function

import time
import unittest
import sys

from redis.exceptions import ConnectionError
from testinstances import RedisInstance

from fluster import FlusterCluster


class FlusterClusterTests(unittest.TestCase):

    def assertCountEqual(self, a, b):
        if sys.version_info > (3, 0):
            super(FlusterClusterTests, self).assertCountEqual(a, b)
        else:
            self.assertItemsEqual(a, b)

    @classmethod
    def setUpClass(cls):
        cls.instances = [RedisInstance(10101),
                         RedisInstance(10102),
                         RedisInstance(10103)]

    @classmethod
    def tearDownClass(cls):
        for instance in cls.instances:
            instance.terminate()

    def setUp(self):
        self.cluster = FlusterCluster([i.conn for i in self.instances],
                                      penalty_box_min_wait=0.5)
        self.keys = ['hi', 'redis', 'test']  # hashes to 3 separate values

    def tearDown(self):
        for instance in self.instances:
            if hasattr(instance.conn, 'pool_id'):
                delattr(instance.conn, 'pool_id')

    def test_basic(self):
        clients = []
        for key in self.keys:
            client = self.cluster.get_client(key)
            self.assertNotIn(client, clients)
            clients.append(client)

    def test_single_failure(self):
        # Set all keys, one on each instance
        for key in self.keys:
            self.cluster.get_client(key).incr(key, 1)

        self.instances[0].terminate()

        # Do it again, one of them will move to a new instance
        changed = None
        for key in self.keys:
            try:
                self.cluster.get_client(key).incr(key, 1)
            except ConnectionError:
                changed = key
                client = self.cluster.get_client(key)
                client.incr(key, 1)

        # Restart instance
        self.instances[0] = RedisInstance(10101)
        time.sleep(0.5)

        # INCR again. Unmoved keys will be '3' and moved will have '1', '1'
        # N.B. RedisInstance doesn't save between instances, so original INCR
        #      was lost.
        for key in self.keys:
            # INCR
            self.cluster.get_client(key).incr(key, 1)

            # Check values in all running instances
            res = [i.conn.get(key) for i in self.instances]
            if key == changed:
                self.assertCountEqual(res, [None, b'1', b'1'])
            else:
                self.assertCountEqual(res, [None, None, b'3'])

    def test_consistent_hashing(self):
        key = 'foo'  # 2 with 3 clients, 0 with 2
        client = self.cluster.get_client(key)
        self.instances[0].terminate()
        try:
            self.assertRaises(
                ConnectionError,
                lambda: self.cluster.get_client(self.keys[0]).incr('hi', 1)
            )
            client_2 = self.cluster.get_client(key)
            self.assertEqual(client, client_2)
        finally:
            # Bring it back up
            self.instances[0] = RedisInstance(10101)

    def test_zrevrange(self):
        """Add a sorted set, turn off the client, add to the set,
        turn the client back on, check results
        """
        key = 'foo'
        for element, count in zip(self.keys, (1.0, 2.0, 3.0)):
            client = self.cluster.get_client(element)
            client.zadd(key, count, element)
        revrange = self.cluster.zrevrange_with_int_score(key, '+inf', 2)
        self.assertEqual(set([3, 2]), set(revrange.values()))

        lost_client = self.cluster.get_client(self.keys[-1])
        self.instances[-1].terminate()

        dropped_element = self.keys[-1]
        new_count = 5
        client = self.cluster.get_client(dropped_element)
        try:
            client.zadd(key, new_count, dropped_element)
            raise Exception("Should not get here, client was terminated")
        except ConnectionError:
            client = self.cluster.get_client(dropped_element)
            print('replaced client', client)
            client.zadd(key, new_count, dropped_element)
        revrange = self.cluster.zrevrange_with_int_score(key, '+inf', 2)
        self.assertEqual(set([new_count, 2]), set(revrange.values()))

        #turn it back on
        self.instances[-1] = RedisInstance(10103)
        time.sleep(0.5)

        revrange = self.cluster.zrevrange_with_int_score(key, '+inf', 2)
        self.assertEqual(set([new_count, 2]), set(revrange.values())) #restarted instance is empty in this case

        client = self.cluster.get_client(dropped_element)
        client.zadd(key, 3, dropped_element) #put original value back in
        revrange = self.cluster.zrevrange_with_int_score(key, '+inf', 2)
        self.assertEqual(set([new_count, 2]), set(revrange.values())) #max value found for duplicates is returned
