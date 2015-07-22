import time

import unittest2
from redis.exceptions import ConnectionError
from testinstances import RedisInstance

from fluster import FlusterCluster

class FlusterClusterTests(unittest2.TestCase):

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
                cl = self.cluster.get_client(key)
                cl.incr(key, 1)

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
                self.assertItemsEqual(res, [None, '1', '1'])
            else:
                self.assertItemsEqual(res, [None, None, '3'])

