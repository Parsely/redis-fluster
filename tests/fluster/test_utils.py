from __future__ import absolute_import, print_function
import time
import unittest
import sys

from testinstances import RedisInstance
from fluster import FlusterCluster, ClusterEmptyError, round_controlled
import redis


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

    def test_cycle_clients(self):
        # should cycle through clients indefinately
        returned_clients = set()
        limit = 15

        assert True

        for idx, client in enumerate(self.cluster):
            returned_clients.update([client])
            assert client is not None
            if idx >= limit:
                break

        assert idx == 15
        assert len(returned_clients) == len(self.cluster.active_clients)

    def test_cycle_clients_with_failures(self):
        # should not include inactive nodes
        self.instances[0].terminate()
        limit = 6
        counter = 0

        for idx, client in enumerate(self.cluster):
            assert client is not None
            try:
                client.incr('key', 1)
                counter += 1
            except Exception as e:
                print("oops", client, e)
                continue  # exception handled by the cluster
            if idx >= limit:
                break

        # Restart instance
        self.instances[0] = RedisInstance(10101)
        time.sleep(0.5)

        assert counter == 6  # able to continue even when node is down
        assert 2 == len(self.cluster.active_clients)
        assert 2 == len(self.cluster.initial_clients.values()) - 1

        # should add restarted nodes back to the list after reported failure
        # calling __iter__ again checks the penalty box
        counter = 0
        for idx, client in enumerate(self.cluster):
            if idx >= limit:
                break
            client.incr('key', 1)
            counter += 1

        assert counter == limit
        assert len(self.cluster.active_clients) == 3  # to verify it added the node back

    def test_cycle_clients_tracking(self):
        # should track separate cycle entry points for each instance
        cluster_instance_1 = self.cluster
        # connect to already-running testinstances, instead of making more,
        # to mimic two FlusterCluster instances
        redis_clients = [redis.StrictRedis(port=conn.port)
                         for conn in self.instances]
        cluster_instance_2 = FlusterCluster([i for i in redis_clients],
                                            penalty_box_min_wait=0.5)

        # advance cluster instance one
        next(cluster_instance_1)

        # should not start at the same point
        assert next(cluster_instance_1) != next(cluster_instance_2)

        for temp_conn in redis_clients:
            del temp_conn

    def test_dropped_connections_while_iterating(self):
        # dropped connections in the middle of an iteration should not cause an infinite loop
        # and should raise an exception
        limit = 21

        assert len(self.cluster.active_clients) == 3

        drop_at_idx = (5, 6, 7)  # at these points, kill a connection
        killed = 0
        with self.assertRaises(ClusterEmptyError) as context:
            for idx, client in enumerate(self.cluster):
                if idx >= limit:
                    break  # in case the test fails to stop
                if idx in drop_at_idx:
                    self.instances[killed].terminate()
                    killed += 1
                    print('killed ', idx, killed)
                try:
                    client.incr('key', 1)
                except:
                    pass  # mimic err handling
            self.assertTrue('All clients are down.' in str(context.exception))

        assert idx == 8  # the next iteration after the last client was killed

        # restart all the instances
        for instance, port in enumerate(range(10101, 10104)):
            self.instances[instance] = RedisInstance(port)

    def test_round_controller(self):
        # the round controller should track rounds and limit iterations
        repeated_sublist = list(range(0, 3))
        lis = repeated_sublist * 5
        desired_rounds = 4  # don't iterate through the whole list
        for idx, item in enumerate(round_controlled(lis, rounds=desired_rounds)):
            pass

        assert idx == desired_rounds * len(repeated_sublist) - 1

        # more specific application
        desired_rounds = 3

        for idx, conn in enumerate(round_controlled(self.cluster, rounds=desired_rounds)):
            pass

        # should raise stopiteration at appropriate time
        assert idx == (desired_rounds * len(self.cluster.active_clients) - 1)
