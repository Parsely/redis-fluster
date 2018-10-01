from __future__ import absolute_import, print_function
import time
import unittest
import sys

from testinstances import RedisInstance

from fluster import FlusterCluster, ClusterEmptyError


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
        # should cycle through clients the given number of times
        desired_rounds = 2
        returned_clients = set()
        active_clients_cycle = self.cluster.get_active_client_cycle(rounds=desired_rounds)

        assert True

        for idx, client in enumerate(active_clients_cycle):
            returned_clients.update([client])
            assert client is not None

        assert (idx + 1) == (desired_rounds * len(self.cluster.active_clients))
        assert len(returned_clients) == len(self.cluster.active_clients)

    def test_cycle_clients_with_failures(self):
        # should not include inactive nodes
        self.instances[0].terminate()

        desired_rounds = 2
        counter = 0
        active_clients_cycle = self.cluster.get_active_client_cycle(rounds=desired_rounds)

        for client in active_clients_cycle:
            assert client is not None
            try:
                client.incr('key', 1)
                counter += 1
            except Exception as e:
                print("oops", client, e)
                continue  # exception handled by the cluster

        # Restart instance
        self.instances[0] = RedisInstance(10101)
        time.sleep(0.5)

        assert counter == 4  # 2 rounds, 2 working clients each round
        assert 2 == len(self.cluster.active_clients)
        assert 2 == len(self.cluster.initial_clients.values()) - 1

        # should add restarted nodes back to the list after reported failure
        # calling __iter__ again checks the penalty box
        counter = 0
        for client in active_clients_cycle:
            client.incr('key', 1)
            counter += 1

        assert counter == len(self.cluster.active_clients) * desired_rounds
        assert len(self.cluster.active_clients) == 3  # to verify it added the node back

    def test_cycle_clients_tracking(self):
        # should track separate cycle entry points for each instance
        active_cycle_1 = self.cluster.get_active_client_cycle(rounds=1)
        active_cycle_2 = self.cluster.get_active_client_cycle(rounds=1)

        # advance cycle 1
        next(active_cycle_1)

        # should not start at the same point
        assert next(active_cycle_1) != next(active_cycle_2)

    def test_dropped_connections_while_iterating(self):
        # dropped connections in the middle of an iteration should not cause an infinite loop
        # and should raise an exception
        active_cycle = self.cluster.get_active_client_cycle(rounds=7)

        assert len(self.cluster.active_clients) == 3

        drop_at_idx = (5, 6, 7)  # at these points, kill a connection
        killed = 0
        with self.assertRaises(ClusterEmptyError) as context:
            for idx, client in enumerate(active_cycle):
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
