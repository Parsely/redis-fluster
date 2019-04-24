from __future__ import absolute_import, print_function

import time
import unittest
import sys

from redis.exceptions import ConnectionError
from testinstances import RedisInstance

from fluster import FlusterCluster, ClusterEmptyError
import redis


class FlusterClusterTests(unittest.TestCase):
    def assertCountEqual(self, a, b):
        if sys.version_info > (3, 0):
            super(FlusterClusterTests, self).assertCountEqual(a, b)
        else:
            self.assertItemsEqual(a, b)

    @classmethod
    def setUpClass(cls):
        cls.instances = [
            RedisInstance(10101),
            RedisInstance(10102),
            RedisInstance(10103),
        ]

    @classmethod
    def tearDownClass(cls):
        for instance in cls.instances:
            instance.terminate()

    def setUp(self):
        self.cluster = FlusterCluster(
            [i.conn for i in self.instances], penalty_box_min_wait=0.5
        )
        self.keys = ["hi", "redis", "test"]  # hashes to 3 separate values

    def tearDown(self):
        for instance in self.instances:
            if hasattr(instance.conn, "pool_id"):
                delattr(instance.conn, "pool_id")

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
                self.assertCountEqual(res, [None, b"1", b"1"])
            else:
                self.assertCountEqual(res, [None, None, b"3"])

    def test_consistent_hashing(self):
        key = "foo"  # 2 with 3 clients, 0 with 2
        client = self.cluster.get_client(key)
        self.instances[0].terminate()
        try:
            self.assertRaises(
                ConnectionError,
                lambda: self.cluster.get_client(self.keys[0]).incr("hi", 1),
            )
            client_2 = self.cluster.get_client(key)
            self.assertEqual(client, client_2)
        finally:
            # Bring it back up
            self.instances[0] = RedisInstance(10101)

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
                client.incr("key", 1)
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
            client.incr("key", 1)
            counter += 1

        assert counter == limit
        assert len(self.cluster.active_clients) == 3  # to verify it added the node back

    def test_long_running_iterations(self):
        # long-running iterations should still add connections back to the cluster
        drop_client = 3
        restart_client = 10
        client_available = restart_client + 1

        for idx, client in enumerate(self.cluster):
            # attempt to use each client
            try:
                client.incr("key", 1)
            except Exception:
                continue  # exception handled by the cluster
            # mimic connection dropping out and returning
            if idx == drop_client:
                self.instances[0].terminate()
            elif idx == restart_client:
                self.instances[0] = RedisInstance(10101)
            # client should be visible after calling next() again
            elif idx == client_available:
                assert len(self.cluster.active_clients) == 3
                break

    def test_cycle_clients_tracking(self):
        # should track separate cycle entry points for each instance
        cluster_instance_1 = self.cluster
        # connect to already-running testinstances, instead of making more,
        # to mimic two FlusterCluster instances
        redis_clients = [redis.StrictRedis(port=conn.port) for conn in self.instances]
        cluster_instance_2 = FlusterCluster(
            [i for i in redis_clients], penalty_box_min_wait=0.5
        )

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
                    print("killed ", idx, killed)
                try:
                    client.incr("key", 1)
                except:
                    pass  # mimic err handling
            self.assertTrue("All clients are down." in str(context.exception))

        assert idx == 8  # the next iteration after the last client was killed

        # restart all the instances
        for instance, port in enumerate(range(10101, 10104)):
            self.instances[instance] = RedisInstance(port)

    def test_zrevrange(self):
        """Add a sorted set, turn off the client, add to the set,
        turn the client back on, check results
        """
        key = "foo"
        for element, count in zip(self.keys, (1.0, 2.0, 3.0)):
            client = self.cluster.get_client(element)
            client.zadd(key, {element: count})
        revrange = self.cluster.zrevrange_with_int_score(key, "+inf", 2)
        self.assertEqual(set([3, 2]), set(revrange.values()))

        lost_client = self.cluster.get_client(self.keys[-1])
        self.instances[-1].terminate()

        dropped_element = self.keys[-1]
        new_count = 5
        client = self.cluster.get_client(dropped_element)
        try:
            client.zadd(key, {dropped_element: new_count})
            raise Exception("Should not get here, client was terminated")
        except ConnectionError:
            client = self.cluster.get_client(dropped_element)
            print("replaced client", client)
            client.zadd(key, {dropped_element: new_count})
        revrange = self.cluster.zrevrange_with_int_score(key, "+inf", 2)
        self.assertEqual(set([new_count, 2]), set(revrange.values()))

        # turn it back on
        self.instances[-1] = RedisInstance(10103)
        time.sleep(0.5)

        revrange = self.cluster.zrevrange_with_int_score(key, "+inf", 2)
        self.assertEqual(
            set([new_count, 2]), set(revrange.values())
        )  # restarted instance is empty in this case

        client = self.cluster.get_client(dropped_element)
        client.zadd(key, {dropped_element: 3})  # put original value back in
        revrange = self.cluster.zrevrange_with_int_score(key, "+inf", 2)
        self.assertEqual(
            set([new_count, 2]), set(revrange.values())
        )  # max value found for duplicates is returned
