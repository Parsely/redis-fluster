from __future__ import absolute_import, print_function
import unittest
import sys

from testinstances import RedisInstance
from fluster import FlusterCluster, round_controlled


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

        for idx, conn in enumerate(
            round_controlled(self.cluster, rounds=desired_rounds)
        ):
            pass

        # should raise stopiteration at appropriate time
        assert idx == (desired_rounds * len(self.cluster.active_clients) - 1)
