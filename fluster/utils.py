from itertools import cycle

from .exceptions import ClusterEmptyError


def round_controlled(cycled_iterable, rounds=1):
    """Raise StopIteration after <rounds> passes through a cycled iterable."""
    round_start = None
    rounds_completed = 0

    for item in cycled_iterable:
        if round_start is None:
            round_start = item
        elif item == round_start:
            rounds_completed += 1

        if rounds_completed == rounds:
            raise StopIteration

        yield item


class ActiveClientCycle(object):
    """Only returns active clients.

    Useful when you need to evenly cycle through active connections, skipping
    dead ones.

    Each user of the class should instantiate a separate object to correctly track
    the last requested client for that user.
    """
    def __init__(self, cluster):
        self.cluster = cluster
        self.clients = cycle(cluster.initial_clients.values())

    def __iter__(self):
        """Restarts the `rounds` tracker, and updates active clients."""
        self.cluster._prune_penalty_box()

        return self

    def __next__(self):
        """Always returns a client, or raises an Exception if none are available."""
        # raise Exception if no clients are available
        if len(self.cluster.active_clients) == 0:
            raise ClusterEmptyError('All clients are down.')

        # always return something
        nxt = None
        while nxt is None:
            nxt = self._next_helper()

        return nxt

    def next(self):
        """Python 2/3 compatibility."""
        return self.__next__()

    def _next_helper(self):
        """Helper that only returns an active connection.
        """
        curr = next(self.clients)

        # only return active connections
        if curr in self.cluster.active_clients:
            return curr
