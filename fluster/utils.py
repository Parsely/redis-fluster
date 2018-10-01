from itertools import cycle

from .exceptions import ClusterEmptyError


class ActiveClientCycle(object):
    """Tracks last returned client, will not iterate more than `rounds` times.

    Useful when you need to evenly cycle through active connections to check
    each once for work.

    Each user of the class should instantiate a separate object to correctly track
    the last requested client for that requester.
    """
    def __init__(self, cluster, rounds=1):
        self.cluster = cluster
        self.round_limit = rounds
        self.clients = cycle(cluster.initial_clients.values())

        # initialize pointers and trackers
        self.current_client = None
        self.round_start = None
        self.rounds_completed = 0

    def __iter__(self):
        """Restarts the `rounds` tracker, and updates active clients."""
        self.round_start = None
        self.rounds_completed = 0

        self.cluster._prune_penalty_box()  # XX TODO make public

        if len(self.cluster.active_clients) == 0:
            raise ClusterEmptyError('All clients are down.')

        return self

    def __next__(self):
        nxt = None
        while nxt is None:
            nxt = self._next_helper()

        return nxt

    def _next_helper(self):
        """Returns an active connection, unless this iterable has already cycled
        through too many times.
        """
        curr = next(self.clients)

        # manage round counting
        if not self.round_start:
            self.round_start = curr
        elif self.round_start == curr:
            self.rounds_completed += 1

        if self.rounds_completed >= self.round_limit:
            raise StopIteration

        # check active clients
        if curr in self.cluster.active_clients:
            return curr
