from collections import defaultdict
import functools
import logging

import mmh3
import redis
from redis.exceptions import ConnectionError

from .exceptions import ClusterEmptyError
from .penalty_box import PenaltyBox

log = logging.getLogger(__name__)


class FlusterCluster(object):
    """A pool of redis instances where dead nodes are automatically removed.

    This implementation is VERY LIMITED. There is NO consistent hashing, and
    no attempt at recovery/rebalancing as nodes are dropped/added. Therefore,
    it's best served for fundamentally ephemeral data where some duplication
    or missing keys isn't a problem.

    Ideal cases for this are things like caches, where another copy of data
    isn't a huge problem (provided expiries are respected).
    """

    @classmethod
    def from_settings(cls, conn_settingses):
        return cls(redis.Redis(**c) for c in conn_settingses)

    def __init__(self,
                 clients,
                 penalty_box_min_wait=10,
                 penalty_box_max_wait=300,
                 penalty_box_wait_multiplier=1.5):
        self.penalty_box = PenaltyBox(min_wait=penalty_box_min_wait,
                                      max_wait=penalty_box_max_wait,
                                      multiplier=penalty_box_wait_multiplier)
        self.active_clients = self._prep_clients(clients)
        self.initial_clients = {c.pool_id: c for c in clients}
        self._sort_clients()

    def _sort_clients(self):
        """Make sure clients are sorted consistently for consistent results."""
        self.active_clients.sort(key=lambda c: c.pool_id)

    def _prep_clients(self, clients):
        """Prep a client by tagging it with and id and wrapping methods.

        Methods are wrapper to catch ConnectionError so that we can remove
        it from the pool until the instance comes back up.

        :returns: patched clients
        """
        for pool_id, client in enumerate(clients):
            # Tag it with an id we'll use to identify it in the pool
            if hasattr(client, 'pool_id'):
                raise ValueError("%r is already part of a pool.", client)
            setattr(client, 'pool_id', pool_id)
            # Wrap all public functions
            self._wrap_functions(client)
        return clients

    def _wrap_functions(self, client):
        """Wrap public functions to catch ConnectionError.

        When an error happens, it puts the client in the penalty box
        so that it won't be retried again for a little while.
        """
        def wrap(fn):
            def wrapper(*args, **kwargs):
                """Simple wrapper for to catch dead clients."""
                try:
                    return fn(*args, **kwargs)
                except ConnectionError:  # TO THE PENALTY BOX!
                    if client in self.active_clients:  # hasn't been removed yet
                        log.warning('%r marked down.', client)
                        self.active_clients.remove(client)
                        self.penalty_box.add(client)
                    raise
            return functools.update_wrapper(wrapper, fn)

        for name in dir(client):
            if name.startswith('_'):
                continue
            # Some things aren't wrapped
            if name in ('echo', 'execute_command', 'parse_response'):
                continue
            obj = getattr(client, name)
            if not callable(obj):
                continue
            log.debug('Wrapping %s', name)
            setattr(client, name, wrap(obj))

    def get_client(self, shard_key):
        """Get the client for a given shard, based on what's available.

        If the proper client isn't available, the next available client
        is returned. If no clients are available, an exception is raised.
        """
        added = False
        for client in self.penalty_box.get():
            log.info('Client %r is back up.', client)
            self.active_clients.append(client)
            added = True
        if added:
            self._sort_clients()

        if len(self.active_clients) == 0:
            raise ClusterEmptyError('All clients are down.')

        # So that hashing is consistent when a node is down, check against
        # the initial client list. Only use the active client list when
        # the desired node is down.
        # N.B.: I know this is not technically "consistent hashing" as
        #       academically defined. It's a hack so that keys which need to
        #       go elsewhere do, while the rest stay on the same instance.
        if not isinstance(shard_key, bytes):
            shard_key = shard_key.encode('utf-8')
        hashed = mmh3.hash(shard_key)
        pos = hashed % len(self.initial_clients)
        if self.initial_clients[pos] in self.active_clients:
            return self.initial_clients[pos]
        else:
            pos = hashed % len(self.active_clients)
            return self.active_clients[pos]

    def zrevrange_with_int_score(self, key, max_score, min_score):
        """Get the zrevrangebyscore across the cluster.
        Highest score for duplicate element is returned.
        A faster method should be written if scores are not needed.
        """
        if len(self.active_clients) == 0:
            raise ClusterEmptyError('All clients are down.')

        element__score = defaultdict(int)
        for client in self.active_clients:
            revrange = client.zrevrangebyscore(
                key, max_score, min_score,
                withscores=True,
                score_cast_func=int,
            )

            for element, count in revrange:
                element__score[element] = max(element__score[element], int(count))

        return element__score
