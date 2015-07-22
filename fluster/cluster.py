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
        self.clients = self._prep_clients(clients)
        self._sort_clients()

    def _sort_clients(self):
        """Make sure clients are sorted consistently for consistent results."""
        self.clients.sort(key=lambda c: c.pool_id)

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
                    if client in self.clients:  # hasn't been removed yet
                        log.warning('%r marked down.', client)
                        self.clients.remove(client)
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
            self.clients.append(client)
            added = True
        if added:
            self._sort_clients()

        if len(self.clients) == 0:
            raise ClusterEmptyError('All clients are down.')

        pos = mmh3.hash(shard_key) % len(self.clients)
        return self.clients[pos]
