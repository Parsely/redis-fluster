import heapq
import logging
import time

from redis.exceptions import ConnectionError, TimeoutError


log = logging.getLogger(__name__)


class PenaltyBox(object):
    """A place for redis clients being put in timeout."""

    def __init__(self, min_wait=10, max_wait=300, multiplier=1.5):
        self._clients = []  # heapq of (release_time, (client, last_wait))
        self._client_ids = set()  # client ids in the penalty box
        self._min_wait = min_wait
        self._max_wait = max_wait
        self._multiplier = multiplier

    def add(self, client):
        """Add a client to the penalty box."""
        if client.pool_id in self._client_ids:
            log.info("%r is already in the penalty box. Ignoring.", client)
            return
        release = time.time() + self._min_wait
        heapq.heappush(self._clients, (release, (client, self._min_wait)))
        self._client_ids.add(client.pool_id)

    def get(self):
        """Get any clients ready to be used.

        :returns: Iterable of redis clients
        """
        now = time.time()
        while self._clients and self._clients[0][0] < now:
            _, (client, last_wait) = heapq.heappop(self._clients)
            connect_start = time.time()
            try:
                client.echo("test")  # reconnected if this succeeds.
                self._client_ids.remove(client.pool_id)
                yield client
            except (ConnectionError, TimeoutError):
                timer = time.time() - connect_start
                wait = min(int(last_wait * self._multiplier), self._max_wait)
                heapq.heappush(self._clients, (time.time() + wait, (client, wait)))
                log.info(
                    "%r is still down after a %s second attempt to connect. Retrying in %ss.",
                    client,
                    timer,
                    wait,
                )
