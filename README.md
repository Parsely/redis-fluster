Redis Fluster Cluster
---------------------

A Redis cluster for when single points of failure leave you flustered.

Overview
========

Redis Fluster is a very limited Redis pool/cluster implementation that makes sharding simple for a few common use cases.

### Quickstart
```python
import redis
from fluster import FlusterCluster
cluster = FlusterCluster([redis.Redis(6379), redis.Redis(6380)])

while True:
  for key in ('foo', 'bar'):
    try:
      client = cluster.get_client(key)
      client.incr(key, 1)
      client.get(key)
    except ConnectionError:
      client = cluster.get_client(key)
      client.incr(key, 1)
      client.get(key)
  time.sleep(1)
```


Limited, how? I want to use this for everything!
================================================

Simply put, don't. Fluster maintains a pool of connections to various Redis instances and will return a connection to one based on a shard key provided. If one goes down, it gets put in a penalty box until it comes back up, at which point it's usable again.

At not point are keys duplicated, nor are they redistributed when nodes drop/join. If you're writing INCR statements and the node goes down, now you're writing them to another instance. Once the original instance returns, you've got two sets of values for the same key. This will be seamless and your program won't crash, so maybe that's enough.

Then what's it good for?
========================
Caches and ephemeral data. The ideal case is where going down is worse than duplicated data. If a Redis fluster node goes down and you've got two copies of a cache, this probably isn't a problem. You've set expiries on the data, right?

Likewise, if you're using lists to distribute a queue to multiple Redis instances, then one dropping and joining isn't a problem. Items queued while it was down went elsewhere and if your workers read from all available instances, then nothing has been lost, or is really all that different.

What about Redis Cluster?
=========================
Looks great! Unfortunately, it's very new and not ready for production use. With all due luck, that will achieve all its promise and this project can quietly fade away.
