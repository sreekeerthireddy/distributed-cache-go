# distributed-cache-go

A distributed in-memory key-value cache built in Go from scratch — LRU eviction, TTL,
consistent-hashing sharding with virtual nodes, concurrent-safe access, and live add/remove
of nodes with data migration.

I built this to actually understand how systems like **Redis Cluster** and **Memcached** work
under the hood — sharding, lock striping, consistent hashing, rebalancing — instead of just
using them. It's pure Go standard library (no external dependencies), and it's tested end to end
under the race detector.

## What it does

- **LRU + TTL, per shard** — O(1) get/put via a hashmap + doubly-linked list; lazy TTL expiry
  (expired entries are dropped on access).
- **Sharding via consistent hashing** with virtual nodes — keys spread evenly across shards, and
  adding/removing a node reshuffles only ~1/N of keys instead of the whole keyspace.
- **Thread-safe** — each shard has its own lock (lock striping), so operations on different shards
  run in parallel. Verified clean under `go test -race`.
- **Dynamic topology** — add or remove nodes at runtime; keys migrate to their new owners with no
  data loss.
- **Benchmark** — measures throughput as the shard count scales, plus a Zipfian (hot-key) workload.

## Architecture

Three layers, each independently testable:

```
Cache   (facade + routing)   cache.go   — ties it together; routes each op to the owning shard
  │
Ring    (topology)           ring.go    — consistent hashing: key -> node, with add/remove
  │
Shard   (storage)            shard.go   — a self-contained LRU + TTL cache with its own lock
```

- **Shard** — the actual data: a `map[string]*list.Element` for O(1) lookup plus a `container/list`
  for recency order, guarded by a `sync.Mutex`. Handles eviction and TTL.
- **Ring** — pure routing, holds no data. Places each node at 128 virtual positions on a hash ring;
  a key is owned by the first node clockwise from its hash.
- **Cache** — holds the ring plus a `map[nodeID]*Shard`. `Get`/`Put`/`Delete` route through the ring
  to the owning shard; `AddNode`/`RemoveNode` change the topology and migrate keys.

## Design notes (the parts I found interesting)

**Lock striping for concurrency.** A single global lock would serialize every operation and defeat
the purpose of sharding. Instead each shard has its own `Mutex`, so N shards allow up to N operations
to run truly in parallel — bounded by the number of CPU cores.

**Consistent hashing needed a better hash.** My first version used FNV-1a to place virtual nodes.
A distribution test caught that it clustered them badly — one node ended up owning 56% of the keyspace
instead of 33%, because FNV avalanches poorly on structured inputs like `"node#0"`, `"node#1"`, ….
Running the hash through a murmur3 finalizer fixed the avalanche, and the distribution evened out.

**Two-level locking.** A cache-level `RWMutex` protects the *topology* (the ring + the shard map),
while each shard's `Mutex` protects its *data*. The subtlety: `Get`/`Put`/`Delete` only *read* the
topology to find the shard — so they take the read lock and run concurrently — even though `Put` and
`Delete` *write* data (to the shard, under the shard's own lock). Only `AddNode`/`RemoveNode`, which
mutate the topology, take the exclusive lock.

**Migration on topology change.** When a node is added or removed, the keys that change ownership move
to their new shard, preserving TTL — atomically, under the exclusive lock (stop-the-world). Consistent
hashing keeps this to ~1/N of keys.

## Benchmark

Throughput vs. shard count — 64 workers, 90% reads / 10% writes, on a 16-core machine:

```
UNIFORM keys     shards   throughput (op/s)   vs 1 shard
                 1        2,560,502           1.00x
                 2        3,535,622           1.38x
                 4        4,952,694           1.93x
                 8        6,471,905           2.53x
                 16       6,541,656           2.55x
```

Throughput scales with the shard count as lock contention spreads across shards, then plateaus near
the core count. The scaling is sub-linear because the cache-level `RWMutex` — taken on every operation
to read the topology — becomes a shared bottleneck (a known limit of `RWMutex` under high read
concurrency). This is exactly what Memcached avoids by keeping its hot path on striped mutexes with no
global lock, and it's the first item on the roadmap.

## Running it

```bash
go test -race ./...      # full test suite under the race detector
go run ./cmd/bench       # throughput benchmark
```

## Roadmap

- **Lock-free reads** via an atomic copy-on-write ring swap — remove the per-op global lock (the
  benchmark bottleneck above).
- **Hot-key handling** — replication for read-hot keys; sharded counters for write-hot keys
  (e.g. a viral post's like counter).
- **Replication** for availability under node failure; **online (forward-on-miss) migration** to
  preserve keys without stop-the-world.
- **Background TTL sweeper**; byte-based capacity limits.

---

Built as a learning project — the goal was to understand distributed-cache internals deeply, so the
code favors clarity and correctness over micro-optimization.
