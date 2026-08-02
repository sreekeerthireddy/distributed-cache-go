# distributed-cache-go

A distributed in-memory key-value cache built in Go from scratch — LRU eviction, TTL,
consistent-hashing sharding with virtual nodes, **lock-free concurrent reads**, and live
add/remove of nodes.

I built this to actually understand how systems like **Redis Cluster** and **Memcached** work
under the hood — sharding, lock striping, consistent hashing, rebalancing — instead of just
using them. It's pure Go standard library (no external dependencies), tested end to end under
the race detector, and benchmarked (with a real optimization story below).

## What it does

- **LRU + TTL, per shard** — O(1) get/put via a hashmap + doubly-linked list; lazy TTL expiry
  (expired entries are dropped on access).
- **Sharding via consistent hashing** with virtual nodes — keys spread evenly across shards, and
  adding/removing a node only shifts ~1/N of the keyspace instead of everything.
- **Lock-free reads** — the routing topology is held in an `atomic.Pointer`, so `Get`/`Put`/`Delete`
  route without any global lock; each shard has its own `Mutex` for its data. Verified clean under
  `go test -race`.
- **Dynamic topology** — add or remove nodes at runtime via a copy-on-write swap of the topology.
- **Benchmark** — measures throughput as the shard count scales, plus a Zipfian (hot-key) workload.

## Architecture

Three layers, each independently testable:

```
Cache   (facade + routing)   cache.go   — atomic copy-on-write topology; routes each op to its shard
  │
Ring    (topology)           ring.go    — consistent hashing: key -> node, with add/remove
  │
Shard   (storage)            shard.go   — a self-contained LRU + TTL cache with its own lock
```

- **Shard** — the actual data: a `map[string]*list.Element` for O(1) lookup plus a `container/list`
  for recency order, guarded by a `sync.Mutex`. Handles eviction and TTL.
- **Ring** — pure routing, holds no data. Places each node at 128 virtual positions on a hash ring;
  a key is owned by the first node clockwise from its hash.
- **Cache** — holds an immutable topology snapshot (`ring` + `map[nodeID]*Shard`) in an
  `atomic.Pointer`. Reads load it lock-free; `AddNode`/`RemoveNode` build a *new* snapshot and swap
  the pointer atomically.

## Design notes (the parts I found interesting)

**Lock striping for concurrency.** A single global lock would serialize every operation and defeat
the purpose of sharding. Each shard has its own `Mutex`, so N shards allow up to N operations to run
in parallel — bounded by the number of CPU cores.

**Consistent hashing needed a better hash.** My first version used FNV-1a to place virtual nodes.
A distribution test caught that it clustered them badly — one node ended up owning 56% of the keyspace
instead of 33%, because FNV avalanches poorly on structured inputs like `"node#0"`, `"node#1"`, ….
Running the hash through a murmur3 finalizer fixed the avalanche and evened out the distribution.

**Lock-free reads via atomic copy-on-write.** The routing topology (ring + shard map) is stored in an
`atomic.Pointer[topology]`. `Get`/`Put`/`Delete` do a single `Load()` — no lock — then route and hit
the owning shard (which has its own lock for data). `AddNode`/`RemoveNode` serialize on a writer-only
mutex, build a brand-new immutable topology (clone the ring, copy the map), and `Store()` it atomically.
Readers always see a complete, consistent snapshot; the old one is GC'd once no reader references it.

I arrived at this by benchmarking (see below): the first version used a cache-level `RWMutex` taken
on every op, which capped throughput. The catch is that an `RWMutex` read lock still *writes* a shared
reader counter (one cache line), so it cache-line-contends across cores even though it's a "read" lock —
and that cost is paid on every op regardless of how rare writes are. Switching to a lock-free
`atomic.Load` removed it. (This is the same reason Memcached keeps its hot path on striped mutexes with
no global lock.)

**Topology changes use drop-and-rewarm.** With lock-free readers you can't hold a global lock to migrate
data atomically, so moved keys aren't migrated — they simply miss on their new shard and refetch. That's
exactly how Memcached handles resharding. (Online, key-preserving migration is on the roadmap.)

## Benchmark

Throughput vs. shard count — 64 workers, 90% reads / 10% writes, on a 16-core machine.
This shows the optimization above:

```
UNIFORM keys        v1: cache RWMutex     v2: atomic COW (lock-free reads)
  1 shard           2.56M                 2.64M
  8 shards          6.47M   (2.53x)       8.63M   (3.27x)
  16 shards         6.54M   (2.55x)      14.05M   (5.32x)
  32 shards         6.02M   (2.35x)      18.81M   (7.12x)
  ─────────────────────────────────────────────────────────
  peak              ~6.5M op/s            ~18.8M op/s   ≈ 2.9x higher
```

Two things fell out of the fix:
1. **Scaling unblocked.** Uniform throughput went from a 2.5x plateau to **7.1x**, tracking toward the
   core count — because the per-op global lock (and its cache-line contention) is gone.
2. **The hot-shard becomes visible.** With the global lock removed, per-shard contention now shows:
   uniform scales **7.1x** while a Zipfian (hot-key) workload scales only **3.2x** — the hot keys pile
   onto a few shards' locks. That gap is the motivation for the hot-key work in the roadmap.

## Running it

```bash
go test -race ./...      # full test suite under the race detector
go run ./cmd/bench       # throughput benchmark (uniform + Zipfian sweeps)
```

## Roadmap

- **Hot-key handling** — replication for read-hot keys; sharded counters for write-hot keys
  (e.g. a viral post's like counter). This is the next bottleneck the benchmark exposed.
- **Replication** for availability under node failure; **online (forward-on-miss) migration** to
  preserve keys on topology changes without drop-and-rewarm.
- **Background TTL sweeper**; byte-based capacity limits.

---

Built as a learning project — the goal was to understand distributed-cache internals deeply, so the
code favors clarity and correctness, with the one real perf optimization measured and documented above.
