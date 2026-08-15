# distributed-cache-go

A distributed in-memory key-value cache built in Go from scratch — LRU eviction, TTL,
consistent-hashing sharding with virtual nodes, **lock-free concurrent reads**, live
add/remove of nodes, and **hot-key handling** (detect-then-replicate/shard).

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
- **Hot-key handling** — a sampled heavy-hitters detector (Space-Saving) finds hot keys online;
  **read-hot** keys are replicated across shards so their reads spread, and **write-hot** counters are
  sharded so their writes spread. Hotness checks are lock-free.
- **Benchmark** — measures throughput as the shard count scales, plus a Zipfian (hot-key) workload.

## Architecture

Three layers, each independently testable:

```
Cache    (facade + routing)   cache.go     — atomic COW topology; routing; hot-key replication + counters
  ├─ Detector (hot keys)      detector.go  — Space-Saving heavy hitters, sampled, lock-free hot set
  │
Ring     (topology)           ring.go      — consistent hashing: key -> node, with add/remove
  │
Shard    (storage)            shard.go     — self-contained LRU + TTL cache, own lock, activity stats
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

**Hot-key detection (heavy hitters, off the hot path).** Finding hot keys is the streaming *heavy-hitters*
problem. I use **Space-Saving** (bounded memory, enumerable top-K) fed by a **1/16 sampler** (a per-goroutine
`math/rand/v2` bitmask — no shared counter), so the detector's lock sees ~1/16 of traffic. A background
goroutine **decays** counts each tick (so "hot" means *recent*) and republishes an immutable hot set through
an `atomic.Pointer` — the same copy-on-write idiom as the topology, so `isHot` is a lock-free read. Only
reads feed the detector, so "hot" means read-hot.

**Read-hot → copy; write-hot → split.** Opposite problems. A read-hot key (a viral post's views) is
**replicated** across R shards via derived keys `k#0..#(R-1)`; reads pick a random replica (falling back to
the primary + backfilling on a miss). A write-hot key (a like counter) *can't* be replicated — every write
would hit every copy — so it's **sharded**: `Incr` adds to a random sub-counter, `GetCounter` sums them,
spreading writes across R locks.

**The promote threshold is measured, not guessed.** A key is promoted when its estimated rate crosses a
fraction of the **measured single-shard throughput** (~2.6M op/s). The fraction is set by queueing theory
(keep per-key shard utilization low so tail latency stays flat), with a **hysteresis gap** so a borderline
key doesn't flap between states.

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
   onto a few shards' locks. That gap is what the hot-key handling below addresses.

### Hot-key handling

Read replication helps read-heavy skew (its target) and costs on write-heavy skew (writes fan out to R+1
shards — which is exactly why write-hot keys use *sharded counters* instead of replication):

```
ZIPFIAN 99% read / 1% write, s=1.5          OFF        R=4        R=8
  throughput                                8.06M      8.40M      9.71M  (1.20x)

Hottest shard's share of ops (ideal 6.2%)   OFF 40.2%    →    R=8 13.2%

Sharded counter — one hot counter, incr/s   1 sub: 3.46M  →  16 sub: 8.86M  (2.56x)
```

Throughput rises a modest **1.2x**, but the hottest shard's load drops **~3× (40% → 13%)** — the real
bottleneck relief that a single throughput number hides. A write-hot counter scales **2.56×** by splitting
across sub-counters. (The detector itself adds ~0% overhead — sampling keeps its lock off the hot path.)

## Running it

```bash
go test -race ./...      # full test suite under the race detector
go run ./cmd/bench       # throughput sweeps + hot-key replication / counter / hot-shard-load comparisons
```

## Roadmap

- ✅ **Hot-key handling (done)** — Space-Saving detection, read-hot replication, write-hot sharded
  counters; cut the hottest shard's load from 40% → 13% on a skewed workload.
- **Replication** for availability under node failure; **online (forward-on-miss) migration** to
  preserve keys on topology changes without drop-and-rewarm.
- **Background TTL sweeper**; byte-based capacity limits.

---

Built as a learning project — the goal was to understand distributed-cache internals deeply, so the
code favors clarity and correctness, with the one real perf optimization measured and documented above.
