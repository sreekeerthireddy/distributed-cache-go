package distcache

import (
	"math/rand/v2"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// topology is an IMMUTABLE snapshot of routing state (ring + shard map). It is
// never mutated after publication — AddNode/RemoveNode build a NEW topology and
// swap the pointer atomically. This makes Get/Put/Delete lock-free.
type topology struct {
	ring   *Ring
	shards map[string]*Shard
}

func (t *topology) shardFor(key string) *Shard {
	id, ok := t.ring.GetNode(key)
	if !ok {
		return nil
	}
	return t.shards[id]
}

// Cache holds the current topology in an atomic.Pointer so reads route
// lock-free; writers (AddNode/RemoveNode) serialize on writeMu and swap in a
// new topology (copy-on-write).
type Cache struct {
	topo     atomic.Pointer[topology]
	writeMu  sync.Mutex // serializes AddNode/RemoveNode (writers only)
	capacity      int
	detector      *detector // nil when hot keys disabled
	replicas      int       // R: replicas per hot key
	replicaTTL    time.Duration
	counterShards int // sub-counters per sharded counter
}

// NewCache builds a cache with numShards shards, each holding up to
// shardCapacity entries, using `vnodes` virtual nodes per shard on the ring.
// By default it also runs a hot-key detector (see options.go); disable with
// WithHotKeys(false). Call Close when done to stop the detector's decay loop.
func NewCache(numShards, shardCapacity, vnodes int, opts ...Option) *Cache {
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}

	ring := NewRing(vnodes)
	shards := make(map[string]*Shard, numShards)
	for i := 0; i < numShards; i++ {
		id := "shard-" + strconv.Itoa(i)
		shards[id] = NewShard(shardCapacity)
		ring.AddNode(id)
	}
	c := &Cache{capacity: shardCapacity, replicas: o.replicas,
		replicaTTL: o.replicaTTL, counterShards: o.counterShards}
	c.topo.Store(&topology{ring: ring, shards: shards})

	if o.hotKeys {
		promote := rateToCount(o.promoteFrac*o.shardMaxRate, o.decayInterval, o.decayFactor, o.sampleRate)
		demote := rateToCount(o.demoteFrac*o.shardMaxRate, o.decayInterval, o.decayFactor, o.sampleRate)
		c.detector = newDetector(detectorConfig{
			capacity: o.ssCapacity, sampleRate: o.sampleRate, interval: o.decayInterval,
			decayFactor: o.decayFactor, promote: promote, demote: demote, maxHot: o.maxHot,
		})
		c.detector.start()
	}
	return c
}

// Close releases background resources (the hot-key detector's decay loop).
// Call exactly once; the cache must not be used afterward.
func (c *Cache) Close() {
	if c.detector != nil {
		c.detector.Close()
	}
}

// Get routes to the owning shard (lock-free topology load). Hot keys are served
// from a random replica, falling back to the primary on a miss.
func (c *Cache) Get(key string) ([]byte, bool) {
	if c.detector != nil {
		c.detector.record(key)
	}
	topo := c.topo.Load()
	if c.replicate(key) {
		return c.getReplicated(topo, key)
	}
	s := topo.shardFor(key)
	if s == nil {
		return nil, false
	}
	return s.Get(key)
}

// Put routes to the owning shard (lock-free topology load). Hot keys are written
// to the primary plus all replicas. Writes do NOT feed the detector — "hot" means
// read-hot, so replication targets read-heavy keys (write-hot keys use Incr).
func (c *Cache) Put(key string, value []byte, ttl time.Duration) {
	topo := c.topo.Load()
	if c.replicate(key) {
		c.putReplicated(topo, key, value, ttl)
		return
	}
	if s := topo.shardFor(key); s != nil {
		s.Put(key, value, ttl)
	}
}

// Delete routes to the owning shard (lock-free topology load). Hot keys are
// cleared from the primary and all replicas.
func (c *Cache) Delete(key string) bool {
	topo := c.topo.Load()
	if c.replicate(key) {
		return c.deleteReplicated(topo, key)
	}
	s := topo.shardFor(key)
	if s == nil {
		return false
	}
	return s.Delete(key)
}

// Len returns the total number of entries across all shards.
func (c *Cache) Len() int {
	t := c.topo.Load()
	total := 0
	for _, s := range t.shards {
		total += s.Len()
	}
	return total
}

// replicaKey derives the i-th replica key for a hot key. The NUL separator makes
// a collision with a real user key practically impossible.
func replicaKey(key string, i int) string {
	return key + "\x00" + strconv.Itoa(i)
}

// replicate reports whether key should be served from replicas right now.
func (c *Cache) replicate(key string) bool {
	return c.detector != nil && c.replicas > 1 && c.detector.isHot(key)
}

// getReplicated reads a hot key: try one random replica, fall back to the primary
// (and backfill that replica) on a miss. All routing uses the passed-in snapshot.
func (c *Cache) getReplicated(topo *topology, key string) ([]byte, bool) {
	rk := replicaKey(key, rand.IntN(c.replicas))
	rs := topo.shardFor(rk)
	if rs != nil {
		if v, ok := rs.Get(rk); ok {
			return v, true // replica hit → the read landed on a spread-out shard
		}
	}
	ps := topo.shardFor(key) // replica miss → primary is the source of truth
	if ps == nil {
		return nil, false
	}
	v, ok := ps.Get(key)
	if ok && rs != nil {
		rs.Put(rk, v, c.replicaTTL) // backfill so future reads hit the replica
	}
	return v, ok
}

// putReplicated writes the canonical primary plus all replicas (bounded TTL).
func (c *Cache) putReplicated(topo *topology, key string, value []byte, ttl time.Duration) {
	if ps := topo.shardFor(key); ps != nil {
		ps.Put(key, value, ttl) // canonical copy keeps the caller's TTL
	}
	rttl := c.replicaTTL
	if ttl > 0 && ttl < rttl {
		rttl = ttl // never outlive the primary
	}
	for i := 0; i < c.replicas; i++ {
		rk := replicaKey(key, i)
		if rs := topo.shardFor(rk); rs != nil {
			rs.Put(rk, value, rttl)
		}
	}
}

// deleteReplicated clears the primary and every replica. Returns whether the
// primary existed.
func (c *Cache) deleteReplicated(topo *topology, key string) bool {
	ok := false
	if ps := topo.shardFor(key); ps != nil {
		ok = ps.Delete(key)
	}
	for i := 0; i < c.replicas; i++ {
		rk := replicaKey(key, i)
		if rs := topo.shardFor(rk); rs != nil {
			rs.Delete(rk)
		}
	}
	return ok
}

// counterKey derives the i-th sub-counter key. The NUL + "c" namespace avoids
// collisions with real user keys and with replica keys.
func counterKey(key string, i int) string {
	return key + "\x00c" + strconv.Itoa(i)
}

// Incr adds delta to key's sharded counter by hitting a random sub-counter,
// spreading write load across counterShards shards. A counter is split, never
// replicated — so it bypasses the detector entirely. Use GetCounter for the total.
func (c *Cache) Incr(key string, delta int64) {
	topo := c.topo.Load()
	ck := counterKey(key, rand.IntN(c.counterShards))
	if s := topo.shardFor(ck); s != nil {
		s.Add(ck, delta)
	}
}

// GetCounter returns the logical total: the sum of all sub-counters. Reads touch
// counterShards shards, which is fine because counter reads are far rarer than
// increments (the whole reason to shard the writes).
func (c *Cache) GetCounter(key string) int64 {
	topo := c.topo.Load()
	var total int64
	for i := 0; i < c.counterShards; i++ {
		ck := counterKey(key, i)
		if s := topo.shardFor(ck); s != nil {
			if v, ok := s.Get(ck); ok {
				n, _ := strconv.ParseInt(string(v), 10, 64)
				total += n
			}
		}
	}
	return total
}

// AddNode adds a shard via copy-on-write, then atomically swaps the topology.
// Drop-and-rewarm: no data migration — keys that now route to the new shard
// miss and refetch. No-op if the node already exists.
func (c *Cache) AddNode(id string) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	old := c.topo.Load()
	if _, exists := old.shards[id]; exists {
		return
	}
	newRing := old.ring.Clone()
	newRing.AddNode(id)

	newShards := make(map[string]*Shard, len(old.shards)+1)
	for k, v := range old.shards {
		newShards[k] = v
	}
	newShards[id] = NewShard(c.capacity)

	c.topo.Store(&topology{ring: newRing, shards: newShards})
}

// RemoveNode removes a shard via copy-on-write. Drop-and-rewarm: the removed
// shard's keys are dropped (reroute to other shards -> miss -> refetch).
// No-op if not present.
func (c *Cache) RemoveNode(id string) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	old := c.topo.Load()
	if _, exists := old.shards[id]; !exists {
		return
	}
	newRing := old.ring.Clone()
	newRing.RemoveNode(id)

	newShards := make(map[string]*Shard, len(old.shards)-1)
	for k, v := range old.shards {
		if k != id {
			newShards[k] = v
		}
	}
	c.topo.Store(&topology{ring: newRing, shards: newShards})
}
