package distcache

import (
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
	capacity int
}

// NewCache builds a cache with numShards shards, each holding up to
// shardCapacity entries, using `vnodes` virtual nodes per shard on the ring.
func NewCache(numShards, shardCapacity, vnodes int) *Cache {
	ring := NewRing(vnodes)
	shards := make(map[string]*Shard, numShards)
	for i := 0; i < numShards; i++ {
		id := "shard-" + strconv.Itoa(i)
		shards[id] = NewShard(shardCapacity)
		ring.AddNode(id)
	}
	c := &Cache{capacity: shardCapacity}
	c.topo.Store(&topology{ring: ring, shards: shards})
	return c
}

// Get routes to the owning shard (lock-free topology load).
func (c *Cache) Get(key string) ([]byte, bool) {
	s := c.topo.Load().shardFor(key)
	if s == nil {
		return nil, false
	}
	return s.Get(key)
}

// Put routes to the owning shard (lock-free topology load).
func (c *Cache) Put(key string, value []byte, ttl time.Duration) {
	s := c.topo.Load().shardFor(key)
	if s == nil {
		return
	}
	s.Put(key, value, ttl)
}

// Delete routes to the owning shard (lock-free topology load).
func (c *Cache) Delete(key string) bool {
	s := c.topo.Load().shardFor(key)
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
