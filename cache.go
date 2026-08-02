package distcache

import (
	"strconv"
	"sync"
	"time"
)

// Cache is the facade: it holds the routing Ring plus a set of Shards
// (nodeID -> *Shard) and dispatches each operation to the owning shard.
//
// The RWMutex protects the TOPOLOGY (ring + shards map), NOT shard data
// (each shard has its own lock). Get/Put/Delete only READ the topology, so
// they take RLock and run concurrently; AddNode/RemoveNode MUTATE it, so
// they take the exclusive Lock.
type Cache struct {
	mu       sync.RWMutex
	ring     *Ring
	shards   map[string]*Shard
	capacity int // per-shard capacity (used when adding shards)
}

// NewCache builds a cache with numShards shards, each holding up to
// shardCapacity entries, using `vnodes` virtual nodes per shard on the ring.
func NewCache(numShards, shardCapacity, vnodes int) *Cache {
	c := &Cache{
		ring:     NewRing(vnodes),
		shards:   make(map[string]*Shard, numShards),
		capacity: shardCapacity,
	}
	for i := 0; i < numShards; i++ {
		id := "shard-" + strconv.Itoa(i)
		c.shards[id] = NewShard(shardCapacity)
		c.ring.AddNode(id)
	}
	return c
}

// shardFor returns the shard that owns key (or nil if the ring is empty).
// Caller must hold at least c.mu.RLock (reads the ring + shards map).
func (c *Cache) shardFor(key string) *Shard {
	nodeID, ok := c.ring.GetNode(key)
	if !ok {
		return nil
	}
	return c.shards[nodeID]
}

// Get routes to the owning shard.
func (c *Cache) Get(key string) ([]byte, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	s := c.shardFor(key)
	if s == nil {
		return nil, false
	}
	return s.Get(key)
}

// Put routes to the owning shard.
func (c *Cache) Put(key string, value []byte, ttl time.Duration) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	s := c.shardFor(key)
	if s == nil {
		return
	}
	s.Put(key, value, ttl)
}

// Delete routes to the owning shard.
func (c *Cache) Delete(key string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	s := c.shardFor(key)
	if s == nil {
		return false
	}
	return s.Delete(key)
}

// Len returns the total number of entries across all shards.
func (c *Cache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	total := 0
	for _, s := range c.shards {
		total += s.Len()
	}
	return total
}

// AddNode adds a new shard and migrates the keys that now belong to it.
// Stop-the-world: holds the exclusive lock for the whole migration, so it's
// atomic w.r.t. all other operations. No-op if the node already exists.
func (c *Cache) AddNode(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.shards[id]; exists {
		return
	}
	newShard := NewShard(c.capacity)
	c.shards[id] = newShard
	c.ring.AddNode(id)

	// Any key whose owner is now `id` moves from its old shard to the new one.
	// (Consistent hashing => adding a node only moves keys TO it.)
	for sid, s := range c.shards {
		if sid == id {
			continue
		}
		for _, e := range s.snapshot() {
			if owner, _ := c.ring.GetNode(e.key); owner == id {
				newShard.putEntry(e.key, e.value, e.expiresAt) // preserves TTL
				s.Delete(e.key)
			}
		}
	}
}

// RemoveNode removes a shard and redistributes its keys to their new owners.
// Stop-the-world: atomic under the exclusive lock. No-op if not present.
func (c *Cache) RemoveNode(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	removed, exists := c.shards[id]
	if !exists {
		return
	}
	entries := removed.snapshot() // capture BEFORE we change the ring
	c.ring.RemoveNode(id)         // now GetNode returns the NEW owners
	delete(c.shards, id)

	// Redistribute the removed shard's keys to their new owners.
	for _, e := range entries {
		if owner, ok := c.ring.GetNode(e.key); ok {
			c.shards[owner].putEntry(e.key, e.value, e.expiresAt)
		}
		// if !ok, the ring is now empty => entry is dropped
	}
}
