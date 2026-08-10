package distcache

import (
	"container/list"
	"strconv"
	"sync"
	"time"
)

// entry is one cached item. It's stored as the Value of a list.Element.
// We duplicate `key` here so that when we evict the LRU (the back of the
// list) we know which map key to delete — the list node gives us the entry
// but not the map key, so we keep the key on hand.
type entry struct {
	key       string
	value     []byte
	expiresAt time.Time // zero value = no expiry
}

// expired reports whether the entry has passed its TTL.
func (e *entry) expired(now time.Time) bool {
	return !e.expiresAt.IsZero() && now.After(e.expiresAt)
}

// Shard is a single, self-contained LRU + TTL cache protected by one lock.
// N shards behind a ring make up the distributed cache. Each shard is
// independent — its lock protects only its own data.
type Shard struct {
	mu       sync.Mutex
	store    map[string]*list.Element // key -> node in ll  (O(1) lookup)
	ll       *list.List               // front = most-recently-used, back = LRU victim
	capacity int                      // max number of entries this shard holds

	hits, misses, writes uint64 // activity counters, maintained under mu
}

// NewShard creates an empty shard that holds up to `capacity` entries.
func NewShard(capacity int) *Shard {
	return &Shard{
		store:    make(map[string]*list.Element),
		ll:       list.New(),
		capacity: capacity,
	}
}

// Get returns the value for key and true if it's present and not expired.
// A hit is marked most-recently-used. Safe for concurrent use.
func (s *Shard) Get(key string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	elem, ok := s.store[key]
	if !ok {
		s.misses++
		return nil, false // miss — key not present
	}

	ent := elem.Value.(*entry)
	if ent.expired(time.Now()) {
		// lazy expiry: an expired entry counts as a miss, and we drop it
		// from BOTH structures so it doesn't linger.
		s.ll.Remove(elem)
		delete(s.store, key)
		s.misses++
		return nil, false
	}

	s.ll.MoveToFront(elem) // hit → mark most-recently-used
	s.hits++
	return ent.value, true
}

// Put inserts or updates key with value and an optional TTL.
// ttl <= 0 means the entry never expires. Safe for concurrent use.
func (s *Shard) Put(key string, value []byte, ttl time.Duration) {
	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl)
	}
	s.putEntry(key, value, expiresAt)
}

// putEntry inserts or updates key with an ABSOLUTE expiry time. Used by Put
// (which converts a ttl) and by migration (which preserves the original
// expiry). Safe for concurrent use.
func (s *Shard) putEntry(key string, value []byte, expiresAt time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.putEntryLocked(key, value, expiresAt)
}

// putEntryLocked is putEntry without locking; the caller must already hold s.mu.
func (s *Shard) putEntryLocked(key string, value []byte, expiresAt time.Time) {
	s.writes++

	// Update path: key already present → refresh value + TTL, mark recent.
	if elem, ok := s.store[key]; ok {
		ent := elem.Value.(*entry)
		ent.value = value
		ent.expiresAt = expiresAt
		s.ll.MoveToFront(elem)
		return
	}

	// Insert path: new key → push to front, add to map.
	ent := &entry{key: key, value: value, expiresAt: expiresAt}
	elem := s.ll.PushFront(ent)
	s.store[key] = elem

	// Enforce capacity → drop the least-recently-used (back of the list).
	if s.ll.Len() > s.capacity {
		s.evictLRU()
	}
}

// Add atomically adds delta to the integer counter at key and returns the new
// value. A missing or expired key starts from 0. The counter is stored as its
// decimal string (so it reuses the LRU/TTL machinery) and never expires.
// Safe for concurrent use.
func (s *Shard) Add(key string, delta int64) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	var cur int64
	if elem, ok := s.store[key]; ok {
		if ent := elem.Value.(*entry); !ent.expired(time.Now()) {
			cur, _ = strconv.ParseInt(string(ent.value), 10, 64)
		}
	}
	cur += delta
	s.putEntryLocked(key, []byte(strconv.FormatInt(cur, 10)), time.Time{})
	return cur
}

// evictLRU removes the least-recently-used entry (the back of the list).
// The caller must already hold s.mu.
func (s *Shard) evictLRU() {
	elem := s.ll.Back()
	if elem == nil {
		return
	}
	ent := elem.Value.(*entry)
	s.ll.Remove(elem)
	delete(s.store, ent.key) // uses the key we stashed in the entry
}

// Delete removes key from the shard. Returns true if the key was present.
// Safe for concurrent use.
func (s *Shard) Delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	elem, ok := s.store[key]
	if !ok {
		return false // nothing to delete
	}
	s.ll.Remove(elem)
	delete(s.store, key)
	return true
}

// Len returns the current number of entries in the shard.
func (s *Shard) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ll.Len()
}

// ShardStat is a point-in-time snapshot of one shard's activity.
type ShardStat struct {
	Hits, Misses, Writes uint64
}

// Ops is the total operations this shard has served.
func (s ShardStat) Ops() uint64 { return s.Hits + s.Misses + s.Writes }

// stat returns a snapshot of the shard's activity counters.
func (s *Shard) stat() ShardStat {
	s.mu.Lock()
	defer s.mu.Unlock()
	return ShardStat{Hits: s.hits, Misses: s.misses, Writes: s.writes}
}

// snapshot returns a copy of all NON-expired entries in the shard. Used by
// migration to move keys to a new owner. Safe for concurrent use.
func (s *Shard) snapshot() []entry {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	out := make([]entry, 0, len(s.store))
	for _, elem := range s.store {
		e := elem.Value.(*entry)
		if e.expired(now) {
			continue // skip expired — they'd be dropped anyway
		}
		out = append(out, *e) // value copy
	}
	return out
}
