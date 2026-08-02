package distcache

import (
	"fmt"
	"sync"
	"testing"
)

func TestCacheBasic(t *testing.T) {
	c := NewCache(4, 100, 128)
	c.Put("a", []byte("1"), 0)
	if got, ok := c.Get("a"); !ok || string(got) != "1" {
		t.Fatalf("Get(a) = %q ok=%v, want 1", got, ok)
	}
	if _, ok := c.Get("missing"); ok {
		t.Fatal("expected miss for absent key")
	}
	if !c.Delete("a") {
		t.Fatal("Delete(a) should be true")
	}
	if _, ok := c.Get("a"); ok {
		t.Fatal("a should be gone after delete")
	}
}

func TestCacheSpreadsAcrossShards(t *testing.T) {
	const numShards = 8
	c := NewCache(numShards, 100000, 128) // big capacity so nothing evicts
	for i := 0; i < 10000; i++ {
		c.Put(fmt.Sprintf("key-%d", i), []byte("v"), 0)
	}
	// every shard should hold some keys (keys spread across shards)
	nonEmpty := 0
	for _, s := range c.shards {
		if s.Len() > 0 {
			nonEmpty++
		}
	}
	if nonEmpty != numShards {
		t.Errorf("only %d/%d shards hold keys — distribution is uneven", nonEmpty, numShards)
	}
	if c.Len() != 10000 {
		t.Errorf("Len = %d, want 10000 (big capacity, no eviction)", c.Len())
	}
}

func TestCacheConcurrent(t *testing.T) {
	c := NewCache(8, 1000, 128)
	const goroutines, iters = 50, 2000
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				key := fmt.Sprintf("key-%d", (seed*iters+i)%5000)
				switch i % 3 {
				case 0:
					c.Put(key, []byte("v"), 0)
				case 1:
					c.Get(key)
				case 2:
					c.Delete(key)
				}
			}
		}(g)
	}
	wg.Wait()
}

func TestAddNodeMigratesKeys(t *testing.T) {
	c := NewCache(4, 1000000, 128) // big capacity — no eviction
	const n = 5000
	for i := 0; i < n; i++ {
		c.Put(fmt.Sprintf("key-%d", i), []byte(fmt.Sprintf("v%d", i)), 0)
	}
	if c.Len() != n {
		t.Fatalf("Len before = %d, want %d", c.Len(), n)
	}

	c.AddNode("shard-new")

	// every key must still be retrievable — nothing lost in migration
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%d", i)
		got, ok := c.Get(key)
		if !ok || string(got) != fmt.Sprintf("v%d", i) {
			t.Fatalf("after AddNode, key %s lost: got %q ok=%v", key, got, ok)
		}
	}
	if c.Len() != n {
		t.Errorf("Len after AddNode = %d, want %d (no keys lost)", c.Len(), n)
	}
	if c.shards["shard-new"].Len() == 0 {
		t.Error("new shard received no keys — migration didn't move anything")
	}
}

func TestRemoveNodeMigratesKeys(t *testing.T) {
	c := NewCache(4, 1000000, 128)
	const n = 5000
	for i := 0; i < n; i++ {
		c.Put(fmt.Sprintf("key-%d", i), []byte(fmt.Sprintf("v%d", i)), 0)
	}

	c.RemoveNode("shard-0")

	if _, exists := c.shards["shard-0"]; exists {
		t.Fatal("shard-0 should be removed from the map")
	}
	// every key must still be retrievable — redistributed to other shards
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%d", i)
		got, ok := c.Get(key)
		if !ok || string(got) != fmt.Sprintf("v%d", i) {
			t.Fatalf("after RemoveNode, key %s lost: got %q ok=%v", key, got, ok)
		}
	}
	if c.Len() != n {
		t.Errorf("Len after RemoveNode = %d, want %d (no keys lost)", c.Len(), n)
	}
}

// TestConcurrentWithTopologyChange runs ops while nodes are added/removed.
// Run with -race to catch topology/data races (the cache RWMutex).
func TestConcurrentWithTopologyChange(t *testing.T) {
	c := NewCache(4, 10000, 128)
	var wg sync.WaitGroup

	for g := 0; g < 20; g++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				key := fmt.Sprintf("key-%d", (seed*2000+i)%3000)
				switch i % 3 {
				case 0:
					c.Put(key, []byte("v"), 0)
				case 1:
					c.Get(key)
				case 2:
					c.Delete(key)
				}
			}
		}(g)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			c.AddNode(fmt.Sprintf("extra-%d", i))
			c.RemoveNode(fmt.Sprintf("extra-%d", i))
		}
	}()

	wg.Wait()
}
