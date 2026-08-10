package distcache

import (
	"fmt"
	"sync"
	"testing"
)

// hotCache builds a cache where "hot" is forced into the hot set. It stops the
// decay loop first so the forced hot set stays put (tests don't depend on timing).
func hotCache(shards, replicas int) *Cache {
	c := NewCache(shards, 100000, 128, WithReplicas(replicas))
	c.detector.Close()
	hs := hotSet{"hot": {}}
	c.detector.hot.Store(&hs)
	return c
}

func TestReadReplicationWritesPrimaryAndReplicas(t *testing.T) {
	c := hotCache(8, 4)
	c.Put("hot", []byte("v"), 0)

	topo := c.topo.Load()
	if v, ok := topo.shardFor("hot").Get("hot"); !ok || string(v) != "v" {
		t.Fatal("primary should hold the value")
	}
	shardsSeen := map[*Shard]bool{}
	for i := 0; i < 4; i++ {
		rk := replicaKey("hot", i)
		s := topo.shardFor(rk)
		if v, ok := s.Get(rk); !ok || string(v) != "v" {
			t.Errorf("replica %d missing", i)
		}
		shardsSeen[s] = true
	}
	if len(shardsSeen) < 2 {
		t.Errorf("replicas should spread across multiple shards, got %d", len(shardsSeen))
	}
}

func TestReadReplicationFallbackAndBackfill(t *testing.T) {
	c := hotCache(8, 4)
	topo := c.topo.Load()
	// just-promoted key: only the primary is warm, replicas cold
	topo.shardFor("hot").Put("hot", []byte("v"), 0)

	if v, ok := c.Get("hot"); !ok || string(v) != "v" {
		t.Fatalf("expected fallback hit, got %q %v", v, ok)
	}
	for i := 0; i < 100; i++ {
		c.Get("hot")
	}
	warm := 0
	for i := 0; i < 4; i++ {
		rk := replicaKey("hot", i)
		if _, ok := topo.shardFor(rk).Get(rk); ok {
			warm++
		}
	}
	if warm == 0 {
		t.Error("expected at least one replica to be backfilled after reads")
	}
}

func TestReadReplicationDeleteClearsAll(t *testing.T) {
	c := hotCache(8, 4)
	c.Put("hot", []byte("v"), 0)
	if !c.Delete("hot") {
		t.Fatal("Delete should report the primary existed")
	}
	if _, ok := c.Get("hot"); ok {
		t.Error("primary should be gone after delete")
	}
	topo := c.topo.Load()
	for i := 0; i < 4; i++ {
		rk := replicaKey("hot", i)
		if _, ok := topo.shardFor(rk).Get(rk); ok {
			t.Errorf("replica %d should be gone after delete", i)
		}
	}
}

func TestReadReplicationColdKeyNotReplicated(t *testing.T) {
	c := hotCache(8, 4) // only "hot" is hot
	c.Put("cold", []byte("v"), 0)

	topo := c.topo.Load()
	if v, ok := c.Get("cold"); !ok || string(v) != "v" {
		t.Fatal("cold key should be readable via primary")
	}
	for i := 0; i < 4; i++ {
		rk := replicaKey("cold", i)
		if _, ok := topo.shardFor(rk).Get(rk); ok {
			t.Errorf("cold key should NOT create replica %d", i)
		}
	}
}

func TestReadReplicationConcurrent(t *testing.T) {
	c := hotCache(8, 4)
	var wg sync.WaitGroup
	for g := 0; g < 20; g++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				switch i % 4 {
				case 0:
					c.Put("hot", []byte("v"), 0)
				case 1, 2:
					c.Get("hot")
				case 3:
					c.Get(fmt.Sprintf("k-%d", i%100))
				}
			}
		}(g)
	}
	wg.Wait()
	if v, ok := c.Get("hot"); !ok || string(v) != "v" {
		t.Errorf("hot key should be readable after concurrent load, got %q %v", v, ok)
	}
}
