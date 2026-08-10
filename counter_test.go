package distcache

import (
	"sync"
	"testing"
)

func TestShardAdd(t *testing.T) {
	s := NewShard(100)
	if got := s.Add("c", 1); got != 1 {
		t.Fatalf("Add on missing key = %d, want 1", got)
	}
	if got := s.Add("c", 4); got != 5 {
		t.Fatalf("Add = %d, want 5", got)
	}
	if got := s.Add("c", -2); got != 3 {
		t.Fatalf("Add = %d, want 3", got)
	}
	if v, ok := s.Get("c"); !ok || string(v) != "3" {
		t.Fatalf("Get(c) = %q ok=%v, want \"3\"", v, ok)
	}
}

func TestCounterSumsSubCounters(t *testing.T) {
	c := NewCache(8, 100000, 128, WithCounterShards(8))
	defer c.Close()
	const n = 1000
	for i := 0; i < n; i++ {
		c.Incr("likes", 1)
	}
	if got := c.GetCounter("likes"); got != n {
		t.Fatalf("GetCounter = %d, want %d", got, n)
	}
	topo := c.topo.Load()
	shardsSeen := map[*Shard]bool{}
	for i := 0; i < 8; i++ {
		ck := counterKey("likes", i)
		if s := topo.shardFor(ck); s != nil {
			if _, ok := s.Get(ck); ok {
				shardsSeen[s] = true
			}
		}
	}
	if len(shardsSeen) < 2 {
		t.Errorf("sub-counters should spread across shards, got %d", len(shardsSeen))
	}
}

func TestCounterConcurrent(t *testing.T) {
	c := NewCache(8, 100000, 128, WithCounterShards(8))
	defer c.Close()
	const goroutines, per = 50, 2000
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < per; i++ {
				c.Incr("likes", 1)
			}
		}()
	}
	wg.Wait()
	if got, want := c.GetCounter("likes"), int64(goroutines*per); got != want {
		t.Fatalf("GetCounter = %d, want %d (lost updates?)", got, want)
	}
}

func TestDetectorTracksReadsNotWrites(t *testing.T) {
	c := NewCache(8, 100000, 128, WithHotKeyThreshold(1000, 0.001, 0.0005))
	c.detector.Close() // stop the loop; drive the detector manually
	c.detector.sample = func() bool { return true }

	for i := 0; i < 200; i++ {
		c.Put("wkey", []byte("v"), 0) // writes only → must NOT feed the detector
	}
	for i := 0; i < 200; i++ {
		c.Get("rkey") // reads only → feed the detector
	}
	c.detector.decayOnce()

	if c.detector.isHot("wkey") {
		t.Error("write-only key should NOT be flagged read-hot")
	}
	if !c.detector.isHot("rkey") {
		t.Error("read-hot key should be flagged")
	}
}
