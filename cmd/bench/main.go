package main

import (
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"time"

	distcache "github.com/sreekeerthireddy/distributed-cache-go"
)

const (
	capacity = 200000 // > numKeys, so nothing evicts (isolates the concurrency effect)
	vnodes   = 128
	workers  = 64
	opsPerWk = 100000
	numKeys  = 10000
)

func main() {
	fmt.Printf("GOMAXPROCS = %d | workers = %d | ops/worker = %d | distinct keys = %d\n\n",
		runtime.GOMAXPROCS(0), workers, opsPerWk, numKeys)
	shardCounts := []int{1, 2, 4, 8, 16, 32}

	fmt.Println("=== UNIFORM keys — throughput scales with #shards (lock striping) ===")
	sweep(shardCounts, uniformKeygen)

	fmt.Println("\n=== ZIPFIAN keys (hot keys) — throughput scales LESS (hot shard) ===")
	sweep(shardCounts, zipfianKeygen)

	compareReplication()
}

// compareReplication measures the Phase-2 payoff: Zipfian throughput with hot-key
// read replication OFF vs ON, at a fixed shard count. Runs long enough for the
// detector to engage (fast decay interval on the ON case).
func compareReplication() {
	const shards = 16
	const opsPerWorker = 500000 // several seconds — lets detection reach steady state

	fastDecay := distcache.WithDecayInterval(200 * time.Millisecond)
	for _, wl := range []struct {
		name     string
		writePct int
		skew     float64
	}{
		{"90% read / 10% write, s=1.3", 10, 1.3},
		{"99% read /  1% write, s=1.3", 1, 1.3},
		{"99% read /  1% write, s=1.5", 1, 1.5},
	} {
		fmt.Printf("\n=== read-hot replication OFF vs ON (16 shards) — %s ===\n", wl.name)
		off := runZipf(shards, opsPerWorker, wl.writePct, wl.skew, distcache.WithHotKeys(false))
		on4 := runZipf(shards, opsPerWorker, wl.writePct, wl.skew, fastDecay, distcache.WithReplicas(4))
		on8 := runZipf(shards, opsPerWorker, wl.writePct, wl.skew, fastDecay, distcache.WithReplicas(8))
		fmt.Printf("%-28s %-18s\n", "config", "throughput(op/s)")
		fmt.Printf("%-28s %-18.0f\n", "OFF", off)
		fmt.Printf("%-28s %-18.0f (%.2fx)\n", "replication ON (R=4)", on4, on4/off)
		fmt.Printf("%-28s %-18.0f (%.2fx)\n", "replication ON (R=8)", on8, on8/off)
	}

	compareCounter()
	compareHotShardLoad()
}

// compareHotShardLoad shows replication's real effect: the hottest shard's share
// of total ops drops as the hot key's reads spread across replicas.
func compareHotShardLoad() {
	const shards = 16
	const opsPerWorker = 500000

	fmt.Println("\n=== hot-shard load: hottest shard's share of ops (Zipfian 99/1 s=1.5, 16 shards) ===")
	fmt.Printf("(ideal share with 16 shards = %.1f%%)\n", 100.0/16)
	off := hotShardShare(shards, opsPerWorker, distcache.WithHotKeys(false))
	on := hotShardShare(shards, opsPerWorker,
		distcache.WithDecayInterval(200*time.Millisecond), distcache.WithReplicas(8))
	fmt.Printf("%-24s %.1f%%\n", "replication OFF", off*100)
	fmt.Printf("%-24s %.1f%%\n", "replication ON (R=8)", on*100)
}

func hotShardShare(numShards, opsPerWorker int, opts ...distcache.Option) float64 {
	c := distcache.NewCache(numShards, capacity, vnodes, opts...)
	defer c.Close()
	for i := 0; i < numKeys; i++ {
		c.Put(fmt.Sprintf("key-%d", i), []byte("v"), 0)
	}
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(id) + 1))
			z := rand.NewZipf(r, 1.5, 1, uint64(numKeys-1))
			for i := 0; i < opsPerWorker; i++ {
				key := fmt.Sprintf("key-%d", z.Uint64())
				if r.Intn(100) < 1 {
					c.Put(key, []byte("v"), 0)
				} else {
					c.Get(key)
				}
			}
		}(w)
	}
	wg.Wait()

	var total, maxOps uint64
	for _, s := range c.ShardStats() {
		total += s.Ops()
		if s.Ops() > maxOps {
			maxOps = s.Ops()
		}
	}
	return float64(maxOps) / float64(total)
}

// compareCounter measures the write-hot payoff: increments/sec on ONE logical
// counter as it's split into more sub-counters (spreading the write across shard
// locks). counterShards=1 is the naive single-counter baseline.
func compareCounter() {
	const shards = 16
	const opsPerWorker = 500000

	fmt.Println("\n=== write-hot sharded counter: increments/sec by #sub-counters (16 shards) ===")
	fmt.Printf("%-16s %-18s\n", "sub-counters", "incr/s")
	base := 0.0
	for i, cs := range []int{1, 2, 4, 8, 16} {
		tp := runIncr(shards, opsPerWorker, cs)
		if i == 0 {
			base = tp
		}
		fmt.Printf("%-16d %-18.0f (%.2fx)\n", cs, tp, tp/base)
	}
}

func runIncr(numShards, opsPerWorker, counterShards int) float64 {
	c := distcache.NewCache(numShards, capacity, vnodes,
		distcache.WithHotKeys(false), distcache.WithCounterShards(counterShards))
	defer c.Close()
	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				c.Incr("likes", 1)
			}
		}()
	}
	wg.Wait()
	return float64(workers*opsPerWorker) / time.Since(start).Seconds()
}

func runZipf(numShards, opsPerWorker, writePct int, skew float64, opts ...distcache.Option) float64 {
	c := distcache.NewCache(numShards, capacity, vnodes, opts...)
	defer c.Close()
	for i := 0; i < numKeys; i++ {
		c.Put(fmt.Sprintf("key-%d", i), []byte("v"), 0)
	}
	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(id) + 1))
			z := rand.NewZipf(r, skew, 1, uint64(numKeys-1))
			for i := 0; i < opsPerWorker; i++ {
				key := fmt.Sprintf("key-%d", z.Uint64())
				if r.Intn(100) < writePct {
					c.Put(key, []byte("v"), 0)
				} else {
					c.Get(key)
				}
			}
		}(w)
	}
	wg.Wait()
	return float64(workers*opsPerWorker) / time.Since(start).Seconds()
}

type keygenFactory func(*rand.Rand) func() string

func uniformKeygen(r *rand.Rand) func() string {
	return func() string { return fmt.Sprintf("key-%d", r.Intn(numKeys)) }
}
func zipfianKeygen(r *rand.Rand) func() string {
	z := rand.NewZipf(r, 1.3, 1, uint64(numKeys-1)) // s=1.3 -> hot keys
	return func() string { return fmt.Sprintf("key-%d", z.Uint64()) }
}

func sweep(shardCounts []int, factory keygenFactory) {
	fmt.Printf("%-8s %-18s %-8s\n", "shards", "throughput(op/s)", "vs 1")
	base := 0.0
	for i, n := range shardCounts {
		tp := runBench(n, factory)
		if i == 0 {
			base = tp
		}
		fmt.Printf("%-8d %-18.0f %.2fx\n", n, tp, tp/base)
	}
}

func runBench(numShards int, factory keygenFactory) float64 {
	c := distcache.NewCache(numShards, capacity, vnodes)
	defer c.Close()
	for i := 0; i < numKeys; i++ { // warm up
		c.Put(fmt.Sprintf("key-%d", i), []byte("v"), 0)
	}

	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(id) + 1))
			gen := factory(r)
			for i := 0; i < opsPerWk; i++ {
				key := gen()
				if r.Intn(10) == 0 { // 10% writes, 90% reads
					c.Put(key, []byte("v"), 0)
				} else {
					c.Get(key)
				}
			}
		}(w)
	}
	wg.Wait()
	return float64(workers*opsPerWk) / time.Since(start).Seconds()
}
