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
