package distcache

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestPutGet(t *testing.T) {
	s := NewShard(10)
	s.Put("a", []byte("hello"), 0)
	got, ok := s.Get("a")
	if !ok || !bytes.Equal(got, []byte("hello")) {
		t.Fatalf("got %q ok=%v, want hello", got, ok)
	}
}

func TestGetMiss(t *testing.T) {
	s := NewShard(10)
	if _, ok := s.Get("nope"); ok {
		t.Fatal("expected miss for absent key")
	}
}

func TestUpdateOverwrites(t *testing.T) {
	s := NewShard(10)
	s.Put("a", []byte("v1"), 0)
	s.Put("a", []byte("v2"), 0)
	got, ok := s.Get("a")
	if !ok || !bytes.Equal(got, []byte("v2")) {
		t.Fatalf("got %q, want v2", got)
	}
	if s.Len() != 1 {
		t.Fatalf("Len = %d, want 1 (update must not add a new entry)", s.Len())
	}
}

func TestLRUEviction(t *testing.T) {
	s := NewShard(2)
	s.Put("a", []byte("1"), 0)
	s.Put("b", []byte("2"), 0)
	s.Put("c", []byte("3"), 0) // over capacity → evict LRU (a)
	if _, ok := s.Get("a"); ok {
		t.Fatal("expected a (least-recently-used) to be evicted")
	}
	if _, ok := s.Get("b"); !ok {
		t.Fatal("expected b to survive")
	}
	if _, ok := s.Get("c"); !ok {
		t.Fatal("expected c to survive")
	}
	if s.Len() != 2 {
		t.Fatalf("Len = %d, want 2", s.Len())
	}
}

func TestGetRefreshesRecency(t *testing.T) {
	s := NewShard(2)
	s.Put("a", []byte("1"), 0)
	s.Put("b", []byte("2"), 0)
	s.Get("a")                 // refresh a → b becomes least-recently-used
	s.Put("c", []byte("3"), 0) // over capacity → should evict b, not a
	if _, ok := s.Get("a"); !ok {
		t.Fatal("a was refreshed by Get, should survive")
	}
	if _, ok := s.Get("b"); ok {
		t.Fatal("b was least-recently-used, should be evicted")
	}
}

func TestTTLExpiry(t *testing.T) {
	s := NewShard(10)
	s.Put("a", []byte("1"), 20*time.Millisecond)
	if _, ok := s.Get("a"); !ok {
		t.Fatal("expected hit before TTL")
	}
	time.Sleep(30 * time.Millisecond)
	if _, ok := s.Get("a"); ok {
		t.Fatal("expected miss after TTL")
	}
	if s.Len() != 0 {
		t.Fatalf("Len = %d, want 0 (expired entry should be purged on access)", s.Len())
	}
}

func TestNoTTL(t *testing.T) {
	s := NewShard(10)
	s.Put("a", []byte("1"), 0) // ttl 0 = never expires
	time.Sleep(10 * time.Millisecond)
	if _, ok := s.Get("a"); !ok {
		t.Fatal("ttl=0 entry should never expire")
	}
}

func TestDelete(t *testing.T) {
	s := NewShard(10)
	s.Put("a", []byte("1"), 0)
	if !s.Delete("a") {
		t.Fatal("Delete should return true for present key")
	}
	if _, ok := s.Get("a"); ok {
		t.Fatal("key should be gone after Delete")
	}
	if s.Delete("a") {
		t.Fatal("Delete should return false for absent key")
	}
}

// TestConcurrent hammers the shard from many goroutines. Run with:
//
//	go test -race
//
// to catch data races (unsynchronized access to store/ll).
func TestConcurrent(t *testing.T) {
	s := NewShard(100)
	const goroutines, iters = 50, 1000

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(seed))) // per-goroutine RNG (not shared)
			for i := 0; i < iters; i++ {
				key := fmt.Sprintf("key-%d", r.Intn(200)) // 200 keys, cap 100 → real eviction
				switch r.Intn(3) {
				case 0:
					s.Put(key, []byte("v"), 0)
				case 1:
					s.Get(key)
				case 2:
					s.Delete(key)
				}
			}
		}(g)
	}
	wg.Wait()

	if s.Len() > 100 {
		t.Fatalf("Len = %d exceeds capacity 100", s.Len()) // capacity invariant must hold
	}
}
