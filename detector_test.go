package distcache

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// testDetector: record-every-call sampler + huge interval (goroutine never fires
// unless started) → fully deterministic via decayOnce().
func testDetector(promote, demote float64) *detector {
	d := newDetector(detectorConfig{
		capacity: 64, sampleRate: 16, interval: time.Hour,
		decayFactor: 0.5, promote: promote, demote: demote, maxHot: 10,
	})
	d.sample = func() bool { return true }
	return d
}

func feed(d *detector, key string, n int) {
	for i := 0; i < n; i++ {
		d.record(key)
	}
}

func TestDetectorFindsHotKeys(t *testing.T) {
	d := testDetector(100, 50)
	feed(d, "hot", 10000)
	for i := 0; i < 40; i++ {
		feed(d, fmt.Sprintf("cold-%d", i), 3)
	}
	d.decayOnce()

	if !d.isHot("hot") {
		t.Fatal("'hot' should be detected as hot")
	}
	for i := 0; i < 40; i++ {
		if k := fmt.Sprintf("cold-%d", i); d.isHot(k) {
			t.Errorf("%s should be cold", k)
		}
	}
}

// Two keys land at the SAME estimate (75), opposite outcomes: the already-hot one
// stays (bar = demote 50), the fresh one is not promoted (bar = promote 100).
func TestDetectorHysteresis(t *testing.T) {
	d := testDetector(100, 50)

	feed(d, "k", 300)
	d.decayOnce() // k: 300→150, est≥100 → hot
	if !d.isHot("k") {
		t.Fatal("k should be hot after crossing promote")
	}

	feed(d, "fresh", 150)
	d.decayOnce() // k:150→75 stays (≥demote); fresh:150→75 not promoted (<promote)
	if !d.isHot("k") {
		t.Error("k should STAY hot at est 75 (hysteresis)")
	}
	if d.isHot("fresh") {
		t.Error("fresh should NOT promote at est 75")
	}

	d.decayOnce() // k:75→37.5 < demote 50 → demoted
	if d.isHot("k") {
		t.Error("k should demote once below the demote bar")
	}
}

// The hot key must survive a flood of unique cold keys, and the table stays bounded.
func TestDetectorEvictionBounded(t *testing.T) {
	d := testDetector(50, 25)
	for i := 0; i < 5000; i++ {
		d.record("hot")
		d.record(fmt.Sprintf("cold-%d", i)) // 5000 unique cold keys, capacity 64
	}
	if len(d.ss.counts) > d.ss.capacity {
		t.Fatalf("table exceeded capacity: %d > %d", len(d.ss.counts), d.ss.capacity)
	}
	d.decayOnce()
	if !d.isHot("hot") {
		t.Error("hot key must survive cold churn (Space-Saving guarantee)")
	}
}

func TestRateToCount(t *testing.T) {
	got := rateToCount(500000, time.Second, 0.5, 16) // ≈ 500000/16/ln2 ≈ 45100
	if got < 40000 || got > 50000 {
		t.Fatalf("rateToCount = %.0f, want ≈45000", got)
	}
}

// Exercises the real background goroutine + Close(). Keeps feeding so the key
// stays above promote until the loop publishes it; bounded poll, no fixed sleep.
func TestDetectorLifecycle(t *testing.T) {
	d := newDetector(detectorConfig{
		capacity: 64, sampleRate: 16, interval: time.Millisecond,
		decayFactor: 0.5, promote: 10, demote: 5, maxHot: 10,
	})
	d.sample = func() bool { return true }
	d.start()
	defer d.Close()

	deadline := time.Now().Add(2 * time.Second)
	for {
		feed(d, "hot", 200) // keep it hot while the loop catches up
		if d.isHot("hot") {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("background loop did not mark 'hot' within timeout")
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// Run under -race: concurrent record (lock) + decayOnce (lock) + isHot (lock-free).
func TestDetectorConcurrentRecord(t *testing.T) {
	d := testDetector(100, 50)
	var wg sync.WaitGroup

	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			for i := 0; i < 5000; i++ {
				d.record("hot")
				d.record(fmt.Sprintf("k-%d", (seed*5000+i)%500))
			}
		}(g)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			d.decayOnce()
			d.isHot("hot")
		}
	}()
	wg.Wait()

	d.decayOnce()
	if !d.isHot("hot") {
		t.Error("hot should be detected after concurrent load")
	}
}
